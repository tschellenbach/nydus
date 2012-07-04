from nydus.db.routers import BaseRouter
from collections import defaultdict
from nydus.db.routers.keyvalue import ConsistentHashingRouter

#caching for setting up hashing clusters
hashing_clusters = {}

class PrefixPartitionRouter(BaseRouter):
    '''
    Routes based on the configured prefixes.
    The default prefix is required for at least one connection.
    If you specify the same prefix for multiple connections it will use the ConsistentHashingRouter
    to sub route
    
    Example config:
    
    'redis': {
        'engine': 'nydus.db.backends.redis.Redis',
        'router': 'nydus.db.routers.redis.PrefixPartitionRouter',
        'hosts': {
            0: {'prefix': 'default', 'db': 0, 'host': 'localhost', 'port': 6379},
            #testing the ketama based hashing by routing 2 to the same prefix
            1: {'prefix': 'user:loves:', 'db': 1, 'host': 'localhost', 'port': 6379},
            2: {'prefix': 'user:loves:', 'db': 2, 'host': 'localhost', 'port': 6379}
        }
    }
    
    We route to one and only one redis.
    '''
    
    def _pre_routing(self, cluster, attr, key, *args, **kwargs):
        """
        Requesting a pipeline without a key to partition on is just plain wrong.
        We raise a valueError if you try
        """
        if not key and attr == 'pipeline':
            raise ValueError('Pipelines requires a key for proper routing')
        return key
    
    def _get_hashing_cluster(self, hosts):
        key = tuple(hosts)
        #memorization at the process/module level
        cluster = hashing_clusters.get(key)
        if not cluster:
            from nydus.db.base import Cluster
            from nydus.db.backends.redis import Redis
            router = ConsistentHashingRouter
            host_dict = dict((host.num, host) for host in hosts)
            cluster = Cluster(
                router=router,
                hosts=host_dict,
            )
            hashing_clusters[key] = cluster
        return cluster
    
    def _route(self, cluster, attr, key, *args, **kwargs):
        """
        Perform routing and return db_nums
        """
        #create a list per prefix, lists are hashed using consistent hashing
        prefix_dict = defaultdict(list)
        for host_name, host in cluster.hosts.items():
            prefix = host.options.get('prefix')
            if not prefix or not isinstance(prefix, basestring):
                error_message = 'Every connection needs to specify a prefix, connection %s had prefix %s' % (host_name, prefix)
                raise ValueError(error_message)
            prefix_dict[prefix].append(host)
            
        #check that we have a default
        if 'default' not in prefix_dict:
            error_message = 'The prefix router requires a default host'
            raise ValueError(error_message)
        
        #check that none of the hashing things have the exact same key (it breaks ketama)
        for prefix, prefix_hosts in prefix_dict.items():
            if len(prefix_hosts) > 1:
                unique_hosts = set([(h.db, h.host, h.port) for h in prefix_hosts])
                if len(prefix_hosts) != len(unique_hosts):
                    error_message = 'Dont use the same db/host/port combination for prefix %s, this breaks Ketama routing' % prefix
                    raise ValueError(error_message)
        
        #do the prefix based routing
        hosts = prefix_dict['default']
        if key:
            for prefix, prefix_hosts in prefix_dict.items():
                if key.startswith(prefix):
                    hosts = prefix_hosts
                
        #if we have more than host, let's use the awesome Ketama based routing
        if len(hosts) > 1:
            hashing_cluster = self._get_hashing_cluster(hosts)
            host_nums = hashing_cluster.router.get_dbs(hashing_cluster, attr, key, *args, **kwargs)
        else:
            host_nums = [h.num for h in hosts]
                
        #sanity check, dont see how this can happen
        if not host_nums:
            error_message = 'The prefix partition router couldnt find a host for command %s and key %s' % (attr, key)
            raise ValueError(error_message)
        elif len(host_nums) > 1:
            error_message = 'We should only route to one server, not multiple, found %s for key %s' % (host_nums, key)
            raise ValueError(error_message)
        
        return host_nums
        
