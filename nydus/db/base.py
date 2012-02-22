"""
nydus.db.base
~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from collections import defaultdict
from nydus.db.routers import BaseRouter
from nydus.utils import import_string, ThreadPool


def create_cluster(settings):
    """
    redis = create_cluster({
        'engine': 'nydus.db.backends.redis.Redis',
        'router': 'nydus.db.routers.redis.PartitionRouter',
        'hosts': {
            0: {'db': 0},
            1: {'db': 1},
            2: {'db': 2},
        }
    })
    """
    # Pull in our client
    if isinstance(settings['engine'], basestring):
        conn = import_string(settings['engine'])
    else:
        conn = settings['engine']

    # Pull in our router
    router = settings.get('router')
    if isinstance(router, basestring):
        router = import_string(router)()
    elif router:
        router = router()
    else:
        router = BaseRouter()

    # Build the connection cluster
    return Cluster(
        router=router,
        hosts=dict(
            (conn_number, conn(num=conn_number, **host_settings))
            for conn_number, host_settings
            in settings['hosts'].iteritems()
        ),
    )


class Cluster(object):
    """
    Holds a cluster of connections.
    """

    def __init__(self, hosts, router=None, max_connection_retries=20):
        self.hosts = hosts
        self.router = router
        self.max_connection_retries = max_connection_retries

    def __len__(self):
        return len(self.hosts)

    def __getitem__(self, name):
        return self.hosts[name]

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return CallProxy(self, attr)

    def __iter__(self):
        for name in self.hosts.iterkeys():
            yield name

    def _execute(self, attr, args, kwargs):
        db_nums = self._db_nums_for(*args, **kwargs)

        if self.router and len(db_nums) is 1 and self.router.retryable:
            # The router supports retryable commands, so we want to run a
            # separate algorithm for how we get connections to run commands on
            # and then possibly retry
            return self._retryable_execute(db_nums, attr, *args, **kwargs)
        else:
            connections = self._connections_for(*args, **kwargs)
            results = [getattr(conn, attr)(*args, **kwargs) for conn in connections]

            # If we only had one db to query, we simply return that res
            if len(results) == 1:
                return results[0]
            else:
                return results

    def disconnect(self):
        """Disconnects all connections in cluster"""
        for connection in self.hosts.itervalues():
            connection.disconnect()

    def get_conn(self, *args, **kwargs):
        """
        Returns a connection object from the router given ``args``.

        Useful in cases where a connection cannot be automatically determined
        during all steps of the process. An example of this would be
        Redis pipelines.
        """
        connections = self._connections_for(*args, **kwargs)

        if len(connections) is 1:
            return connections[0]
        else:
            return connections

    def map(self, workers=None):
        return DistributedContextManager(self, workers)

    def _retryable_execute(self, db_nums, attr, *args, **kwargs):
        retries = 0

        while retries <= self.max_connection_retries:
            if len(db_nums) > 1:
                raise Exception('Retryable router returned multiple DBs')
            else:
                connection = self[db_nums[0]]

            try:
                return getattr(connection, attr)(*args, **kwargs)
            except tuple(connection.retryable_exceptions):
                # We had a failure, so get a new db_num and try again, noting
                # the DB number that just failed, so the backend can mark it as
                # down
                db_nums = self._db_nums_for(retry_for=db_nums[0], *args, **kwargs)
                retries += 1
        else:
            raise Exception('Maximum amount of connection retries exceeded')

    def _db_nums_for(self, *args, **kwargs):
        if self.router:
            return self.router.get_db(self, 'get_conn', *args, **kwargs)
        else:
            return range(len(self))

    def _connections_for(self, *args, **kwargs):
        return [self[n] for n in self._db_nums_for(*args, **kwargs)]


class CallProxy(object):
    """
    Handles routing function calls to the proper connection.
    """
    def __init__(self, cluster, attr):
        self._cluster = cluster
        self._attr = attr

    def __call__(self, *args, **kwargs):
        return self._cluster._execute(self._attr, args, kwargs)


class EventualCommand(object):
    _attr = None
    _wrapped = None
    _evaled = False

    # introspection support:
    __members__ = property(lambda self: self.__dir__())

    def __init__(self, attr):
        self.__dict__.update({
            '_attr': attr,
            '_wrapped': None,
            '_evaled': False,
            '_args': [],
            '_kwargs': {},
            '_ident': None,
        })

    def __call__(self, *args, **kwargs):
        self.__dict__.update({
            '_args': args,
            '_kwargs': kwargs,
            '_ident': ':'.join(map(str, [id(self._attr), id(self._args), id(self._kwargs)])),
        })
        return self

    def __repr__(self):
        if self._evaled:
            return repr(self._wrapped)
        return u'<EventualCommand: %s args=%s kwargs=%s>' % (self._attr, self._args, self._kwargs)

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        return getattr(self._wrapped, name)

    def __setattr__(self, name, value):
        if name in self.__dict__:
            self.__dict__[name] = value
        else:
            setattr(self._wrapped, name, value)

    def __delattr__(self, name):
        if name == "_wrapped":
            raise TypeError("can't delete _wrapped.")
        delattr(self._wrapped, name)

    def __dir__(self):
        return dir(self._wrapped)

    def __str__(self):
        return str(self._wrapped)

    def __unicode__(self):
        return unicode(self._wrapped)

    def __deepcopy__(self, memo):
        # Changed to use deepcopy from copycompat, instead of copy
        # For Python 2.4.
        from django.utils.copycompat import deepcopy
        return deepcopy(self._wrapped, memo)

    # Need to pretend to be the wrapped class, for the sake of objects that care
    # about this (especially in equality tests)
    def __get_class(self):
        return self._wrapped.__class__
    __class__ = property(__get_class)

    def __eq__(self, other):
        return self._wrapped == other

    def __hash__(self):
        return hash(self._wrapped)

    def _set_value(self, value):
        self._wrapped = value
        self._evaled = True


class DistributedConnection(object):
    def __init__(self, cluster, workers=None):
        self._cluster = cluster
        self._workers = min(workers or len(cluster), 16)
        self._commands = []
        self._complete = False

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            command = EventualCommand(attr)
            self._commands.append(command)
            return command

    def _execute(self):
        num_commands = len(self._commands)
        if num_commands == 0:
            self._commands = []
            return

        pool = None
        command_map = {}
        pipelined = all(self._cluster[n].supports_pipelines for n in self._cluster)
        pending_commands = defaultdict(list)

        # used in pipelining
        if pipelined:
            pipe_command_map = defaultdict(list)

            # Build up a collection of pipes
            pipes = dict((n, self._cluster[n].get_pipeline()) for n in self._cluster)

        # build up a list of pending commands and their routing information
        for command in self._commands:
            cmd_ident = command._ident
            cmd_attr = command._attr
            cmd_args = command._args
            cmd_kwargs = command._kwargs

            command_map[cmd_ident] = command

            if self._cluster.router:
                db_nums = self._cluster.router.get_db(self._cluster, cmd_attr, *cmd_args, **cmd_kwargs)
            else:
                db_nums = range(len(self._cluster))

            # The number of commands is based on the total number of executable commands
            num_commands += len(db_nums)

            # Don't bother with the pooling if we only need to do one operation on a single machine
            if num_commands == 1:
                self._commands = [getattr(self._cluster[n], cmd_attr)(*cmd_args, **cmd_kwargs) for n in n]
                return

            # Create the threadpool and pipe jobs into it
            if not pool:
                pool = ThreadPool(self._workers)

            # update the pipelined dbs
            for db_num in db_nums:
                # map the ident to a db
                if pipelined:
                    pipe_command_map[db_num].append(cmd_ident)

                # add to pending commands
                pending_commands[db_num].append(command)

        # execute our pending commands either in the pool, or using a pipeline
        for db_num, command_list in pending_commands.iteritems():
            for command in command_list:
                if pipelined:
                    # add to pipeline
                    pipes[db_num].add(command)
                else:
                    # execute in pool
                    pool.add(command._ident, getattr(self._cluster[db_num], command._attr), command._args, command._kwargs)

        # We need to finalize our commands with a single execute in pipelines
        if pipelined:
            for db, pipe in pipes.iteritems():
                pool.add(db, pipe.execute, (), {})

        # Consolidate commands with their appropriate results
        result_map = pool.join()

        # Results get grouped by their command signature, so we have to separate the logic
        if pipelined:
            for db, result in result_map.iteritems():
                if len(result) == 1:
                    result = result[0]
                for i, value in enumerate(result):
                    command_map[pipe_command_map[db][i]]._set_value(value)

        else:
            for command in self._commands:
                result = result_map[command._ident]
                if len(result) == 1:
                    result = result[0]
                command._set_value(result)

        self._complete = True

    def get_results(self):
        assert self._complete, 'you must execute the commands before fetching results'

        return self._commands


class DistributedContextManager(object):
    def __init__(self, cluster, workers=None):
        self._workers = workers
        self._cluster = cluster

    def __enter__(self):
        self._handler = DistributedConnection(self._cluster, self._workers)
        return self._handler

    def __exit__(self, exc_type, exc_value, tb):
        # we need to break up each command and route it
        self._handler._execute()


class LazyConnectionHandler(dict):
    """
    Maps clusters of connections within a dictionary.
    """
    def __init__(self, conf_callback):
        self.conf_callback = conf_callback
        self.conf_settings = {}
        self._is_ready = False

    def __getitem__(self, key):
        if not self.is_ready():
            self.reload()
        return super(LazyConnectionHandler, self).__getitem__(key)

    def __getattr__(self, key):
        if not self.is_ready():
            self.reload()
        return super(LazyConnectionHandler, self).__getattr__(key)

    def is_ready(self):
        return self._is_ready
        # if self.conf_settings != self.conf_callback():
        #     return False
        # return True

    def reload(self):
        for conn_alias, conn_settings in self.conf_callback().iteritems():
            self[conn_alias] = create_cluster(conn_settings)
        self._is_ready = True

    def disconnect(self):
        """Disconnects all connections in cluster"""
        for connection in self.itervalues():
            connection.disconnect()