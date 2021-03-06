from __future__ import absolute_import

import time

from collections import Iterable
from inspect import getargspec

from mock import patch

from tests import BaseTest
from nydus.db.base import Cluster
from nydus.db.backends import BaseConnection
from nydus.db.routers import BaseRouter, RoundRobinRouter
from nydus.db.routers.keyvalue import ConsistentHashingRouter, PartitionRouter


class PrefixPartitionTest(BaseTest):
    def setUp(self):
        from nydus.db import create_cluster
        engine = 'nydus.db.backends.redis.Redis'
        router = 'nydus.db.routers.redis.PrefixPartitionRouter'
        nydus_config = dict(engine=engine, router=router, hosts={
            'default': {'db': 0, 'host': 'localhost', 'port': 6379},
            'user:loves:': {'db': 1, 'host': 'localhost', 'port': 6379}
        })
        redis = create_cluster(nydus_config)
        self.redis = redis
    
    def test_partitions(self):
        '''
        Verify if we write ton one and only one redis database
        '''
        import mock
        
        keys = [
            ('user:loves:test', 1), 
            ('default_test',0),
            ('hash:entity:test', 0)
        ]
        
        for key, redis_db in keys:
            with mock.patch('redis.client.StrictRedis.execute_command') as fake_set:
                result = self.redis.set(key, '1')
                args, kwargs = fake_set.call_args
                instance, cmd, key, key_value = args 
                connection_kwargs = instance.connection_pool.connection_kwargs
                db = connection_kwargs['db']
                self.assertEqual(db, redis_db)
                
    def test_missing_default(self):
        from nydus.db import create_cluster
        from functools import partial
        
        engine = 'nydus.db.backends.redis.Redis'
        router = 'nydus.db.routers.redis.PrefixPartitionRouter'
        nydus_config = dict(engine=engine, router=router, hosts={
            'base': {'db': 0, 'host': 'localhost', 'port': 6379},
            'user:loves:': {'db': 1, 'host': 'localhost', 'port': 6379}
        })
        redis = create_cluster(nydus_config)
        
        redis_call = partial(redis.get, 'thiswillbreak')
        self.assertRaises(ValueError, redis_call)
        
    def test_pipeline(self):
        redis = self.redis
        #we prefer map above direct pipeline usage, but if you really need it:
        redis.pipeline('default:test')
        
        #this should fail as we require a key
        self.assertRaises(ValueError, redis.pipeline)


class DummyConnection(BaseConnection):
    def __init__(self, i):
        self.host = 'dummyhost'
        self.i = i
        super(DummyConnection, self).__init__(i)

    @property
    def identifier(self):
        return "%s:%s" % (self.host, self.i)


class BaseRouterTest(BaseTest):
    Router = BaseRouter
    class TestException(Exception): pass

    def setUp(self):
        self.router = self.Router()
        self.hosts = dict((i, DummyConnection(i)) for i in range(5))
        self.cluster = Cluster(router=self.Router, hosts=self.hosts)

    def get_dbs(self, *args, **kwargs):
        kwargs.setdefault('cluster', self.cluster)
        return self.router.get_dbs(*args, **kwargs)

    def test_not_ready(self):
        self.assertTrue(not self.router._ready)

    def test_get_dbs_iterable(self):
        db_nums = self.get_dbs(attr='test', key='foo')
        self.assertIsInstance(db_nums, Iterable)

    def test_get_dbs_unabletosetuproute(self):
        with patch.object(self.router, '_setup_router', return_value=False):
            with self.assertRaises(BaseRouter.UnableToSetupRouter):
                self.get_dbs(attr='test', key='foo')
        
    def test_setup_router_returns_true(self):
        self.assertTrue(self.router.setup_router(self.cluster))

    def test_offers_router_interface(self):
        self.assertTrue(callable(self.router.get_dbs))
        dbargs, _, _, dbdefaults = getargspec(self.router.get_dbs)
        self.assertTrue(set(dbargs) >= set(['self', 'cluster', 'attr', 'key']))
        self.assertIsNone(dbdefaults[0])

        self.assertTrue(callable(self.router.setup_router))
        setupargs, _, _, setupdefaults = getargspec(self.router.setup_router)
        self.assertTrue(set(setupargs) >= set(['self', 'cluster']))
        self.assertIsNone(setupdefaults)

    def test_returns_whole_cluster_without_key(self):
        self.assertEquals(self.hosts.keys(), self.get_dbs(attr='test'))

    def test_get_dbs_handles_exception(self):
        with patch.object(self.router, '_route') as _route:
            with patch.object(self.router, '_handle_exception') as _handle_exception:
                _route.side_effect = self.TestException()

                self.get_dbs(attr='test', key='foo')

                self.assertTrue(_handle_exception.called)

        
class BaseBaseRouterTest(BaseRouterTest):
    def test__setup_router_returns_true(self):
        self.assertTrue(self.router._setup_router(self.cluster))

    def test__pre_routing_returns_key(self):
        key = 'foo'

        self.assertEqual(key, self.router._pre_routing(self.cluster, 'foo', key))

    def test__route_returns_first_db_num(self):
        self.assertEqual(self.cluster.hosts.keys()[0], self.router._route(self.cluster, 'test', 'foo')[0])

    def test__post_routing_returns_db_nums(self):
        db_nums = self.hosts.keys()

        self.assertEqual(db_nums, self.router._post_routing(self.cluster, 'test', 'foo', db_nums))

    def test__handle_exception_raises_same_exception(self):
        e = self.TestException()

        with self.assertRaises(self.TestException):
            self.router._handle_exception(e)

    def test_returns_sequence_with_one_item_when_given_key(self):
        self.assertEqual(len(self.get_dbs(attr='test', key='foo')), len(self.hosts))


class BaseRoundRobinRouterTest(BaseRouterTest):
    Router = RoundRobinRouter

    def setUp(self):
        super(BaseRoundRobinRouterTest, self).setUp()
        assert self.router._setup_router(self.cluster)

    def test_ensure_db_num(self):
        db_num = 0
        s_db_num = str(db_num)

        self.assertEqual(self.router.ensure_db_num(db_num), db_num)
        self.assertEqual(self.router.ensure_db_num(s_db_num), db_num)

    def test_esnure_db_num_raises(self):
        with self.assertRaises(RoundRobinRouter.InvalidDBNum):
            self.router.ensure_db_num('a')

    def test_flush_down_connections(self):
        self.router._get_db_attempts = 9001
        self._down_connections = {0: time.time()}

        self.router.flush_down_connections()

        self.assertEqual(self.router._get_db_attempts, 0)
        self.assertEqual(self.router._down_connections, {})

    def test_mark_connection_down(self):
        db_num = 0

        self.router.mark_connection_down(db_num)

        self.assertAlmostEqual(self.router._down_connections[db_num], time.time(), delta=10)

    def test_mark_connection_up(self):
        db_num = 0

        self.router.mark_connection_down(db_num)

        self.assertIn(db_num, self.router._down_connections)

        self.router.mark_connection_up(db_num)

        self.assertNotIn(db_num, self.router._down_connections)

    def test__pre_routing_updates__get_db_attempts(self):
        self.router._pre_routing(self.cluster, 'test', 'foo')

        self.assertEqual(self.router._get_db_attempts, 1)

    @patch('nydus.db.routers.RoundRobinRouter.flush_down_connections')
    def test__pre_routing_flush_down_connections(self, _flush_down_connections):
        self.router._get_db_attempts = RoundRobinRouter.attempt_reconnect_threshold + 1

        self.router._pre_routing(self.cluster, 'test', 'foo')

        self.assertTrue(_flush_down_connections.called)

    @patch('nydus.db.routers.RoundRobinRouter.mark_connection_down')
    def test__pre_routing_retry_for(self, _mark_connection_down):
        db_num = 0

        self.router._pre_routing(self.cluster, 'test', 'foo', retry_for=db_num)

        _mark_connection_down.assert_called_with(db_num)

    @patch('nydus.db.routers.RoundRobinRouter.mark_connection_up')
    def test__post_routing_mark_connection_up(self, _mark_connection_up):
        db_nums = [0]

        self.assertEqual(self.router._post_routing(self.cluster, 'test', 'foo', db_nums), db_nums)
        _mark_connection_up.assert_called_with(db_nums[0])


class RoundRobinRouterTest(BaseRoundRobinRouterTest):
    def test__setup_router(self):
        self.assertTrue(self.router._setup_router(self.cluster))
        self.assertIsInstance(self.router._hosts_cycler, Iterable)

    def test__route_cycles_through_keys(self):
        db_nums = self.hosts.keys() * 2
        results = [self.router._route(self.cluster, 'test', 'foo')[0] for _ in db_nums]

        self.assertEqual(results, db_nums)

    def test__route_retry(self):
        self.router.retry_timeout = 0

        db_num = 0

        self.router.mark_connection_down(db_num)

        db_nums = self.router._route(self.cluster, 'test', 'foo')

        self.assertEqual(db_nums, [db_num])

    def test__route_skip_down(self):
        db_num = 0

        self.router.mark_connection_down(db_num)

        db_nums = self.router._route(self.cluster, 'test', 'foo')

        self.assertNotEqual(db_nums, [db_num])
        self.assertEqual(db_nums, [db_num+1])

    def test__route_hostlistexhausted(self):
        [self.router.mark_connection_down(db_num) for db_num in self.hosts.keys()]

        with self.assertRaises(RoundRobinRouter.HostListExhausted):
            self.router._route(self.cluster, 'test', 'foo')



class ConsistentHashingRouterTest(BaseRoundRobinRouterTest):
    Router = ConsistentHashingRouter

    def get_dbs(self, *args, **kwargs):
        kwargs['attr'] = 'test'
        return super(ConsistentHashingRouterTest, self).get_dbs(*args, **kwargs)

    def test_retry_gives_next_host_if_primary_is_offline(self):
        self.assertEquals([2], self.get_dbs(key='foo'))
        self.assertEquals([4], self.get_dbs(key='foo', retry_for=2))

    def test_retry_host_change_is_sticky(self):
        self.assertEquals([2], self.get_dbs(key='foo'))
        self.assertEquals([4], self.get_dbs(key='foo', retry_for=2))

        self.assertEquals([4], self.get_dbs(key='foo'))

    def test_adds_back_down_host_once_attempt_reconnect_threshold_is_passed(self):
        ConsistentHashingRouter.attempt_reconnect_threshold = 3

        self.assertEquals([2], self.get_dbs(key='foo'))
        self.assertEquals([4], self.get_dbs(key='foo', retry_for=2))
        self.assertEquals([4], self.get_dbs(key='foo'))

        # Router should add host 1 back to the pool now
        self.assertEquals([2], self.get_dbs(key='foo'))

        ConsistentHashingRouter.attempt_reconnect_threshold = 100000

    def test_raises_host_list_exhaused_if_no_host_can_be_found(self):
        # Kill the first 4
        [self.get_dbs(retry_for=i) for i in range(4)]

        # And the 5th should raise an error
        self.assertRaises(
            ConsistentHashingRouter.HostListExhausted,
            self.get_dbs, **dict(key='foo', retry_for=4))

