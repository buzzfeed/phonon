#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mock
import time
import redis
import pickle
import datetime
import mockredis
import unittest
import logging
from mockredis import mock_strict_redis_client

from phonon.client import Client
from phonon.client.config import configure
from phonon.client.config.node import Node
from phonon.client.config import LOCAL_TZ
from phonon.operation import Operation
from phonon.exceptions import ReadError, EmptyResult

from phonon import logger

console = logger.get_logger(__name__, log_level=logging.INFO)

def mock_redis(f):

    @mock.patch('redis.StrictRedis', mock_strict_redis_client)
    def wrapped(*args, **kwargs):
        return f(*args, **kwargs)

    wrapped.__name__ = f.__name__

    return wrapped

class ClientTest(unittest.TestCase):

    def setUp(self):
        configure({'ec': ['A', 'B'],
                   'wc': ['C', 'D']})

        self.client = Client()

    @mock_redis
    def test_set_sets_all_types_as_str(self):
        assert self.client.set('a', 1)
        a = self.client.get('a')
        assert isinstance(int(a), int), "{0} is not an int".format(a)
        assert int(a) == 1, "{0} != {1}".format(a, 1)

        assert self.client.set('b', 2L)
        b = self.client.get('b')
        assert long(b) == 2
        assert isinstance(long(b), long)

        assert self.client.set('c', 'foo')
        c = self.client.get('c')
        assert c == 'foo'
        assert isinstance(c, str)

        assert self.client.set('d', u"\u20ac")
        assert self.client.get('d') == '€'
        assert isinstance(self.client.get('d'), str)

        assert self.client.set('e', True)
        e = self.client.get('e')
        assert e == 'True', "{0} != True".format(e)
        assert isinstance(self.client.get('e'), str)

        assert self.client.set('f', False)
        assert self.client.get('f') == 'False'
        assert isinstance(self.client.get('f'), str)

        assert self.client.set('g', None)
        assert self.client.get('g') == 'None'
        assert isinstance(self.client.get('g'), str)

        assert self.client.set('h', '')
        assert self.client.get('h') == ''
        assert isinstance(self.client.get('h'), str)

        assert self.client.set('i', -1)
        assert self.client.get('i') == '-1'
        assert isinstance(self.client.get('i'), str)

        assert self.client.set('j', 1.)
        assert self.client.get('j') == '1.0'
        assert isinstance(self.client.get('j'), str)

    @mock_redis
    def test_get_connects_when_not_connected(self):
        assert self.client.get('foo') == None # Causes Connection

        a = Node('A', region='wc')
        b = Node('B', region='wc')
        c = Node('C', region='wc')
        d = Node('D', region='wc')
        e = Node('E', region='wc')

        assert self.client.has_connection(a) == True
        assert self.client.has_connection(b) == True
        assert self.client.has_connection(c) == True
        assert self.client.has_connection(d) == True
        assert self.client.has_connection(e) == False

    @mock_redis
    def test_set_connects_when_not_connected(self):
        assert self.client.set('foo', 'a')

        a = Node('A', region='wc')
        b = Node('B', region='wc')
        c = Node('C', region='wc')
        d = Node('D', region='wc')
        e = Node('E', region='wc')

        assert self.client.has_connection(a) == True
        assert self.client.has_connection(b) == True
        assert self.client.has_connection(c) == True
        assert self.client.has_connection(d) == True
        assert self.client.has_connection(e) == False

    @mock_redis
    def test_pipeline_connects_when_not_connected(self):
        a = Node('A', region='wc')
        assert not self.client.has_connection(a)
        pipe = self.client.pipeline(a)
        assert self.client.has_connection(a)

    @mock_redis
    def test_pipeline_returns_pipeline(self):
        a = Node('A', region='wc')
        assert not self.client.has_connection(a)
        pipe = self.client.pipeline(a)
        assert isinstance(pipe, (redis.client.StrictPipeline, mockredis.pipeline.MockRedisPipeline))

    @mock_redis
    def test_get_consensus_gets_majority_nominally(self):
        self.client.set('biz', 'bar')

        node_a = Node(hostname='A', region='ec')
        node_b = Node(hostname='B', region='ec')
        node_c = Node(hostname='C', region='wc')
        node_d = Node(hostname='D', region='wc')

        a = self.client.get_connection(node_a)
        b = self.client.get_connection(node_b)
        c = self.client.get_connection(node_c)
        d = self.client.get_connection(node_d)

        assert a.get('biz') == 'bar'
        assert b.get('biz') == 'bar'
        assert c.get('biz') == 'bar'
        assert d.get('biz') == 'bar'

        self.client.get('biz') == 'bar'

        a.set('biz', 'baz')

        assert a.get('biz') == 'baz'
        assert b.get('biz') == 'bar'
        assert c.get('biz') == 'bar'
        assert d.get('biz') == 'bar'

        self.client.get('biz') == 'bar'

        # Inconsistent will be corrected. Have to reset.
        a.set('biz', 'baz')
        b.set('biz', 'baz')

        assert a.get('biz') == 'baz'
        assert b.get('biz') == 'baz'
        assert c.get('biz') == 'bar'
        assert d.get('biz') == 'bar'

        with self.assertRaisesRegexp(ReadError, "Maximum retries exceeded."):
            self.client.get('biz')

    @mock_redis
    def test_get_consensus_raises_when_no_majority_exists(self):
        self.client.set('biz', 'bar')

        node_a = Node(hostname='A', region='ec')
        node_b = Node(hostname='B', region='ec')
        node_c = Node(hostname='C', region='wc')
        node_d = Node(hostname='D', region='wc')

        a = self.client.get_connection(node_a)
        b = self.client.get_connection(node_b)
        c = self.client.get_connection(node_c)
        d = self.client.get_connection(node_d)

        assert a.get('biz') == 'bar'
        assert b.get('biz') == 'bar'
        assert c.get('biz') == 'bar'
        assert d.get('biz') == 'bar'

        self.client.get('biz') == 'bar'

        a.set('biz', 'baz')
        b.set('biz', 'baz')

        with self.assertRaisesRegexp(ReadError, "Maximum retries exceeded."):
            self.client.get('biz')

    @mock_redis
    def test_get_consensus_continues_on_read_error(self):
        self.client.set('biz', 'bar')

        node_a = Node(hostname='A', region='ec')
        node_b = Node(hostname='B', region='ec')
        node_c = Node(hostname='C', region='wc')
        node_d = Node(hostname='D', region='wc')

        a = self.client.get_connection(node_a)
        b = self.client.get_connection(node_b)
        c = self.client.get_connection(node_c)
        d = self.client.get_connection(node_d)

        assert a.get('biz') == 'bar'
        assert b.get('biz') == 'bar'
        assert c.get('biz') == 'bar'
        assert d.get('biz') == 'bar'

        def get(*args, **kwargs):
            raise ReadError("Fake read error")

        backup = a.get
        a.get = get

        try:
            assert self.client.get('biz') == 'bar'
        finally:
            a.get = backup

    @mock_redis
    @mock.patch('phonon.client.logger.error')
    def test_get_consensus_logs_read_errors(self, error_log):
        self.client.set('biz', 'bar')

        node_a = Node(hostname='A', region='ec')
        node_b = Node(hostname='B', region='ec')
        node_c = Node(hostname='C', region='wc')
        node_d = Node(hostname='D', region='wc')

        a = self.client.get_connection(node_a)
        b = self.client.get_connection(node_b)
        c = self.client.get_connection(node_c)
        d = self.client.get_connection(node_d)

        assert a.get('biz') == 'bar'
        assert b.get('biz') == 'bar'
        assert c.get('biz') == 'bar'
        assert d.get('biz') == 'bar'

        def get(*args, **kwargs):
            raise ReadError("Fake read error")

        backup = a.get
        a.get = get

        try:
            assert self.client.get('biz') == 'bar'
            error_log.assert_called_with("Error during rollback: Fake read error")
        finally:
            a.get = backup

    @mock_redis
    def test_get_consensus_checks_all_nodes_on_shard(self):
        self.client.set('biz', 'bar')

        node_a = Node(hostname='A', region='ec')
        node_b = Node(hostname='B', region='ec')
        node_c = Node(hostname='C', region='wc')
        node_d = Node(hostname='D', region='wc')

        a = self.client.get_connection(node_a)
        b = self.client.get_connection(node_b)
        c = self.client.get_connection(node_c)
        d = self.client.get_connection(node_d)

        assert a.get('biz') == 'bar'
        assert b.get('biz') == 'bar'
        assert c.get('biz') == 'bar'
        assert d.get('biz') == 'bar'

        def get(*args, **kwargs):
            raise ReadError("Fake read error")

        backup = a.get
        a.get = get

        try:
            assert self.client.get('biz') == 'bar'
        finally:
            a.get = backup

    def test_get_consensus_raises_when_no_votes(self):
        with self.assertRaisesRegexp(EmptyResult, "No result at all from the shard."):
            self.client._Client__get_majority_and_inconsistencies([])

    def test_get_consensus_returns_inconsistencies(self):
        votes = ['a', 'a', 'a', 'b']
        majority, inconsistent = self.client._Client__get_majority_and_inconsistencies(votes)
        assert 3 in inconsistent
        assert majority == 'a'

    @mock_redis
    def test_previously_failed_commits_get_rolled_back_on_query_to_commit(self):
        self.client.set('biz', 'bar')

        node_a = Node(hostname='A', region='ec')
        node_b = Node(hostname='B', region='ec')
        node_c = Node(hostname='C', region='wc')
        node_d = Node(hostname='D', region='wc')

        a = self.client.get_connection(node_a)
        b = self.client.get_connection(node_b)
        c = self.client.get_connection(node_c)
        d = self.client.get_connection(node_d)

        assert a.get('biz') == 'bar'
        assert b.get('biz') == 'bar'
        assert c.get('biz') == 'bar'
        assert d.get('biz') == 'bar'

        aop = Operation.from_str(a.get('biz.oplog'))
        bop = Operation.from_str(b.get('biz.oplog'))
        cop = Operation.from_str(c.get('biz.oplog'))
        dop = Operation.from_str(d.get('biz.oplog'))

        a.set('biz', 'baz')
        a.set('biz.oplog', aop.to_str())
        b.set('biz', 'boz')
        b.set('biz.oplog', bop.to_str())
        c.set('biz', 'winner')
        c.set('biz.oplog', cop.to_str())
        d.set('biz', 'oof')
        d.set('biz.oplog', dop.to_str())


    def test_write_oplog_succeeds(self):
        pass

    def test_query_to_commit_writes_oplog_even_when_op_fails(self):
        pass

    def test_query_to_commit_raises_rollback_when_op_fails(self):
        pass

    def test_query_to_commit_raises_rollback_when_oplog_fails(self):
        pass

    def test_query_to_commit_raises_rollback_when_unexpected_errors(self):
        pass

    def test_rollback_uses_latest_committed_when_multiiple_are_committed(self):
        pass

    def test_rollback_succeeds_when_none_are_committed(self):
        pass

    def test_rollback_succeeds_when_no_oplogs_exist(self):
        pass

    def test_rollback_leaves_state_consistent_on_success(self):
        pass

    def test_rollback_does_all_it_can_on_failure(self):
        pass

    # [TODO: write tests for operations]
