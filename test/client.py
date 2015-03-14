#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mock
import uuid
import redis
import mockredis
import unittest
import logging
from mockredis import mock_strict_redis_client

from phonon.client import Client
from phonon.client.config import configure
from phonon.client.config.node import Node
from phonon.operation import Operation
from phonon.exceptions import ReadError, EmptyResult, WriteError

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
        assert self.client.get('foo') == None  # Causes Connection

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

    @mock_redis
    def test_write_oplog_succeeds(self):
        unique = uuid.uuid4()

        node_a = Node(hostname='A', region='ec')
        node_b = Node(hostname='B', region='ec')
        node_c = Node(hostname='C', region='wc')
        node_d = Node(hostname='D', region='wc')

        a = self.client.get_connection(node_a)
        b = self.client.get_connection(node_b)
        c = self.client.get_connection(node_c)
        d = self.client.get_connection(node_d)

        assert self.client.set('biz', str(unique))

        aop = Operation.from_str(a.get('biz.oplog'))
        bop = Operation.from_str(b.get('biz.oplog'))
        cop = Operation.from_str(c.get('biz.oplog'))
        dop = Operation.from_str(d.get('biz.oplog'))

        assert aop.is_committed()
        assert bop.is_committed()
        assert cop.is_committed()
        assert dop.is_committed()

        assert aop._Operation__meta['pvalue'] is None
        assert aop.call.kwargs == {}
        assert aop.call.args == ('biz', str(unique))
        assert aop.call.func == 'set'

    @mock_redis
    @mock.patch('phonon.client.Client._Client__write_oplog')
    @mock.patch('phonon.client.Client._Client__rollback')
    def test_query_to_commit_raises_rollback_when_oplog_fails(self, rollback, write_oplog):
        write_oplog.side_effect = Exception("Failed to write oplog")
        with self.assertRaisesRegexp(WriteError, 'Maximum retries exceeded.'):
            a = self.client.set('a', 1)
        rollback.assert_called_once()

    @mock_redis
    @mock.patch('phonon.client.Client.pipeline')
    @mock.patch('phonon.client.Client._Client__rollback')
    def test_query_to_commit_raises_rollback_when_op_fails(self, rollback, pipeline):
        _ = mock.MagicMock()
        _.execute = mock.MagicMock()
        _.execute.side_effect = redis.exceptions.ConnectionError('Error writing to socket')
        pipeline.return_value = _
        with self.assertRaisesRegexp(WriteError, 'Maximum retries exceeded.'):
            a = self.client.set('a', 1)
        rollback.assert_called_once()

    @mock_redis
    @mock.patch('phonon.client.Client.pipeline')
    @mock.patch('phonon.client.Client._Client__rollback')
    def test_query_to_commit_raises_rollback_when_unexpected_errors(self, rollback, pipeline):
        _ = mock.MagicMock()
        _.execute = mock.MagicMock()
        _.execute.return_value = [False, True]
        pipeline.return_value = _

        with self.assertRaisesRegexp(WriteError, 'Maximum retries exceeded.'):
            a = self.client.set('a', 1)

        rollback.assert_called_once()

    @mock_redis
    def test_rollback_succeeds_when_none_are_committed(self):
        self.client.set('a', 1)

        def _raise(*args, **kwargs):
            raise Exception("Raised instead of committed.")

        self.client._Client__commit = _raise

        assert self.client.get('a') == '1'

        with self.assertRaisesRegexp(Exception, 'Raised instead of committed.'):
            self.client.set('a', 2)
            # [TODO: It's not rolling back.]

        node_a = Node(hostname='A', region='ec')
        node_b = Node(hostname='B', region='ec')
        node_c = Node(hostname='C', region='wc')
        node_d = Node(hostname='D', region='wc')

        a = self.client.get_connection(node_a)
        b = self.client.get_connection(node_b)
        c = self.client.get_connection(node_c)
        d = self.client.get_connection(node_d)

        assert a.get('a') == '1', a.get('a')
        assert b.get('a') == '1', b.get('a')
        assert c.get('a') == '1', c.get('a')
        assert d.get('a') == '1', d.get('a')

    def test_rollback_succeeds_when_no_oplogs_exist(self):
        pass

    def test_rollback_leaves_state_consistent_on_success(self):
        pass

    def test_rollback_does_all_it_can_on_failure(self):
        pass

    # [TODO: write tests for operations]
