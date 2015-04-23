import unittest
import datetime
import pytz
import logging

from phonon import  TTL
from phonon.process import Process
from phonon.nodelist import Nodelist

logging.disable(logging.CRITICAL)


class NodelistTest(unittest.TestCase):

    def setUp(self):
        if hasattr(Process, "client"):
            Process.client.flushdb()

    def test_create_node_list(self):
        p = Process()
        nodelist = Nodelist(p, "key")
        assert nodelist.nodelist_key == "phonon_key.nodelist"  
        assert Process.client.hgetall(nodelist.nodelist_key) != {}

        p.stop()

    def test_refresh_session_refreshes_time(self):
        p = Process()
        nodelist = Nodelist(p, "key")
        now = datetime.datetime.now(pytz.utc)
        Process.client.hset(nodelist.nodelist_key, p.id, now)
        nodelist.refresh_session()
        updated_now = nodelist.get_last_updated(p.id)
        assert isinstance(updated_now, datetime.datetime)
        assert updated_now != now
        p.stop()

    def test_find_expired_nodes(self):
        now = datetime.datetime.now(pytz.utc)
        expired = now - datetime.timedelta(seconds=2 * TTL + 1)

        p = Process()
        nodelist = Nodelist(p, "key")

        Process.client.hset(nodelist.nodelist_key, '1', now.isoformat())
        Process.client.hset(nodelist.nodelist_key, '2', expired.isoformat())

        target = nodelist.find_expired_nodes()
        assert u'2' in target, target
        assert u'1' not in target, target

        p.stop()

    def test_remove_expired_nodes(self):
        now = datetime.datetime.now(pytz.utc)
        expired = now - datetime.timedelta(seconds=2 * TTL + 1)

        p = Process()
        nodelist = Nodelist(p, "key")

        Process.client.hset(nodelist.nodelist_key, '1', expired.isoformat())
        Process.client.hset(nodelist.nodelist_key, '2', expired.isoformat())

        nodes = nodelist.get_all_nodes()
        assert '1' in nodes
        assert '2' in nodes

        nodelist.remove_expired_nodes()
        nodes = nodelist.get_all_nodes()
        assert '1' not in nodes
        assert '2' not in nodes

        p.stop()

    def test_refreshed_node_not_deleted(self):
        now = datetime.datetime.now(pytz.utc)
        expired = now - datetime.timedelta(seconds=2 * TTL + 1)

        p = Process()
        nodelist = Nodelist(p, 'key')

        Process.client.hset(nodelist.nodelist_key, '1', expired.isoformat())
        Process.client.hset(nodelist.nodelist_key, '2', expired.isoformat())

        expired = nodelist.find_expired_nodes()
        assert u'2' in expired, expired
        assert u'1' in expired, expired
        Process.client.hset(nodelist.nodelist_key, '1', now.isoformat())

        nodelist.refresh_session('1')
        nodelist.remove_expired_nodes(expired)

        assert nodelist.get_last_updated('1') is not None, nodelist.get_last_updated('1')
        assert nodelist.get_last_updated('2') is None, nodelist.get_last_updated('2')

        p.stop()

    def test_remove_node(self):
        p = Process()
        nodelist = Nodelist(p, 'key')
        nodelist.refresh_session('1')

        nodes = nodelist.get_all_nodes()
        assert '1' in nodes

        nodelist.remove_node('1')
        nodes = nodelist.get_all_nodes()
        assert '1' not in nodes
        p.stop()

    def test_clear_nodelist(self):
        p = Process()
        nodelist = Nodelist(p, 'key')
        nodes = nodelist.clear_nodelist()
        nodes = nodelist.get_all_nodes()
        assert nodes == {}
        p.stop()