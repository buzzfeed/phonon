import unittest
import pickle
import mock
import json
import redis
from dateutil import parser
import datetime
import pytz
import time
import logging
import uuid

from phonon import LOCAL_TZ, TTL
from phonon.reference import Reference
from phonon.process import Process
from phonon.update import Update
from phonon.cache import LruCache
from phonon.nodelist import Nodelist

logging.disable(logging.CRITICAL)


def get_milliseconds_timestamp():
    return (datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000


class ProcessTest(unittest.TestCase):

    def setUp(self):
        if hasattr(Process, "client"):
            Process.client.flushdb()

    def test_init_establishes_connection_once(self):
        p1 = Process()
        p2 = Process()

        assert Process.client is p1.client
        assert Process.client is p2.client
        assert p1.client is p2.client

        p1.stop()
        p2.stop()

    def test_init_ignores_differing_connection_setting(self):
        p1 = Process()
        p2 = Process(host="notlocalhost", port=123)

        p2_connection = p2.client.connection_pool.connection_kwargs
        assert p2_connection['host'] != "notlocalhost"
        assert p2_connection['port'] != 123

        p1.stop()
        p2.stop()

    def test_init_create_heartbeat_data(self):
        p = Process()

        assert p.heartbeat_interval == 10
        assert p.heartbeat_hash_name == "phonon_heartbeat"
        assert isinstance(p._Process__heartbeat_ref, Reference)

        p.stop()

    @mock.patch('time.time', side_effect=get_milliseconds_timestamp)
    def test_heartbeat_updates(self, time_time_patched):
        p = Process(heartbeat_interval=.1, recover_failed_processes=False)

        current_time = p.client.hget(p.heartbeat_hash_name, p.id)

        time.sleep(.5)

        new_time = p.client.hget(p.heartbeat_hash_name, p.id)

        assert int(new_time) >= int(current_time) + (p.heartbeat_interval * 1000)

        p.stop()

    @mock.patch('time.time', side_effect=get_milliseconds_timestamp)
    def test_multiple_heartbeats_update(self, time_time_patched):
        p1 = Process(heartbeat_interval=.1, recover_failed_processes=False)
        p2 = Process(heartbeat_interval=.1, recover_failed_processes=False)

        current_time_1 = p1.client.hget(p1.heartbeat_hash_name, p1.id)
        current_time_2 = p2.client.hget(p2.heartbeat_hash_name, p2.id)

        time.sleep(1)

        new_time_1 = p1.client.hget(p1.heartbeat_hash_name, p1.id)
        new_time_2 = p2.client.hget(p2.heartbeat_hash_name, p2.id)

        assert int(new_time_1) >= int(current_time_1) + (p1.heartbeat_interval * 1000)
        assert int(new_time_2) >= int(current_time_2) + (p2.heartbeat_interval * 1000)

        p1.stop()
        p2.stop()

    @mock.patch('time.time', side_effect=get_milliseconds_timestamp)
    def test_stop_cleans_up(self, time_time_patched):
        p1 = Process(heartbeat_interval=.1, recover_failed_processes=False)
        p2 = Process(heartbeat_interval=.1, recover_failed_processes=False)

        current_time_1 = p1.client.hget(p1.heartbeat_hash_name, p1.id)

        assert p2._Process__heartbeat_ref.count() is 2
        p1.stop()

        time.sleep(.5)

        new_time_2 = p1.client.hget(p1.heartbeat_hash_name, p1.id)

        assert new_time_2 == current_time_1
        assert p2._Process__heartbeat_ref.count() is 1

        p2.stop()

        assert p2._Process__heartbeat_ref.count() is 0

    def test_process_registry_tracks_references(self):
        p1 = Process()
        a = p1.create_reference("foo")

        assert p1.client.hexists(p1.registry_key, a.resource_key)

        a.dereference()

        assert p1.client.hexists(p1.registry_key, a.resource_key) is False

        p1.stop()

    def test_lock_wrapper(self):
        p1 = Process()
        lock = Process.Lock(p1, "foo", True)
        lock2 = Process.Lock(p1, "foo", False)

        assert lock.block is True
        assert lock2.block is False
        assert lock.lock_key == "foo.lock"
        assert lock.client is p1.client
        assert lock._Lock__process is p1
        assert lock._Lock__lock is None

        with lock as acquired_lock:
            assert lock._Lock__lock is acquired_lock
            assert(isinstance(acquired_lock, redis.lock.LuaLock))

            self.assertRaises(Process.AlreadyLocked, lock2.__enter__)

        p1.stop()

    def test_remove_from_registry(self):
        p1 = Process()

        assert len(p1.get_registry()) == 0

        p1.create_reference("test")
        assert len(p1.get_registry()) == 1

        p1.remove_from_registry("test")
        assert len(p1.get_registry()) == 0

        p1.stop()

    def test_process_recovery(self):
        p1 = Process(heartbeat_interval=.1)
        p2 = Process(heartbeat_interval=.1)
        p1._Process__heartbeat_timer.cancel()
        p2._Process__heartbeat_timer.cancel()

        assert len(p1.get_registry()) == 0

        dead_process_registry = p1._Process__get_registry_key("12345")
        p1.add_to_registry("r1", dead_process_registry)
        p1.add_to_registry("r2", dead_process_registry)
        p1.client.hset(p1.heartbeat_hash_name, "12345", int(int(time.time()) - 6 * p1.heartbeat_interval))

        p1._Process__recover_failed_processes()

        assert len(p1.get_registry()) == 2
        assert "12345" not in p1.client.hgetall(p1.heartbeat_hash_name)

        ref = p1.create_reference("r1")
        ref_list = ref.nodelist.get_all_nodes()
        assert "12345" not in ref_list
        assert p1.id in ref_list

        ref = p1.create_reference("r2")
        ref_list = ref.nodelist.get_all_nodes()
        assert "12345" not in ref_list
        assert p1.id in ref_list

        p1.stop()
        p2.stop()

    def test_process_self_recovery(self):
        p1 = Process(heartbeat_interval=.1)

        p1.create_reference("test")
        p1._Process__heartbeat_timer.cancel()

        original_id = p1.id
        p1.client.hset(p1.heartbeat_hash_name, original_id, int(int(time.time()) - 6 * p1.heartbeat_interval))

        p1._Process__recover_failed_processes()

        assert p1.id != original_id
        assert len(p1.get_registry()) == 0

        p1._Process__update_heartbeat()
        p1._Process__heartbeat_timer.cancel()

        assert len(p1.get_registry()) == 1

        p1.stop()

    def test_no_active_process(self):
        p1 = Process(heartbeat_interval=.1)
        ref = p1.create_reference("test")
        p1._Process__heartbeat_timer.cancel()
        original_id = p1.id
        p1.client.hset(p1.heartbeat_hash_name, p1.id, int(int(time.time()) - 6 * p1.heartbeat_interval))

        p1.id = unicode(uuid.uuid4())
        p1._Process__recover_failed_processes()

        assert original_id in p1.client.hgetall(p1.heartbeat_hash_name)

        # Proces comes back alive
        p1.id = original_id
        p1._Process__recover_failed_processes()
        p1._Process__update_heartbeat()

        assert original_id not in p1.client.hgetall(p1.heartbeat_hash_name)

        p1.stop()

    def test_failed_process_locked(self):

        p1 = Process(heartbeat_interval=.1)
        Process.BLOCKING_TIMEOUT = 1
        p1._Process__heartbeat_timer.cancel()

        assert len(p1.get_registry()) == 0

        dead_process_registry = p1._Process__get_registry_key("12345")
        p1.add_to_registry("r1", dead_process_registry)
        p1.add_to_registry("r2", dead_process_registry)
        p1.client.hset(p1.heartbeat_hash_name, "12345", int(int(time.time()) - 6 * p1.heartbeat_interval))

        with p1.lock(p1._Process__get_registry_key("12345")):
            p1._Process__recover_failed_processes()

        Process.BLOCKING_TIMEOUT = 500
        p1.stop()

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

class ReferenceTest(unittest.TestCase):

    def setUp(self):
        self.client = redis.StrictRedis(host='localhost')
        self.client.flushall()

    def tearDown(self):
        self.client = redis.StrictRedis(host='localhost')
        self.client.flushall()

    def test_init_creates_keys(self):
        p = Process()
        a = p.create_reference('foo')
        assert a.nodelist.nodelist_key == 'phonon_foo.nodelist'
        assert a.resource_key == 'foo'
        assert a.times_modified_key == 'phonon_foo.times_modified'
        p.stop()

    def test_lock_is_non_reentrant(self):
        p = Process()
        a = p.create_reference('foo')

        with a.lock():
            try:
                lock = None
                with a.lock(block=False) as lock:
                    pass
            except:
                assert lock is None
        p.stop()

    def test_lock_acquires_and_releases(self):
        p = Process()
        a = p.create_reference('foo')
        with a.lock():
            try:
                lock = None
                with a.lock(block=False) as lock:
                    pass
            except:
                assert lock is None

        with a.lock():
            pass

        p.stop()

    def test_nodelist_updates_multinodes(self):
        p1 = Process()
        p2 = Process()
        p3 = Process()

        import threading
        t = threading.Thread(target=p1.create_reference, args=('foo',))
        t2 = threading.Thread(target=p2.create_reference, args=('foo',))

        t.start()
        t2.start()
        a = p3.create_reference("foo")

        t.join()
        t2.join()

        nodes = a.nodelist.get_all_nodes()
        assert p1.id in nodes
        assert p2.id in nodes

        p1.stop()
        p2.stop()
        p3.stop()

    def test_nodelist_dereferences_multinodes(self):
        p1 = Process()
        p2 = Process()

        a = p1.create_reference('foo')
        b = p2.create_reference('foo')

        import threading
        t = threading.Thread(target=a.dereference)
        t2 = threading.Thread(target=b.dereference)

        t.start()
        t2.start()

        t.join()
        t2.join()

        nodes = a.nodelist.get_all_nodes()
        assert nodes == {}

        p1.stop()
        p2.stop()

    def test_refresh_session_sets_time_initially(self):
        p = Process()
        a = p.create_reference('foo')
        a.refresh_session()
        nodes = a.nodelist.get_all_nodes()
        assert a.nodelist.count() == 1, "{0}: {1}".format(nodes, len(nodes))
        assert isinstance(a.nodelist.get_last_updated(p.id), datetime.datetime)
        p.stop()

    def test_refresh_session_resets_time(self):
        p = Process()
        a = p.create_reference('foo')
        start = a.nodelist.get_last_updated(p.id)
        a.refresh_session()
        end = a.nodelist.get_last_updated(p.id)
        assert end > start
        assert isinstance(end, datetime.datetime)
        assert isinstance(start, datetime.datetime)
        p.stop()

    def test_get_and_increment_times_modified(self):
        p = Process()
        a = p.create_reference('foo')
        assert a.get_times_modified() == 0
        a.increment_times_modified()
        assert a.get_times_modified() == 1, a.get_times_modified()
        a.increment_times_modified()
        a.increment_times_modified()
        assert a.get_times_modified() == 3
        p2 = Process()
        b = p2.create_reference('foo')
        b.increment_times_modified()
        assert b.get_times_modified() == 4
        p.stop()
        p2.stop()

    def test_count_for_one_reference(self):
        p = Process()
        a = p.create_reference('foo')
        assert a.count() == 1
        p.stop()

    def test_count_for_multiple_references(self):
        p = Process()
        a = p.create_reference('foo')

        p2 = Process()
        b = p2.create_reference('foo')

        p3 = Process()
        c = p3.create_reference('foo')

        assert a.count() == b.count()
        assert b.count() == c.count()
        assert c.count() == 3

        p.stop()
        p2.stop()
        p3.stop()

    def test_count_decrements_when_dereferenced(self):
        p = Process()
        a = p.create_reference('foo')

        p2 = Process()
        b = p2.create_reference('foo')

        p3 = Process()
        c = p3.create_reference('foo')

        assert a.count() == b.count()
        assert b.count() == c.count()
        assert c.count() == 3, c.count()
        a.dereference()
        assert a.count() == 2, b.count()
        b.dereference()
        assert a.count() == 1, a.count()
        c.dereference()
        assert a.count() == 0, a.nodelist.get_all_nodes()

        p.stop()
        p2.stop()
        p3.stop()

    def test_dereference_removes_pid_from_pids(self):
        p = Process()
        a = p.create_reference('foo')

        p2 = Process()
        b = p2.create_reference('foo')

        pids = a.nodelist.get_all_nodes()
        assert a._Reference__process.id in pids
        assert b._Reference__process.id in pids
        a.dereference()
        pids = a.nodelist.get_all_nodes()
        assert a._Reference__process.id not in pids
        b.dereference()
        pids = a.nodelist.get_all_nodes()
        assert b._Reference__process.id not in pids
        assert len(pids) == 0

        p.stop()
        p2.stop()

    def test_dereference_cleans_up(self):
        p = Process()
        a = p.create_reference('foo')

        p2 = Process()
        b = p2.create_reference('foo')

        pids = a.nodelist.get_all_nodes()
        assert a._Reference__process.id in pids
        assert b._Reference__process.id in pids
        a.dereference()
        pids = a.nodelist.get_all_nodes()
        assert a._Reference__process.id not in pids
        b.dereference()
        pids = a.nodelist.get_all_nodes()
        assert b._Reference__process.id not in pids
        assert len(pids) == 0
        assert a.nodelist.get_all_nodes() == {}, a.nodelist.get_all_nodes()
        assert Process.client.get(a.resource_key) is None, Process.client.get(a.resource_key)
        assert Process.client.get(a.times_modified_key) is None, Process.client.get(a.times_modified_key)

        p.stop()
        p2.stop()

    def test_dereference_handles_when_never_modified(self):
        p = Process()
        a = p.create_reference('foo')
        pids = a.nodelist.get_all_nodes()
        assert len(pids) == 1, pids

        a.dereference()
        pids = a.nodelist.get_all_nodes()
        assert len(pids) == 0, pids

        p.stop()

    def test_dereference_calls_callback(self):
        p = Process()
        a = p.create_reference('foo')

        p2 = Process()
        b = p2.create_reference('foo')
        foo = [1]

        def callback(*args, **kwargs):
            foo.pop()

        b.dereference(callback, args=('first',))
        assert len(foo) == 1
        a.dereference(callback, args=('second',))
        assert len(foo) == 0

        p.stop()
        p2.stop()


class UserUpdate(Update):

    def merge(self, user_update):
        for k, v in (user_update.doc or {}).items():
            if k not in self.doc:
                self.doc[k] = float(v)
            else:
                self.doc[k] += float(v)

    def execute(self):
        self.called = True
        obj = {
            'doc': self.doc,
            'spec': self.spec,
            'collection': self.collection,
            'database': self.database
        }
        client = self._Update__process.client
        client.set("{0}.write".format(self.resource_id), json.dumps(obj))


class UserUpdateCustomField(Update):

    def __init__(self, my_field, *args, **kwargs):
        self.my_field = my_field
        super(UserUpdateCustomField, self).__init__(*args, **kwargs)

    def merge(self, user_update):
        for k, v in (user_update.my_field or {}).items():
            if k not in self.my_field:
                self.my_field[k] = float(v)
            else:
                self.my_field[k] += float(v)

    def execute(self):
        self.called = True
        obj = {
            'my_field': self.my_field,
            'spec': self.spec,
            'collection': self.collection,
            'database': self.database
        }
        client = self._Update__process.client
        client.set("{0}.write".format(self.resource_id), json.dumps(obj))

    def state(self):
        return {"my_field": self.my_field}

    def clear(self):
        return {"my_field": {}}


class UpdateTest(unittest.TestCase):

    def test_process(self):
        p = Process()
        a = UserUpdate(process=p, _id='123', database='test', collection='user',
                       spec={'_id': 123}, doc={'a': 1., 'b': 2., 'c': 3.})
        self.assertIs(p, a.process())
        self.assertIs(p.client, a.process().client)

        p.stop()

    def test_initializer_updates_ref_count(self):
        p = Process()
        a = UserUpdate(process=p, _id='123', database='test', collection='user',
                       spec={'_id': 123}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=False)

        client = a._Update__process.client
        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 1
        assert a._Update__process.id in nodelist

        p.stop()

    def test_cache_caches(self):
        p = Process()
        a = UserUpdate(process=p, _id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=False)
        a.cache()
        client = a._Update__process.client
        cached = pickle.loads(client.get(a.resource_id))
        state = cached.__getstate__()
        del state['resource_id']
        del state['hard_expiration']
        assert state == {u'doc': {u'a': 1.0, u'c': 3.0, u'b': 2.0},
                         u'spec': {u'_id': 12345},
                         u'collection': u'user',
                         u'database': u'test'}

        client.flushall()
        b = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 6.}, init_cache=False)
        p2 = Process()
        c = UserUpdate(process=p2, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 6.}, init_cache=False)

        client = a._Update__process.client
        assert client.get(b.resource_id) is None, client.get(b.resource_id)

        assert c.ref.count() == 2, c.ref.count()
        b.end_session()
        assert c.ref.count() == 1, c.ref.count()
        cached = pickle.loads(client.get(b.resource_id))

        observed_doc = cached.doc
        observed_spec = cached.spec
        observed_coll = cached.collection
        observed_db = cached.database

        expected_doc = {u'd': 4.0, u'e': 5.0, u'f': 6.0}
        expected_spec = {u'_id': 456}
        expected_coll = u'user'
        expected_db = u'test'

        for k, v in observed_doc.items():
            assert expected_doc[k] == v, k
        for k, v in expected_doc.items():
            assert observed_doc[k] == v, k

        assert c.ref.count() == 1, c.ref.count()

        assert observed_spec == expected_spec
        assert observed_coll == expected_coll
        assert observed_db == expected_db

        c.end_session()
        assert c.ref.count() == 0, c.ref.count()
        assert client.get(c.resource_id) is None, client.get(c.resource_id)

        target = json.loads(client.get("{0}.write".format(b.resource_id)) or "{}")

        expected_doc = {u'd': 8.0, u'e': 10.0, u'f': 12.0}
        expected_spec = {u'_id': 456}
        expected_coll = u'user'
        expected_db = u'test'

        for k, v in target.get('doc').items():
            assert expected_doc[k] == v
        for k, v in expected_doc.items():
            assert target['doc'][k] == v

        p.stop()
        p2.stop()

    def test_data_is_recovered(self):
        p = Process()
        client = p.client

        client.flushall()

        a = UserUpdate(process=p, _id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=True)

        p._Process__heartbeat_timer.cancel()

        assert len(p.get_registry()) == 1

        cached = pickle.loads(client.get(a.resource_id) or "{}")
        state = cached.__getstate__()
        del state['resource_id']
        del state['hard_expiration']
        assert state == {u'doc': {u'a': 1.0, u'c': 3.0, u'b': 2.0},
                         u'spec': {u'_id': 12345},
                         u'collection': u'user',
                         u'database': u'test'}

        p.client.hset(p.heartbeat_hash_name, p.id, int(time.time()) - 6 * p.heartbeat_interval)

        p.id = unicode(uuid.uuid4())
        p.registry_key = p._Process__get_registry_key(p.id)

        assert len(p.get_registry()) == 0

        p._Process__update_heartbeat()
        p._Process__heartbeat_timer.cancel()

        assert len(p.get_registry()) == 1

        a = UserUpdate(process=p, _id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=True)

        cached = pickle.loads(client.get(a.resource_id) or "{}")

        state = cached.__getstate__()
        del state['resource_id']
        state == {u'doc': {u'a': 2.0, u'c': 6.0, u'b': 4.0},
                  u'spec': {u'_id': 12345},
                  u'collection': u'user',
                  u'database': u'test'}

        p.stop()

    def test_session_refreshes(self):
        p = Process()
        a = UserUpdate(process=p, _id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, soft_session=5)
        b = UserUpdate(process=p, _id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, soft_session=5)

        old_soft_expiration = a.soft_expiration
        a.refresh(b)

        assert a.soft_expiration >= a.soft_expiration
        p.stop()

    def test_end_session_raises_when_deadlocked(self):
        pass

    def test_end_session_executes_for_unique_references(self):
        pass

    def test_force_expiry_init_cache(self):
        p = Process()
        client = p.client
        a = UserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=True)
        b = UserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.}, init_cache=True)

        assert a.ref.count() == 1, a.ref.count()
        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 1
        assert b.ref.count() == 1, b.ref.count()
        nodelist = b.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 1

        assert a.ref.get_times_modified() == 2, a.ref.get_times_modified()
        a.force_expiry()

        assert a.ref.count() == 0, a.ref.count()
        assert client.get(a.resource_id) is None, client.get(a.resource_id)
        assert b.ref.count() == 0, b.ref.count()
        assert client.get(b.resource_id) is None, client.get(b.resource_id)

        target = json.loads(client.get("{0}.write".format(a.resource_id)) or "{}")
        observed_doc = target.get('doc')
        observed_spec = target.get('spec')
        observed_coll = target.get('collection')
        observed_db = target.get('database')
        # expect doc to be combination of a and b
        expected_doc = {u'a': 5.0, u'b': 7.0, u'c': 9.0}
        expected_spec = {u'_id': 123456}
        expected_coll = u'user'
        expected_db = u'test'

        for k, v in observed_doc.items():
            assert expected_doc[k] == v, k
        for k, v in expected_doc.items():
            assert observed_doc[k] == v, k

        assert observed_coll == expected_coll
        assert observed_db == expected_db
        assert observed_spec == expected_spec

        p.stop()

    def test_force_expiry_two_processes(self):
        p = Process()
        p2 = Process()
        a = UserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        b = UserUpdate(process=p2, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.}, init_cache=True)

        client = a._Update__process.client

        assert a.ref.count() == 2, a.ref.count()
        assert b.ref.count() == 2, b.ref.count()
        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 2
        nodelist = b.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 2

        assert a.ref.get_times_modified() == 1, a.ref.get_times_modified()
        a.force_expiry()

        assert a.ref.count() == 0, a.ref.count()
        assert client.get(a.resource_id) is None, client.get(a.resource_id)
        assert b.ref.count() == 0, b.ref.count()
        assert client.get(b.resource_id) is None, client.get(b.resource_id)

        target = json.loads(client.get("{0}.write".format(a.resource_id)) or "{}")
        observed_doc = target.get('doc')
        observed_spec = target.get('spec')
        observed_coll = target.get('collection')
        observed_db = target.get('database')
        # expect doc to be combination of a and b
        expected_doc = {u'a': 5.0, u'b': 7.0, u'c': 9.0}
        expected_spec = {u'_id': 123456}
        expected_coll = u'user'
        expected_db = u'test'

        for k, v in observed_doc.items():
            assert expected_doc[k] == v, k
        for k, v in expected_doc.items():
            assert observed_doc[k] == v, k

        assert observed_coll == expected_coll
        assert observed_db == expected_db
        assert observed_spec == expected_spec

        p.stop()
        p2.stop()

    def test_force_expiry_multiple_processes_caching(self):
        p = Process()
        p2 = Process()
        p3 = Process()
        a = UserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        b = UserUpdate(process=p2, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.})
        c = UserUpdate(process=p3, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})

        client = a._Update__process.client

        assert a.ref.count() == 3, a.ref.count()
        assert b.ref.count() == 3, b.ref.count()
        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 3
        nodelist = b.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 3

        b.cache()
        b.end_session()
        assert a.ref.get_times_modified() == 1, a.ref.get_times_modified()
        a.force_expiry()

        assert a.ref.count() == 0, a.ref.count()
        assert client.get(a.resource_id) is None, client.get(a.resource_id)
        assert b.ref.count() == 0, b.ref.count()
        assert client.get(b.resource_id) is None, client.get(b.resource_id)

        target = json.loads(client.get("{0}.write".format(a.resource_id)) or "{}")
        observed_doc = target.get('doc')
        observed_spec = target.get('spec')
        observed_coll = target.get('collection')
        observed_db = target.get('database')
        # expect doc to be combination of a and b
        expected_doc = {u'a': 5.0, u'b': 7.0, u'c': 9.0}
        expected_spec = {u'_id': 123456}
        expected_coll = u'user'
        expected_db = u'test'

        for k, v in observed_doc.items():
            assert expected_doc[k] == v, k
        for k, v in expected_doc.items():
            assert observed_doc[k] == v, k

        assert observed_coll == expected_coll
        assert observed_db == expected_db
        assert observed_spec == expected_spec

        c.cache()
        c.end_session()
        target = json.loads(client.get("{0}.write".format(c.resource_id)) or "{}")
        observed_doc = target.get('doc')
        observed_spec = target.get('spec')
        observed_coll = target.get('collection')
        observed_db = target.get('database')
        # expect doc to be only c
        expected_doc = {u'a': 7.0, u'b': 8.0, u'c': 9.0}
        expected_spec = {u'_id': 123456}
        expected_coll = u'user'
        expected_db = u'test'

        for k, v in observed_doc.items():
            assert expected_doc[k] == v, k
        for k, v in expected_doc.items():
            assert observed_doc[k] == v, k

        assert observed_coll == expected_coll
        assert observed_db == expected_db
        assert observed_spec == expected_spec

        p.stop()
        p2.stop()
        p3.stop()

    def test_force_expiry_same_process_caching(self):
        p = Process()
        a = UserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        b = UserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.})
        c = UserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})

        client = a._Update__process.client

        assert a.ref.count() == 1, a.ref.count()
        assert b.ref.count() == 1, b.ref.count()
        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 1
        nodelist = b.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 1

        b._Update__cache()
        assert a.ref.get_times_modified() == 1, a.ref.get_times_modified()
        a.force_expiry()

        assert a.ref.count() == 0, a.ref.count()
        assert client.get(a.resource_id) is None, client.get(a.resource_id)
        assert b.ref.count() == 0, b.ref.count()
        assert client.get(b.resource_id) is None, client.get(b.resource_id)

        target = json.loads(client.get("{0}.write".format(a.resource_id)) or "{}")
        observed_doc = target.get('doc')
        observed_spec = target.get('spec')
        observed_coll = target.get('collection')
        observed_db = target.get('database')
        # expect doc to be combination of a and b
        expected_doc = {u'a': 5.0, u'b': 7.0, u'c': 9.0}
        expected_spec = {u'_id': 123456}
        expected_coll = u'user'
        expected_db = u'test'

        for k, v in observed_doc.items():
            assert expected_doc[k] == v, k
        for k, v in expected_doc.items():
            assert observed_doc[k] == v, k

        assert observed_coll == expected_coll
        assert observed_db == expected_db
        assert observed_spec == expected_spec

        c.cache()
        c.end_session()
        target = json.loads(client.get("{0}.write".format(c.resource_id)) or "{}")
        observed_doc = target.get('doc')
        observed_spec = target.get('spec')
        observed_coll = target.get('collection')
        observed_db = target.get('database')
        # expect doc to be only c
        expected_doc = {u'a': 7.0, u'b': 8.0, u'c': 9.0}
        expected_spec = {u'_id': 123456}
        expected_coll = u'user'
        expected_db = u'test'

        for k, v in observed_doc.items():
            assert expected_doc[k] == v, k
        for k, v in expected_doc.items():
            assert observed_doc[k] == v, k

        assert observed_coll == expected_coll
        assert observed_db == expected_db
        assert observed_spec == expected_spec

        p.stop()

    def test_force_expiry_caching_conflicts(self):
        p = Process()
        a = UserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        c = UserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})

        p2 = Process()
        b = UserUpdate(process=p2, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.})
        d = UserUpdate(process=p2, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 20., 'b': 21., 'c': 22.})

        client = a._Update__process.client

        assert a.ref.count() == 2, a.ref.count()
        assert b.ref.count() == 2, b.ref.count()
        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 2
        nodelist = b.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 2

        b._Update__cache()
        assert a.ref.get_times_modified() == 1, a.ref.get_times_modified()
        a.force_expiry()

        assert a.ref.count() == 0, a.ref.count()
        assert b.ref.count() == 0, b.ref.count()

        target = json.loads(client.get("{0}.write".format(a.resource_id)) or "{}")
        observed_doc = target.get('doc')
        # expect doc to be combination of a and b
        expected_doc = {u'a': 5.0, u'b': 7.0, u'c': 9.0}

        for k, v in observed_doc.items():
            assert expected_doc[k] == v, k
        for k, v in expected_doc.items():
            assert observed_doc[k] == v, k

        e = UserUpdate(process=p2, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 30., 'b': 31., 'c': 32.})

        c._Update__cache()
        d.end_session()

        target = json.loads(client.get("{0}.write".format(c.resource_id)) or "{}")
        observed_doc = target.get('doc')
        observed_spec = target.get('spec')
        observed_coll = target.get('collection')
        observed_db = target.get('database')
        # expect doc to be combination of c and d
        expected_doc = {u'a': 27.0, u'b': 29.0, u'c': 31.0}
        expected_spec = {u'_id': 123456}
        expected_coll = u'user'
        expected_db = u'test'
        for k, v in observed_doc.items():
            assert expected_doc[k] == v, observed_doc
        for k, v in expected_doc.items():
            assert observed_doc[k] == v, observed_doc

        assert observed_coll == expected_coll
        assert observed_db == expected_db
        assert observed_spec == expected_spec

        e.end_session()
        target = json.loads(client.get("{0}.write".format(e.resource_id)) or "{}")
        observed_doc = target.get('doc')
        # expect doc to be only e
        expected_doc = {u'a': 30.0, u'b': 31.0, u'c': 32.0}
        for k, v in observed_doc.items():
            assert expected_doc[k] == v, observed_doc
        for k, v in expected_doc.items():
            assert observed_doc[k] == v, observed_doc

        p.stop()
        p2.stop()


class LruCacheTest(unittest.TestCase):

    def setUp(self):
        self.cache = LruCache(max_entries=5, async=False)
        self.async_cache = LruCache(max_entries=5, async=True)

    def test_purge(self):
        a = self.get_update('a')
        b = self.get_update('b')

        self.cache.set(1, a)
        self.cache.set(2, b)

        assert self.cache.get(1).key == a.key
        assert self.cache.get(2).key == b.key

        a.is_expired = lambda: True
        b.is_expired = lambda: False
        self.cache.purge()

        assert a.called()
        assert not b.called()

    def test_purge_async(self):
        a = self.get_update('a')
        b = self.get_update('b')

        self.async_cache.set(1, a)
        self.async_cache.set(2, b)

        assert self.async_cache.get(1).key == a.key
        assert self.async_cache.get(2).key == b.key

        a.is_expired = lambda: True
        b.is_expired = lambda: False
        self.async_cache.purge()

        retries = 100
        while retries > 0 and not a.called():
            time.sleep(0.01)
            retries += 1

        assert a.called()
        assert not b.called()

    def get_update(self, key):
        class Update(object):

            def __init__(self, key):
                self.key = key
                self.__called = False
                self.soft_expiration = datetime.datetime.now(LOCAL_TZ) + datetime.timedelta(15)
                self.hard_expiration = datetime.datetime.now(LOCAL_TZ) + datetime.timedelta(15)

            def merge(self, other):
                self.__other = other

            def end_session(self):
                self.__called = True

            def assert_end_session_called(self):
                assert self.__called

            def assert_merged(self, other):
                assert other is self.__other

            def refresh(self, other):
                self.soft_expiration = datetime.datetime.now(LOCAL_TZ) + datetime.timedelta(15)
                self.merge(other)

            def is_expired(self):
                return datetime.datetime.now(LOCAL_TZ) > self.hard_expiration

            def called(self):
                return self.__called
        return Update(key)

    def test_set_reorders_repeated_elements_async(self):
        a = self.get_update('a')
        b = self.get_update('b')
        c = self.get_update('c')
        self.async_cache.set(1, a)
        assert self.async_cache.size() == 1
        self.async_cache.set(2, b)
        assert self.async_cache.size() == 2
        self.async_cache.set(1, a)
        assert self.async_cache.size() == 2
        a.assert_merged(a)
        self.async_cache.expire_oldest()

        retries = 100
        while not b.called() and retries > 0:
            time.sleep(0.01)
            retries -= 1

        b.assert_end_session_called()
        assert self.async_cache.size() == 1

    def test_set_reorders_repeated_elements(self):
        a = self.get_update('a')
        b = self.get_update('b')
        c = self.get_update('c')
        self.cache.set(1, a)
        assert self.cache.size() == 1
        self.cache.set(2, b)
        assert self.cache.size() == 2
        self.cache.set(1, a)
        assert self.cache.size() == 2
        a.assert_merged(a)
        self.cache.expire_oldest()

        b.assert_end_session_called()
        assert self.cache.size() == 1

    def test_set_expires_oldest_to_add_new_async(self):
        a = self.get_update('a')
        b = self.get_update('b')
        c = self.get_update('c')
        d = self.get_update('d')
        e = self.get_update('e')
        f = self.get_update('f')

        assert self.async_cache.size() == 0
        self.async_cache.set('a', a)
        assert self.async_cache.size() == 1
        self.async_cache.set('b', b)
        assert self.async_cache.size() == 2
        self.async_cache.set('c', c)
        assert self.async_cache.size() == 3
        self.async_cache.set('d', d)
        assert self.async_cache.size() == 4
        self.async_cache.set('e', e)
        assert self.async_cache.size() == 5
        self.async_cache.set('f', f)
        assert self.async_cache.size() == 5

        retries = 100
        while not a.called() and retries > 0:
            time.sleep(0.01)
            retries -= 1

        a.assert_end_session_called()

    def test_set_expires_oldest_to_add_new(self):
        a = self.get_update('a')
        b = self.get_update('b')
        c = self.get_update('c')
        d = self.get_update('d')
        e = self.get_update('e')
        f = self.get_update('f')

        assert self.cache.size() == 0
        self.cache.set('a', a)
        assert self.cache.size() == 1
        self.cache.set('b', b)
        assert self.cache.size() == 2
        self.cache.set('c', c)
        assert self.cache.size() == 3
        self.cache.set('d', d)
        assert self.cache.size() == 4
        self.cache.set('e', e)
        assert self.cache.size() == 5
        self.cache.set('f', f)
        assert self.cache.size() == 5

        a.assert_end_session_called()

    def test_get_returns_elements(self):
        a = self.get_update('a')
        self.cache.set('a', a)
        assert self.cache.get('a') is a
        assert self.cache.size() == 1
        assert self.cache.get('a') is a

    def test_expire_expires_at_key(self):
        a = self.get_update('a')
        b = self.get_update('b')

        self.cache.set('a', a)
        self.cache.set('b', b)
        assert self.cache.size() == 2

        assert self.cache.get('a') is a
        self.cache.expire('a')
        assert self.cache.size() == 1
        a.assert_end_session_called()

    def test_expire_expires_at_key_async(self):
        a = self.get_update('a')
        b = self.get_update('b')

        self.async_cache.set('a', a)
        self.async_cache.set('b', b)
        assert self.async_cache.size() == 2

        assert self.async_cache.get('a') is a
        self.async_cache.expire('a')
        retries = 100
        while not a.called() and retries > 0:
            time.sleep(0.01)
            retries -= 1

        assert self.async_cache.size() == 1

        a.assert_end_session_called()

    def test_expire_all_expires_all(self):
        updates = [self.get_update('a'),
                   self.get_update('b'),
                   self.get_update('c'),
                   self.get_update('d'),
                   self.get_update('e')]

        for size, update in enumerate(updates):
            self.cache.set(update.key, update)
            assert self.cache.size() == size + 1

        self.cache.expire_all()
        assert self.cache.size() == 0
        for update in updates:
            update.assert_end_session_called()

    def test_expire_all_expires_all_async(self):
        updates = [self.get_update('a'),
                   self.get_update('b'),
                   self.get_update('c'),
                   self.get_update('d'),
                   self.get_update('e')]

        for size, update in enumerate(updates):
            self.async_cache.set(update.key, update)
            assert self.async_cache.size() == size + 1

        self.async_cache.expire_all()
        assert self.async_cache.size() == 0

        retries = 100
        while not all([update.called() for update in updates]) and retries > 0:
            time.sleep(0.01)
            retries -= 1

        for update in updates:
            update.assert_end_session_called()

    def test_failres_are_kept(self):
        class FailingUpdate(object):

            def end_session(self):
                raise Exception("Failed.")

        failing = FailingUpdate()
        self.cache.set('a', failing)
        try:
            self.cache.expire('a')
        except Exception, e:
            pass

        assert self.cache.get_last_failed() is failing

    def test_failres_are_kept_async(self):
        class FailingUpdate(object):

            def end_session(self):
                raise Exception("Failed.")

        failing = FailingUpdate()
        self.async_cache.set('a', failing)
        try:
            self.async_cache.expire('a')
        except Exception, e:
            pass

        retries = 100
        while not self.async_cache.get_last_failed() is failing and retries > 0:
            time.sleep(0.01)
            retries -= 1

        assert self.async_cache.get_last_failed() is failing

    def test_init_cache_merges_properly(self):
        p = Process()
        p.client.flushdb()

        a = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, init_cache=True)
        b = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, init_cache=True)

        self.cache.set('456', a)
        self.cache.set('456', b)

        p2 = Process()
        c = UserUpdate(process=p2, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 3.}, init_cache=True)
        d = UserUpdate(process=p2, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 4.}, init_cache=True)

        self.cache2 = LruCache(max_entries=5)

        self.cache2.set('456', c)
        self.cache2.set('456', d)

        self.cache.expire_all()
        self.cache2.expire_all()

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 20.0, "d": 16.0, "f": 10.0}
        assert written['spec'] == {"_id": 456}
        assert written['collection'] == "user"
        assert written['database'] == "test"

        p.client.flushdb()
        p.stop()
        p2.stop()

    def test_init_cache_merges_properly_async(self):
        p = Process()
        p.client.flushdb()

        a = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, init_cache=True)
        b = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, init_cache=True)

        self.async_cache.set('456', a)
        self.async_cache.set('456', b)

        p2 = Process()
        c = UserUpdate(process=p2, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 3.}, init_cache=True)
        d = UserUpdate(process=p2, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 4.}, init_cache=True)

        self.cache2 = LruCache(max_entries=5, async=True)

        self.cache2.set('456', c)
        self.cache2.set('456', d)

        self.async_cache.expire_all()
        self.cache2.expire_all()

        retries = 100
        while retries > 0 and not hasattr(a, 'called'):
            retries -= 1
            time.sleep(0.01)

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 20.0, "d": 16.0, "f": 10.0}
        assert written['spec'] == {"_id": 456}
        assert written['collection'] == "user"
        assert written['database'] == "test"

        p.client.flushdb()
        p.stop()
        p2.stop()

    def test_init_cache_merges_properly_async(self):
        p = Process()
        p.client.flushdb()

        a = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, init_cache=True)
        b = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, init_cache=True)

        self.async_cache.set('456', a)
        self.async_cache.set('456', b)

        p2 = Process()
        c = UserUpdate(process=p2, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 3.}, init_cache=True)
        d = UserUpdate(process=p2, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 4.}, init_cache=True)

        self.cache2 = LruCache(max_entries=5, async=True)

        self.cache2.set('456', c)
        self.cache2.set('456', d)

        self.async_cache.expire_all()
        self.cache2.expire_all()

        retries = 100
        while not all([hasattr(u, 'called') for u in [a, b, c, d]]) and retries > 0:
            retries -= 1
            time.sleep(0.01)

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 20.0, "d": 16.0, "f": 10.0}
        assert written['spec'] == {"_id": 456}
        assert written['collection'] == "user"
        assert written['database'] == "test"

        p.client.flushdb()
        p.stop()
        p2.stop()

    def test_init_cache_merges_properly_with_custom_fields(self):
        p = Process()
        p.client.flushdb()

        a = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 1.}, process=p, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)
        b = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 2.}, process=p, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)

        self.cache.set('456', a)
        self.cache.set('456', b)

        p2 = Process()
        c = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 3.}, process=p2, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)
        d = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 4.}, process=p2, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)

        self.cache2 = LruCache(max_entries=5)

        self.cache2.set('456', c)
        self.cache2.set('456', d)

        self.cache.expire_all()
        self.cache2.expire_all()

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['my_field'] == {"e": 20.0, "d": 16.0, "f": 10.0}
        assert written['spec'] == {"_id": 456}
        assert written['collection'] == "user"
        assert written['database'] == "test"

        p.client.flushdb()
        p.stop()
        p2.stop()

    def test_init_cache_merges_properly_with_custom_fields_async(self):
        p = Process()
        p.client.flushdb()

        a = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 1.}, process=p, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)
        b = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 2.}, process=p, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)

        self.async_cache.set('456', a)
        self.async_cache.set('456', b)

        p2 = Process()
        c = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 3.}, process=p2, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)
        d = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 4.}, process=p2, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)

        self.cache2 = LruCache(max_entries=5, async=True)

        self.cache2.set('456', c)
        self.cache2.set('456', d)

        self.async_cache.expire_all()
        self.cache2.expire_all()

        retries = 100
        while not all([hasattr(u, 'called') for u in [a, b, c, d]]) and retries > 0:
            retries -= 1
            time.sleep(0.01)

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['my_field'] == {"e": 20.0, "d": 16.0, "f": 10.0}
        assert written['spec'] == {"_id": 456}
        assert written['collection'] == "user"
        assert written['database'] == "test"

        p.client.flushdb()
        p.stop()
        p2.stop()

    def test_init_cache_merges_properly_with_custom_fields_async(self):
        p = Process()
        p.client.flushdb()

        a = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 1.}, process=p, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)
        b = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 2.}, process=p, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)

        self.async_cache.set('456', a)
        self.async_cache.set('456', b)

        p2 = Process()
        c = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 3.}, process=p2, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)
        d = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 4.}, process=p2, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)

        self.cache2 = LruCache(max_entries=5, async=True)

        self.cache2.set('456', c)
        self.cache2.set('456', d)

        self.async_cache.expire_all()
        self.cache2.expire_all()

        retries = 100
        while not all([hasattr(u, 'called') for u in [a, b, c, d]]) and retries > 0:
            time.sleep(0.01)
            retries -= 1

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['my_field'] == {"e": 20.0, "d": 16.0, "f": 10.0}
        assert written['spec'] == {"_id": 456}
        assert written['collection'] == "user"
        assert written['database'] == "test"

        p.client.flushdb()
        p.stop()
        p2.stop()

    def test_cache_handles_soft_sessions(self):
        p = Process()

        a = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, soft_session=.005)
        b = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, soft_session=.005)

        self.cache.set('456', a)
        time.sleep(.04)
        set_return = self.cache.set('456', b)

        assert set_return is False
        written = p.client.get('{0}.write'.format(a.resource_id))
        assert written is None

        time.sleep(.04)
        get_return = self.cache.get('456')
        assert get_return is None

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 10.0, "d": 8.0, "f": 3.0}

        p.client.flushdb()
        p.stop()

    def test_cache_handles_soft_sessions_async(self):
        p = Process()
        p.client.flushdb()
        a = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, soft_session=.01)
        b = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, soft_session=.01)

        self.async_cache.set('456', a)
        set_return = self.async_cache.set('456', b)

        time.sleep(0.01)

        assert set_return is False
        written = p.client.get('{0}.write'.format(a.resource_id))
        assert written is None, written

        get_return = self.async_cache.get('456')
        assert get_return is None, get_return

        retries = 100
        while not any([hasattr(u, 'called') for u in [a, b]]) and retries > 0:
            retries -= 1
            time.sleep(0.01)

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 10.0, "d": 8.0, "f": 3.0}

        p.client.flushdb()
        p.stop()

    def test_cache_ends_expired_sessions(self):
        p = Process()

        a = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        c = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)

        self.cache.set('456', a)
        time.sleep(.04)
        set_return = self.cache.set('456', b)

        assert set_return is None

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 10.0, "d": 8.0, "f": 3.0}

        self.cache.set('456', c)
        time.sleep(.04)

        get_return = self.cache.get('456')

        assert get_return is None

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))

        assert written['doc'] == {"e": 5.0, "d": 4.0, "f": 1.0}

        p.client.flushdb()
        p.stop()

    def test_cache_ends_expired_sessions_async(self):
        p = Process()

        a = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        c = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)

        self.async_cache.set('456', a)
        time.sleep(.04)
        set_return = self.async_cache.set('456', b)

        assert set_return is None

        retries = 100
        while not hasattr(a, 'called') and retries > 0:
            retries -= 1
            time.sleep(0.01)

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 10.0, "d": 8.0, "f": 3.0}

        self.async_cache.set('456', c)
        time.sleep(.04)

        get_return = self.async_cache.get('456')

        assert get_return is None

        retries = 100
        while not hasattr(c, 'called') and retries > 0:
            time.sleep(0.01)
            retries -= 1

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))

        assert written['doc'] == {"e": 5.0, "d": 4.0, "f": 1.0}

        p.client.flushdb()
        p.stop()

    def test_cache_ends_multiprocess_expired_sessions(self):
        p = Process()
        p2 = Process()

        self.cache2 = LruCache(max_entries=5)

        a = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        c = UserUpdate(process=p2, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 1., 'e': 2., 'f': 3.}, hard_session=.005)

        self.cache.set('456', a)
        time.sleep(.04)
        set_return = self.cache.set('456', b)
        set_return_2 = self.cache2.set('456', c)

        assert set_return is None
        assert set_return_2 is True

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 10.0, "d": 8.0, "f": 3.0}

        get_return_2 = self.cache2.get('456')
        assert get_return_2 is None

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 2.0, "d": 1.0, "f": 3.0}

        p.client.flushdb()
        p.stop()
        p2.stop()

    def test_cache_ends_multiprocess_expired_sessions_async(self):
        p = Process()
        p2 = Process()

        self.cache2 = LruCache(max_entries=5, async=True)

        a = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        c = UserUpdate(process=p2, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 1., 'e': 2., 'f': 3.}, hard_session=.005)

        self.cache.set('456', a)
        time.sleep(.04)
        set_return = self.cache.set('456', b)
        set_return_2 = self.cache2.set('456', c)

        assert set_return is None
        assert set_return_2 is True

        retries = 100
        while not hasattr(a, 'called') and retries > 0:
            retries -= 1
            time.sleep(0.01)

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 10.0, "d": 8.0, "f": 3.0}

        get_return_2 = self.cache2.get('456')
        assert get_return_2 is None

        retries = 100
        while not hasattr(c, 'called') and retries > 0:
            retries -= 1
            time.sleep(0.01)

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 2.0, "d": 1.0, "f": 3.0}

        p.client.flushdb()
        p.stop()
        p2.stop()

    def test_cache_ends_multiprocess_expired_sessions_async2(self):
        p = Process()
        p2 = Process()

        self.cache2 = LruCache(max_entries=5, async=True)

        a = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = UserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        c = UserUpdate(process=p2, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 1., 'e': 2., 'f': 3.}, hard_session=.005)

        self.async_cache.set('456', a)
        time.sleep(.04)
        set_return = self.async_cache.set('456', b)
        set_return_2 = self.cache2.set('456', c)

        assert set_return is None
        assert set_return_2 is True

        retries = 100
        while not hasattr(a, 'called') and retries > 0:
            retries -= 1
            time.sleep(0.01)

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 10.0, "d": 8.0, "f": 3.0}

        get_return_2 = self.cache2.get('456')
        assert get_return_2 is None

        retries = 100
        while not hasattr(c, 'called') and retries > 0:
            retries -= 1
            time.sleep(0.01)

        written = json.loads(p.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 2.0, "d": 1.0, "f": 3.0}

        p.client.flushdb()
        p.stop()
        p2.stop()
