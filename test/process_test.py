import unittest
import mock
import redis
import datetime
import time
import logging
import uuid

from phonon.reference import Reference
from phonon.process import Process

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
