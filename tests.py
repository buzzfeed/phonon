import unittest
import redis

from disref import Reference

class DisRefTest(unittest.TestCase):

    def setUp(self):
        self.client = redis.Redis(host='localhost') 
        self.client.flushall()

    def test_init_establishes_connection_once(self):
        a = Reference(1, 'foo') 
        client = a.client
        b = Reference(2, 'bar')
        assert a.client is b.client
        assert client is b.client

    def test_init_creates_keys(self):
        a = Reference(1, 'foo')
        assert a.reflist_key == 'foo.reflist' 
        assert a.resource_key == 'foo.key' 
        assert a.times_modified_key == 'foo.times_modified'

    def test_lock_is_non_reentrant(self):
        a = Reference(1, 'foo') 
        assert a.lock() == True
        assert a.lock(block=False) == False

    def test_lock_acquires_and_releases(self):
        a = Reference(1, 'foo')
        assert a.lock() == True
        assert a.lock(block=False) == False
        a.release() 
        assert a.lock() == True

    def test_locked_context(self):
        pass

    def test_refresh_session_sets_time_initially(self):
        a = Reference(1, 'foo')
        reflist = self.client.get(a.reflist_key)

    def test_refresh_session_resets_time(self):
        pass

    def test_get_and_increment_times_modified(self):
        pass

    def test_count_for_one_reference(self):
        pass

    def test_count_for_multiple_references(self):
        pass

    def test_count_decrements_when_dereferenced(self):
        pass

    def test_remove_failed_processes(self):
        pass

    def test_dereference_removes_pid_from_pids(self):
        pass

    def test_dereference_cleans_up(self):
        pass

    def test_dereference_handles_when_never_modified(self):
        pass

    def test_dereference_calls_callback(self):
        pass

