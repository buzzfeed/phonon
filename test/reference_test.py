import unittest
import redis
import datetime
import threading

from phonon.process import Process

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
