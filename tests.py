import unittest
import json
import redis
from dateutil import parser
import datetime
import pytz

from disref import Reference
from disref.update import Update

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

    def test_refresh_session_sets_time_initially(self):
        a = Reference(1, 'foo')
        a.refresh_session()
        reflist = json.loads(a.client.get(a.reflist_key) or "{}")
        assert len(reflist) == 1, "{0}: {1}".format(reflist, len(reflist))
        assert isinstance(parser.parse(reflist[u'1']), datetime.datetime)

    def test_refresh_session_resets_time(self):
        a = Reference(1, 'foo')
        reflist = json.loads(a.client.get(a.reflist_key) or "{}")
        start = parser.parse(reflist[u'1'])
        a.refresh_session()
        reflist = json.loads(a.client.get(a.reflist_key) or "{}")
        end = parser.parse(reflist[u'1'])
        assert end > start
        assert isinstance(end, datetime.datetime)
        assert isinstance(start, datetime.datetime)

    def test_get_and_increment_times_modified(self):
        a = Reference(1, 'foo')
        assert a.get_times_modified() == 0
        a.increment_times_modified()
        assert a.get_times_modified() == 1, a.get_times_modified()
        a.increment_times_modified()
        a.increment_times_modified()
        assert a.get_times_modified() == 3
        b = Reference(2, 'foo')
        b.increment_times_modified() 
        assert b.get_times_modified() == 4

    def test_count_for_one_reference(self):
        a = Reference(1, 'foo')
        assert a.count() == 1

    def test_count_for_multiple_references(self):
        a = Reference(1, 'foo')
        b = Reference(2, 'foo')
        c = Reference(3, 'foo')
        assert a.count() == b.count()
        assert b.count() == c.count()
        assert c.count() == 3

    def test_count_decrements_when_dereferenced(self):
        a = Reference(1, 'foo')
        b = Reference(2, 'foo')
        c = Reference(3, 'foo')
        assert a.count() == b.count()
        assert b.count() == c.count()
        assert c.count() == 3
        a.dereference()
        assert a.count() == 2
        b.dereference()
        assert a.count() == 1
        c.dereference() 
        assert a.count() == 0, a.client.get(a.reflist_key)

    def test_remove_failed_processes(self):
        now = datetime.datetime.now(pytz.utc)
        expired = now - datetime.timedelta(seconds=2 * Reference.TTL + 1)
        pids = {u'1': now.isoformat(),
                u'2': expired.isoformat()}
        a = Reference(5, 'biz')
        target = a.remove_failed_processes(pids)
        assert u'2' not in target, target
        assert u'1' in target, target
        assert target[u'1'] == now.isoformat(), target

    def test_dereference_removes_pid_from_pids(self):
        a = Reference(1, 'foo')
        b = Reference(2, 'foo')
        pids = json.loads(a.client.get(a.reflist_key) or "{}")
        assert u'1' in pids
        assert u'2' in pids
        a.dereference()
        pids = json.loads(a.client.get(a.reflist_key) or "{}")
        assert u'1' not in pids
        b.dereference()
        pids = json.loads(a.client.get(b.reflist_key) or "{}")
        assert u'2' not in pids
        assert len(pids) == 0

    def test_dereference_cleans_up(self):
        a = Reference(1, 'foo')
        b = Reference(2, 'foo')
        pids = json.loads(a.client.get(a.reflist_key) or "{}")
        assert u'1' in pids
        assert u'2' in pids
        a.dereference()
        pids = json.loads(a.client.get(a.reflist_key) or "{}")
        assert u'1' not in pids
        b.dereference()
        pids = json.loads(a.client.get(b.reflist_key) or "{}")
        assert u'2' not in pids
        assert len(pids) == 0
        assert a.client.get(a.reflist_key) == None, a.client.get(a.reflist_key) 
        assert a.client.get(a.resource_key) == None, a.client.get(a.resource_key)
        assert a.client.get(a.times_modified_key) == None, a.client.get(a.times_modified_key)

    def test_dereference_handles_when_never_modified(self):
        a = Reference(1, 'foo')
        pids = json.loads(a.client.get(a.reflist_key) or "{}")
        assert len(pids) == 1, pids

        a.dereference()
        pids = json.loads(a.client.get(a.reflist_key) or "{}")
        assert len(pids) == 0, pids

    def test_dereference_calls_callback(self):
        a = Reference(1, 'foo')
        b = Reference(2, 'foo')
        foo = [1]
        
        def callback(*args, **kwargs):
            print "Callback called", args
            foo.pop()

        b.dereference(callback, args=('first',))
        assert len(foo) == 1
        a.dereference(callback, args=('second',))
        assert len(foo) == 0

class UpdateTest(unittest.TestCase):

    class UserUpdate(Update):

        def merge(self, user_update):
            pass

        def cache(self):
            obj = {
                    'doc': self.doc,
                    'spec': self.spec,
                    'collection': self.collection,
                    'database': self.database
            }
            self.client.set(self.resource_id, json.loads(obj))

        def execute(self):
            obj = {
                    'doc': self.doc,
                    'spec': self.spec,
                    'collection': self.collection,
                    'database': self.database
            }
            self.client.set("{0}.write".format(self.resource_id), json.loads(obj)) 

    def test_initializer_updates_ref_count(self):
        pass

    def test_cache_caches(self):
        pass

    def test_execute_executes(self):
        pass

    def test_merge_merges(self):
        pass

    def test_end_session_raises_when_deadlocked(self):
        pass

    def test_end_session_executes_for_unique_references(self):
        pass

    def test_end_session_caches_for_non_unique_refrences(self):
        pass

    def test_end_session_executes_pulling_from_cache(self):
        pass
