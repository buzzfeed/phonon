import unittest
import pickle
import json
import time
import logging
import uuid
import threading

from phonon.process import Process
from phonon.update import Update, ConflictFreeUpdate

logging.disable(logging.CRITICAL)

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
        client = self.process().client
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
        client = self.process().client
        client.set("{0}.write".format(self.resource_id), json.dumps(obj))

    def state(self):
        return {"my_field": self.my_field}

    def clear(self):
        return {"my_field": {}}

class ConflictFreeUserUpdate(ConflictFreeUpdate):

    def execute(self):
        self.called = True
        client = self.process().client
        for key, val in self.doc.items():
            client.incr("{0}.write.{1}".format(self.resource_id, key), int(val))

class BaseUpdateTest(unittest.TestCase):

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

        client = a.process().client
        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 1
        assert a.process().id in nodelist

        p.stop()

    def test_session_refreshes(self):
        p = Process()
        a = UserUpdate(process=p, _id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, soft_session=5)
        b = UserUpdate(process=p, _id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, soft_session=5)

        old_soft_expiration = a.soft_expiration
        a.refresh(b)

        assert a.soft_expiration >= old_soft_expiration
        p.stop()

    def test_end_session_raises_when_deadlocked(self):
        pass

    def test_end_session_executes_for_unique_references(self):
        pass

class UpdateTest(unittest.TestCase):
    def test_cache_caches(self):
        p = Process()
        a = UserUpdate(process=p, _id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=False)
        a.cache()
        client = a.process().client
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

        client = a.process().client
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
        assert state['doc'] == {u'a': 2.0, u'c': 6.0, u'b': 4.0}
        assert state['spec'] == {u'_id': 12345}
        assert state['collection'] == 'user'
        assert state['database'] == 'test'

        p.stop()

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

        client = a.process().client

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

        client = a.process().client

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

        client = a.process().client

        assert a.ref.count() == 1, a.ref.count()
        assert b.ref.count() == 1, b.ref.count()
        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 1
        nodelist = b.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 1

        b._cache()
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

        client = a.process().client

        assert a.ref.count() == 2, a.ref.count()
        assert b.ref.count() == 2, b.ref.count()
        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 2
        nodelist = b.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 2

        b._cache()
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

        c._cache()
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

class ConflictFreeUpdateTest(unittest.TestCase):
    def setUp(self):
        if hasattr(Process, "client"):
            Process.client.flushall()

    def test_cache_caches(self):
        p = Process()

        a = ConflictFreeUserUpdate(process=p, _id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=False)
        client = a.process().client
        client.flushall()
        a.cache()
        data = a._ConflictFreeUpdate__get_cached_doc()
        assert data == {u'a': '1', u'c': '3', u'b': '2'}, data

        client.flushall()
        b = ConflictFreeUserUpdate(process=p, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 6.}, init_cache=False)
        p2 = Process()
        c = ConflictFreeUserUpdate(process=p2, _id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 6.}, init_cache=False)

        assert b._ConflictFreeUpdate__get_cached_doc() == {}, b._ConflictFreeUpdate__get_cached_doc()

        assert c.ref.count() == 2, c.ref.count()
        b.end_session()
        assert c.ref.count() == 1, c.ref.count()
        cached = b._ConflictFreeUpdate__get_cached_doc()

        expected_doc = {u'd': '4', u'e': '5', u'f': '6'}

        for k, v in cached.items():
            assert expected_doc[k] == v, k

        assert c.ref.count() == 1, c.ref.count()

        c.end_session()
        assert c.ref.count() == 0, c.ref.count()
        assert c._ConflictFreeUpdate__get_cached_doc() == {}, c._ConflictFreeUpdate__get_cached_doc()

        merged_doc = {u'd': 8.0, u'e': 10.0, u'f': 12.0}
        executed_doc = {u'd': "8", u'e': "10", u'f': "12"}

        for k, v in c.doc.items():
            assert merged_doc[k] == v
        for k, v in executed_doc.items():
            assert client.get("{0}.write.{1}".format(c.resource_id, k)) == v

        p.stop()
        p2.stop()
    
    def test_data_is_recovered(self):
        p = Process()
        client = p.client

        client.flushall()

        a = ConflictFreeUserUpdate(process=p, _id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=True)

        p._Process__heartbeat_timer.cancel()

        assert len(p.get_registry()) == 1

        cached = a._ConflictFreeUpdate__get_cached_doc()
        assert cached == {u'a': '1', u'c': '3', u'b': '2'}

        p.client.hset(p.heartbeat_hash_name, p.id, int(time.time()) - 6 * p.heartbeat_interval)

        p.id = unicode(uuid.uuid4())
        p.registry_key = p._Process__get_registry_key(p.id)

        assert len(p.get_registry()) == 0

        p._Process__update_heartbeat()
        p._Process__heartbeat_timer.cancel()

        cached = a._ConflictFreeUpdate__get_cached_doc()
        assert cached == {u'a': '1', u'c': '3', u'b': '2'}

        assert len(p.get_registry()) == 1
        a = ConflictFreeUserUpdate(process=p, _id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=True)
        cached = a._ConflictFreeUpdate__get_cached_doc()
        assert cached == {u'a': '2', u'c': '6', u'b': '4'}, cached
        p.stop()

    def test_force_expiry_init_cache(self):
        p = Process()
        client = p.client
        a = ConflictFreeUserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=True)
        b = ConflictFreeUserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.}, init_cache=True)

        assert a.ref.count() == 1, a.ref.count()
        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 1
        assert b.ref.count() == 1, b.ref.count()
        nodelist = b.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 1

        a.force_expiry()

        assert a.ref.count() == 0, a.ref.count()
        assert client.get(a.resource_id) is None, client.get(a.resource_id)
        assert b.ref.count() == 0, b.ref.count()
        assert client.get(b.resource_id) is None, client.get(b.resource_id)

        merged_doc = {u'a': 5.0, u'b': 7.0, u'c': 9.0}
        executed_doc = {u'a': "5", u'b': "7", u'c': "9"}

        for k, v in b.doc.items():
            assert merged_doc[k] == v
        for k, v in executed_doc.items():
            assert client.get("{0}.write.{1}".format(b.resource_id, k)) == v

        p.stop()

    def test_force_expiry_two_processes(self):
        p = Process()
        p2 = Process()
        a = ConflictFreeUserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        b = ConflictFreeUserUpdate(process=p2, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.}, init_cache=True)

        client = a.process().client

        assert a.ref.count() == 2, a.ref.count()
        assert b.ref.count() == 2, b.ref.count()
        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 2
        nodelist = b.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 2

        a.force_expiry()

        assert a.ref.count() == 0, a.ref.count()
        assert client.get(a.resource_id) is None, client.get(a.resource_id)
        assert b.ref.count() == 0, b.ref.count()
        assert client.get(b.resource_id) is None, client.get(b.resource_id)

        merged_doc = {u'a': 5.0, u'b': 7.0, u'c': 9.0}
        executed_doc = {u'a': "5", u'b': "7", u'c': "9"}

        for k, v in a.doc.items():
            assert merged_doc[k] == v
        for k, v in executed_doc.items():
            assert client.get("{0}.write.{1}".format(a.resource_id, k)) == v

        p.stop()
        p2.stop()

    def test_force_expiry_multiple_processes_caching(self):
        p = Process()
        p2 = Process()
        p3 = Process()
        a = ConflictFreeUserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        b = ConflictFreeUserUpdate(process=p2, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.})
        c = ConflictFreeUserUpdate(process=p3, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})

        client = a.process().client

        assert a.ref.count() == 3, a.ref.count()
        assert b.ref.count() == 3, b.ref.count()
        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 3
        nodelist = b.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 3

        b._cache()
        b.end_session()
        a.force_expiry()

        assert a.ref.count() == 0, a.ref.count()
        assert client.get(a.resource_id) is None, client.get(a.resource_id)
        assert b.ref.count() == 0, b.ref.count()
        assert client.get(b.resource_id) is None, client.get(b.resource_id)

        merged_doc = {u'a': 5.0, u'b': 7.0, u'c': 9.0}
        executed_doc = {u'a': "5", u'b': "7", u'c': "9"}

        for k, v in a.doc.items():
            assert merged_doc[k] == v
        for k, v in executed_doc.items():
            assert client.get("{0}.write.{1}".format(a.resource_id, k)) == v

        c._cache()
        c.end_session()

        merged_doc = {u'a': 7.0, u'b': 8.0, u'c': 9.0}
        executed_doc = {u'a': "12", u'b': "15", u'c': "18"}

        for k, v in c.doc.items():
            assert merged_doc[k] == v
        for k, v in executed_doc.items():
            assert client.get("{0}.write.{1}".format(c.resource_id, k)) == v

        p.stop()
        p2.stop()
        p3.stop()

    def test_force_expiry_same_process_caching(self):
        p = Process()
        p.client.flushall()

        a = ConflictFreeUserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        b = ConflictFreeUserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.})
        c = ConflictFreeUserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})

        client = a.process().client

        assert a.ref.count() == 1, a.ref.count()
        assert b.ref.count() == 1, b.ref.count()
        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 1
        nodelist = b.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 1

        b._cache()
        a.force_expiry()
        assert a.ref.count() == 0, a.ref.count()
        assert client.get(a.resource_id) is None, client.get(a.resource_id)
        assert b.ref.count() == 0, b.ref.count()
        assert client.get(b.resource_id) is None, client.get(b.resource_id)

        merged_doc = {u'a': 5.0, u'b': 7.0, u'c': 9.0}
        executed_doc = {u'a': "5", u'b': "7", u'c': "9"}

        for k, v in a.doc.items():
            assert merged_doc[k] == v
        for k, v in executed_doc.items():
            assert client.get("{0}.write.{1}".format(a.resource_id, k)) == v

        c._cache()
        c.end_session()

        merged_doc = {u'a': 7.0, u'b': 8.0, u'c': 9.0}
        executed_doc = {u'a': "12", u'b': "15", u'c': "18"}

        for k, v in c.doc.items():
            assert merged_doc[k] == v
        for k, v in executed_doc.items():
            assert client.get("{0}.write.{1}".format(c.resource_id, k)) == v

        p.stop()

    def test_force_expiry_caching_conflicts(self):
        p = Process()
        p.client.flushall()
        a = ConflictFreeUserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        c = ConflictFreeUserUpdate(process=p, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})

        p2 = Process()
        b = ConflictFreeUserUpdate(process=p2, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.})
        d = ConflictFreeUserUpdate(process=p2, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 20., 'b': 21., 'c': 22.})

        client = a.process().client

        assert a.ref.count() == 2, a.ref.count()
        assert b.ref.count() == 2, b.ref.count()
        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 2
        nodelist = b.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 2

        b._cache()
        a.force_expiry()

        assert a.ref.count() == 0, a.ref.count()
        assert b.ref.count() == 0, b.ref.count()

        merged_doc = {u'a': 5.0, u'b': 7.0, u'c': 9.0}
        executed_doc = {u'a': "5", u'b': "7", u'c': "9"}

        for k, v in a.doc.items():
            assert merged_doc[k] == v
        for k, v in executed_doc.items():
            assert client.get("{0}.write.{1}".format(a.resource_id, k)) == v


        e = ConflictFreeUserUpdate(process=p2, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 30., 'b': 31., 'c': 32.})

        c._cache()
        d.end_session()

        merged_doc = {u'a': 27.0, u'b': 29.0, u'c': 31.0}
        executed_doc = {u'a': "32", u'b': "36", u'c': "40"}

        for k, v in d.doc.items():
            assert merged_doc[k] == v
        for k, v in executed_doc.items():
            assert client.get("{0}.write.{1}".format(d.resource_id, k)) == v

        e.end_session()

        merged_doc = {u'a': 30.0, u'b': 31.0, u'c': 32.0}
        executed_doc = {u'a': "62", u'b': "67", u'c': "72"}

        for k, v in e.doc.items():
            assert merged_doc[k] == v
        for k, v in executed_doc.items():
            assert client.get("{0}.write.{1}".format(d.resource_id, k)) == v

        p.stop()
        p2.stop()

    def test_cache_does_not_lock(self):
        p1 = Process()
        client = p1.client

        a = ConflictFreeUserUpdate(process=p1, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})        

        with a.ref.lock():
            a._cache()

        cached = {u'a': "1", u'b': "2", u'c': "3"}
        for k, v in a._ConflictFreeUpdate__get_cached_doc().items():
            assert cached[k] == v

        p1.stop()

    def test_multiple_processes_cache_concurrently_without_lock(self):
        p1 = Process()
        client = p1.client
        client.flushdb()
        p2 = Process()

        a = ConflictFreeUserUpdate(process=p1, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        b = ConflictFreeUserUpdate(process=p1, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})
        c = ConflictFreeUserUpdate(process=p2, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})


        t = threading.Thread(target=a._cache)
        t2 = threading.Thread(target=b._cache)
        t3 = threading.Thread(target=c._cache)

        t.start()
        t2.start()
        t3.start()
        t.join()
        t2.join()
        t3.join()

        cached = {u'a': "15", u'b': "18", u'c': "21"}
        for k, v in b._ConflictFreeUpdate__get_cached_doc().items():
            assert cached[k] == v

        p1.stop()
        p2.stop()

    def test_execute_does_not_lock(self):
        p1 = Process()
        client = p1.client

        a = ConflictFreeUserUpdate(process=p1, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})        

        with a.ref.lock():
            a._execute()

        executed_doc = {u'a': "1", u'b': "2", u'c': "3"}
        for k, v in executed_doc.items():
            assert client.get("{0}.write.{1}".format(a.resource_id, k)) == v

        p1.stop()


    def test_multiple_processes_execute_concurrently_without_lock(self):
        p1 = Process()
        client = p1.client
        p2 = Process()

        a = ConflictFreeUserUpdate(process=p1, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        b = ConflictFreeUserUpdate(process=p1, _id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})

        with a.ref.lock():
            a._cache()

        t = threading.Thread(target=a.end_session)
        t2 = threading.Thread(target=b.end_session)

        t.start()
        t2.start()
        t.join()
        t2.join()

        executed_doc = {u'a': "8", u'b': "10", u'c': "12"}
        for k, v in executed_doc.items():
            assert client.get("{0}.write.{1}".format(b.resource_id, k)) == v

        p1.stop()
        p2.stop()
