import unittest
import pickle
import json
import time
import uuid
import threading

import phonon.connections
from phonon.update import Update, ConflictFreeUpdate
from phonon import get_logger


logger = get_logger(__name__)


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
        client = self.conn.client
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
        client = self.conn.client
        key, value = "{}.write".format(self.resource_id), json.dumps(obj)
        rc = client.set(key, value)

    def state(self):
        return {"my_field": self.my_field}

    def clear(self):
        return {"my_field": {}}


class ConflictFreeUserUpdate(ConflictFreeUpdate):

    def execute(self):
        self.called = True
        client = self.conn.client
        for key, val in self.doc.items():
            redis_key = "{0}.write.{1}".format(self.resource_id, key)
            client.incr(redis_key, int(val))


class BaseUpdateTest(unittest.TestCase):

    def setUp(self):
        phonon.connections.connect(hosts=['localhost'])
        phonon.connections.connection.client.flushall()

    def test_process(self):
        conn = phonon.connections.connection
        a = UserUpdate(_id='123', database='test', collection='user',
                       spec={'_id': 123}, doc={'a': 1., 'b': 2., 'c': 3.})
        self.assertIs(conn, a.conn)
        self.assertIs(conn.client, a.conn.client)

    def test_initializer_updates_ref_count(self):
        a = UserUpdate(_id='123', database='test', collection='user',
                       spec={'_id': 123}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=False)

        nodelist = a.ref.nodelist.get_all_nodes()
        assert len(nodelist) == 1
        assert a.conn.id in nodelist

    def test_session_refreshes(self):
        a = UserUpdate(_id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, soft_session=5)
        b = UserUpdate(_id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, soft_session=5)

        old_soft_expiration = a.soft_expiration
        a.refresh(b)

        assert a.soft_expiration >= old_soft_expiration

    def test_end_session_raises_when_deadlocked(self):
        pass

    def test_end_session_executes_for_unique_references(self):
        pass


class UpdateTest(unittest.TestCase):

    def setUp(self):
        phonon.connections.connect(hosts=['localhost'])
        phonon.connections.connection.client.flushall()

    def test_cache_caches(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = UserUpdate(_id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=False)
        a.cache()
        client = a.conn.client
        cached = pickle.loads(client.get(a.resource_id))
        state = cached.__getstate__()
        del state['resource_id']
        del state['hard_expiration']
        assert state == {u'doc': {u'a': 1.0, u'c': 3.0, u'b': 2.0},
                         u'spec': {u'_id': 12345},
                         u'collection': u'user',
                         u'database': u'test'}

        client.flushall()
        b = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 6.}, init_cache=False)
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        c = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 6.}, init_cache=False)

        client = a.conn.client
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

    def test_data_is_recovered(self):
        conn = phonon.connections.connection
        client = phonon.connections.connection.client
        client.flushall()

        a = UserUpdate(_id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=True)

        conn.heart.stop()

        assert len(conn.get_registry()) == 1, len(conn.get_registry())

        cached = pickle.loads(client.get(a.resource_id) or "{}")
        state = cached.__getstate__()
        del state['resource_id']
        del state['hard_expiration']
        assert state == {u'doc': {u'a': 1.0, u'c': 3.0, u'b': 2.0},
                         u'spec': {u'_id': 12345},
                         u'collection': u'user',
                         u'database': u'test'}

        client.hset(conn.HEARTBEAT_KEY, conn.id, int(time.time()) - 6 * conn.HEARTBEAT_INTERVAL)

        conn.id = unicode(uuid.uuid4())
        conn.registry_key = conn.get_registry_key(conn.id)

        assert len(conn.get_registry()) == 0

        conn.send_heartbeat()
        conn.heart.stop()

        assert len(conn.get_registry()) == 1

        a = UserUpdate(_id='12345', database='test', collection='user',
                       spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=True)

        cached = pickle.loads(client.get(a.resource_id) or "{}")

        state = cached.__getstate__()
        del state['resource_id']
        assert state['doc'] == {u'a': 2.0, u'c': 6.0, u'b': 4.0}
        assert state['spec'] == {u'_id': 12345}
        assert state['collection'] == 'user'
        assert state['database'] == 'test'

    def test_set_force_expiry(self):
        a = UserUpdate(_id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=True)

        assert a.ref.force_expiry is False
        a.set_force_expiry()
        assert a.ref.force_expiry is True
        a.set_force_expiry(False)
        assert a.ref.force_expiry is False
        a.set_force_expiry(True)
        assert a.ref.force_expiry is True

    def test_force_expiry_init_cache(self):
        client = phonon.connections.connection.client
        a = UserUpdate(_id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=True)
        b = UserUpdate(_id='123456', database='test', collection='user',
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

    def test_force_expiry_two_processes(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = UserUpdate(_id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        b = UserUpdate(_id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.}, init_cache=True)

        client = a.conn.client

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

    def test_force_expiry_multiple_processes_caching(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = UserUpdate(_id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        b = UserUpdate(_id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.})
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        c = UserUpdate(_id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})

        client = a.conn.client

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

    def test_force_expiry_same_process_caching(self):
        a = UserUpdate(_id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        b = UserUpdate(_id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.})
        c = UserUpdate(_id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})

        client = a.conn.client

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

    def test_force_expiry_caching_conflicts(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = UserUpdate(_id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        c = UserUpdate(_id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})

        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        b = UserUpdate(_id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.})
        d = UserUpdate(_id='123456', database='test', collection='user',
                       spec={'_id': 123456}, doc={'a': 20., 'b': 21., 'c': 22.})

        client = a.conn.client

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

        e = UserUpdate(_id='123456', database='test', collection='user',
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


class ConflictFreeUpdateTest(unittest.TestCase):

    def setUp(self):
        phonon.connections.connect(hosts=['localhost'])
        phonon.connections.connection.client.flushall()

    def test_cache_caches(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = ConflictFreeUserUpdate(_id='12345', database='test', collection='user',
                                   spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=False)
        client = a.conn.client
        client.flushall()
        a.cache()
        data = a._ConflictFreeUpdate__get_cached_doc()
        assert data == {u'a': '1', u'c': '3', u'b': '2'}, data

        client.flushall()
        b = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 6.}, init_cache=False)

        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        c = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
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

    def test_data_is_recovered(self):
        client = phonon.connections.connection.client
        client.flushall()

        a = ConflictFreeUserUpdate(_id='12345', database='test', collection='user',
                                   spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=True)

        phonon.connections.connection.heart.stop()

        assert len(phonon.connections.connection.get_registry()) == 1

        cached = a._ConflictFreeUpdate__get_cached_doc()
        assert cached == {u'a': '1', u'c': '3', u'b': '2'}

        client.hset(phonon.connections.connection.HEARTBEAT_KEY,
                    phonon.connections.connection.id,
                    int(time.time()) - 6 * phonon.connections.connection.HEARTBEAT_INTERVAL)

        conn = phonon.connections.connection
        conn.id = unicode(uuid.uuid4())
        conn.registry_key = conn.get_registry_key(conn.id)

        assert len(conn.get_registry()) == 0

        conn.send_heartbeat()
        conn.heart.stop()

        cached = a._ConflictFreeUpdate__get_cached_doc()
        assert cached == {u'a': '1', u'c': '3', u'b': '2'}

        assert len(conn.get_registry()) == 1, len(conn.get_registry())

        a = ConflictFreeUserUpdate(_id='12345', database='test', collection='user',
                                   spec={'_id': 12345}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=True)
        cached = a._ConflictFreeUpdate__get_cached_doc()
        assert cached == {u'a': '2', u'c': '6', u'b': '4'}, cached

    def test_force_expiry_init_cache(self):
        client = phonon.connections.connection.client
        a = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.}, init_cache=True)
        b = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
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

    def test_force_expiry_two_processes(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        b = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.}, init_cache=True)

        client = a.conn.client

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

    def test_force_expiry_multiple_processes_caching(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        b = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.})
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        c = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})

        client = a.conn.client

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

    def test_force_expiry_same_process_caching(self):
        phonon.connections.connection.client.flushall()

        a = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        b = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.})
        c = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})

        client = a.conn.client

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

    def test_force_expiry_caching_conflicts(self):
        phonon.connections.connection.client.flushall()
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])

        a = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        c = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})

        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        b = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 4., 'b': 5., 'c': 6.})
        d = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 20., 'b': 21., 'c': 22.})

        client = a.conn.client

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

        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        e = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 30., 'b': 31., 'c': 32.})

        c._cache()
        d.force_expiry()

        merged_doc = {u'a': 27.0, u'b': 29.0, u'c': 31.0}
        executed_doc = {u'a': "32", u'b': "36", u'c': "40"}

        for k, v in d.doc.items():
            assert merged_doc[k] == v
        for k, expected in executed_doc.items():
            observed = client.get("{0}.write.{1}".format(d.resource_id, k))
            assert observed == expected, "Got {} and expected {}".format(observed, expected)

        e.end_session()

        merged_doc = {u'a': 30.0, u'b': 31.0, u'c': 32.0}
        executed_doc = {u'a': "62", u'b': "67", u'c': "72"}

        for k, v in e.doc.items():
            assert merged_doc[k] == v
        for k, v in executed_doc.items():
            assert client.get("{0}.write.{1}".format(d.resource_id, k)) == v

    def test_cache_does_not_lock(self):
        client = phonon.connections.connection.client

        a = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})

        with a.ref.lock():
            a._cache()

        cached = {u'a': "1", u'b': "2", u'c': "3"}
        for k, v in a._ConflictFreeUpdate__get_cached_doc().items():
            assert cached[k] == v

    def test_multiple_processes_cache_concurrently_without_lock(self):
        client = phonon.connections.connection.client
        client.flushdb()

        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        b = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 7., 'b': 8., 'c': 9.})
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        c = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
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

    def test_execute_does_not_lock(self):
        client = phonon.connections.connection.client

        a = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})

        with a.ref.lock():
            a._execute()

        executed_doc = {u'a': "1", u'b': "2", u'c': "3"}
        for k, v in executed_doc.items():
            assert client.get("{0}.write.{1}".format(a.resource_id, k)) == v

    def test_multiple_processes_execute_concurrently_without_lock(self):
        client = phonon.connections.connection.client

        a = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
                                   spec={'_id': 123456}, doc={'a': 1., 'b': 2., 'c': 3.})
        b = ConflictFreeUserUpdate(_id='123456', database='test', collection='user',
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
