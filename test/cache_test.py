import unittest
import json
import time
import logging

import redis

import phonon.connections
from phonon.cache import LruCache

from test.update_test import UserUpdate, UserUpdateCustomField, ConflictFreeUserUpdate

logging.disable(logging.CRITICAL)


class LruCacheTest(unittest.TestCase):

    def setUp(self):
        redis.StrictRedis().flushall()
        phonon.connections.connect(hosts=['localhost'])
        phonon.connections.connection.local_registry = set()
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
                self.soft_expiration = time.time() + 15
                self.hard_expiration = time.time() + 15

            def merge(self, other):
                self.__other = other

            def end_session(self):
                self.__called = True

            def assert_end_session_called(self):
                assert self.__called

            def assert_merged(self, other):
                assert other is self.__other

            def refresh(self, other):
                self.soft_expiration = time.time() + 15
                self.merge(other)

            def is_expired(self):
                return time.time() > self.hard_expiration

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
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, init_cache=True)
        b = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, init_cache=True)

        self.cache.set('456', a)
        self.cache.set('456', b)

        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        c = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 3.}, init_cache=True)
        d = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 4.}, init_cache=True)

        self.cache2 = LruCache(max_entries=5)

        self.cache2.set('456', c)
        self.cache2.set('456', d)

        self.cache.expire_all()
        self.cache2.expire_all()

        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 20.0, "d": 16.0, "f": 10.0}
        assert written['spec'] == {"_id": 456}
        assert written['collection'] == "user"
        assert written['database'] == "test"

    def test_init_cache_merges_properly_conflict_free(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, init_cache=True)
        b = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, init_cache=True)

        self.cache.set('456', a)
        self.cache.set('456', b)

        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        c = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 3.}, init_cache=True)
        d = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 4.}, init_cache=True)

        self.cache2 = LruCache(max_entries=5)

        self.cache2.set('456', c)
        self.cache2.set('456', d)

        self.cache.expire_all()
        self.cache2.expire_all()

        executed_doc = {u'e': "20", u'd': "16", u'f': "10"}
        for k, v in executed_doc.items():
            assert phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k)) == v

    def test_init_cache_merges_properly_async(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, init_cache=True)
        b = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, init_cache=True)

        self.async_cache.set('456', a)
        self.async_cache.set('456', b)

        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        c = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 3.}, init_cache=True)
        d = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 4.}, init_cache=True)

        self.cache2 = LruCache(max_entries=5, async=True)

        self.cache2.set('456', c)
        self.cache2.set('456', d)

        self.async_cache.expire_all()
        self.cache2.expire_all()

        retries = 100
        while retries > 0 and not any([hasattr(u, 'called') for u in [a, c]]):
            retries -= 1
            time.sleep(0.01)
        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 20.0, "d": 16.0, "f": 10.0}
        assert written['spec'] == {"_id": 456}
        assert written['collection'] == "user"
        assert written['database'] == "test"

    def test_init_cache_merges_properly_async_conflict_free(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = ConflictFreeUserUpdate(_id='457', database='test', collection='user',
                                   spec={u'_id': 457}, doc={'d': 4., 'e': 5., 'f': 1.}, init_cache=True)
        b = ConflictFreeUserUpdate(_id='457', database='test', collection='user',
                                   spec={u'_id': 457}, doc={'d': 4., 'e': 5., 'f': 2.}, init_cache=True)

        self.async_cache.set('456', a)
        self.async_cache.set('456', b)

        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        c = ConflictFreeUserUpdate(_id='457', database='test', collection='user',
                                   spec={u'_id': 457}, doc={'d': 4., 'e': 5., 'f': 3.}, init_cache=True)
        d = ConflictFreeUserUpdate(_id='457', database='test', collection='user',
                                   spec={u'_id': 457}, doc={'d': 4., 'e': 5., 'f': 4.}, init_cache=True)

        self.cache2 = LruCache(max_entries=5, async=True)

        self.cache2.set('456', c)
        self.cache2.set('456', d)

        self.async_cache.expire_all()
        self.cache2.expire_all()

        retries = 100
        while not any([hasattr(u, 'called') for u in [a, b, c, d]]) and retries > 0:
            retries -= 1
            time.sleep(0.01)

        executed_doc = {u'e': "20", u'd': "16", u'f': "10"}
        for k, expected in executed_doc.items():
            observed = phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k))
            assert observed == expected, "{} != {}".format(observed, expected)

    def test_init_cache_merges_properly_with_custom_fields(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 1.}, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)
        b = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 2.}, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)

        self.cache.set('456', a)
        self.cache.set('456', b)

        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        c = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 3.}, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)
        d = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 4.}, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)

        self.cache2 = LruCache(max_entries=5)

        self.cache2.set('456', c)
        self.cache2.set('456', d)

        self.cache.expire_all()
        self.cache2.expire_all()

        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))
        assert written['my_field'] == {"e": 20.0, "d": 16.0, "f": 10.0}
        assert written['spec'] == {"_id": 456}
        assert written['collection'] == "user"
        assert written['database'] == "test"

    def test_init_cache_merges_properly_with_custom_fields_async(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 1.}, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)
        b = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 2.}, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)

        self.async_cache.set('456', a)
        self.async_cache.set('456', b)

        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        c = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 3.}, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)
        d = UserUpdateCustomField(my_field={'d': 4., 'e': 5., 'f': 4.}, _id='456',
                                  database='test', collection='user', spec={u'_id': 456}, init_cache=True)

        self.cache2 = LruCache(max_entries=5, async=True)

        self.cache2.set('456', c)
        self.cache2.set('456', d)

        self.async_cache.expire_all()
        self.cache2.expire_all()

        self.async_cache._LruCache__expire_queue.put(phonon.cache.Finish)
        self.cache2._LruCache__expire_queue.put(phonon.cache.Finish)

        self.async_cache._LruCache__expire_thread.join(1)
        self.cache2._LruCache__expire_thread.join(1)

        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))
        assert written['my_field'] == {"e": 20.0, "d": 16.0, "f": 10.0}, written["my_field"]
        assert written['spec'] == {"_id": 456}
        assert written['collection'] == "user"
        assert written['database'] == "test"

    def test_cache_handles_soft_sessions(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, soft_session=.005)
        b = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, soft_session=.005)

        self.cache.set('456', a)
        time.sleep(.04)
        set_return = self.cache.set('456', b)

        assert set_return is False
        written = phonon.connections.connection.client.get('{0}.write'.format(a.resource_id))
        assert written is None

        time.sleep(.04)
        get_return = self.cache.get('456')
        assert get_return is None

        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 10.0, "d": 8.0, "f": 3.0}

    def test_cache_handles_soft_sessions_conflict_free(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, soft_session=.005)
        b = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, soft_session=.005)

        self.cache.set('456', a)
        time.sleep(.04)
        set_return = self.cache.set('456', b)

        assert set_return is False
        executed_doc = {u'e': "10", u'd': "8", u'f': "3"}
        for k, v in executed_doc.items():
            assert phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k)) is None

        time.sleep(.04)
        get_return = self.cache.get('456')
        assert get_return is None

        executed_doc = {u'e': "10", u'd': "8", u'f': "3"}
        for k, v in executed_doc.items():
            assert phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k)) == v

    def test_cache_handles_soft_sessions_async(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, soft_session=.01)
        b = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, soft_session=.01)

        self.async_cache.set('456', a)
        set_return = self.async_cache.set('456', b)

        time.sleep(0.03)

        assert set_return is False
        written = phonon.connections.connection.client.get('{0}.write'.format(a.resource_id))
        assert written is None, written

        get_return = self.async_cache.get('456')
        assert get_return is None, get_return

        retries = 100
        while not any([hasattr(u, 'called') for u in [a, b]]) and retries > 0:
            retries -= 1
            time.sleep(0.01)

        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 10.0, "d": 8.0, "f": 3.0}

    def test_cache_handles_soft_sessions_async_conflict_free(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, soft_session=.01)
        b = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, soft_session=.01)

        self.async_cache.set('456', a)
        set_return = self.async_cache.set('456', b)

        time.sleep(0.02)

        assert set_return is False
        executed_doc = {u'e': "10", u'd': "8", u'f': "3"}
        for k, v in executed_doc.items():
            assert phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k)) is None

        get_return = self.async_cache.get('456')
        assert get_return is None, get_return

        retries = 100
        while not any([hasattr(u, 'called') for u in [a, b]]) and retries > 0:
            retries -= 1
            time.sleep(0.01)

        executed_doc = {u'e': "10", u'd': "8", u'f': "3"}
        for k, v in executed_doc.items():
            assert phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k)) == v

    def test_cache_ends_expired_sessions(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        c = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)

        self.cache.set('456', a)
        time.sleep(.04)
        set_return = self.cache.set('456', b)

        assert set_return is None

        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 10.0, "d": 8.0, "f": 3.0}

        self.cache.set('456', c)
        time.sleep(.04)

        get_return = self.cache.get('456')

        assert get_return is None

        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))

        assert written['doc'] == {"e": 5.0, "d": 4.0, "f": 1.0}

    def test_cache_ends_expired_sessions_conflict_free(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        c = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)

        self.cache.set('456', a)
        time.sleep(.04)
        set_return = self.cache.set('456', b)

        assert set_return is None

        executed_doc = {u'e': "10", u'd': "8", u'f': "3"}
        for k, v in executed_doc.items():
            assert phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k)) == v

        self.cache.set('456', c)
        time.sleep(.04)

        get_return = self.cache.get('456')

        assert get_return is None

        executed_doc = {u'e': "15", u'd': "12", u'f': "4"}
        for k, v in executed_doc.items():
            assert phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k)) == v

    def test_cache_ends_expired_sessions_async(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        c = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)

        self.async_cache.set('456', a)
        time.sleep(.04)
        set_return = self.async_cache.set('456', b)

        assert set_return is None

        retries = 100
        while not hasattr(a, 'called') and retries > 0:
            retries -= 1
            time.sleep(0.01)

        val = phonon.connections.connection.client.get('{0}.write'.format(a.resource_id))
        assert val is not None
        written = json.loads(val)
        assert written['doc'] == {"e": 10.0, "d": 8.0, "f": 3.0}

        self.async_cache.set('456', c)
        time.sleep(.04)

        get_return = self.async_cache.get('456')

        assert get_return is None

        retries = 100
        while not hasattr(c, 'called') and retries > 0:
            time.sleep(0.01)
            retries -= 1

        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))

        assert written['doc'] == {"e": 5.0, "d": 4.0, "f": 1.0}

    def test_cache_ends_expired_sessions_async_conflict_free(self):
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        a = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        c = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)

        self.async_cache.set('456', a)
        time.sleep(.04)
        set_return = self.async_cache.set('456', b)

        assert set_return is None

        retries = 100
        while not hasattr(a, 'called') and retries > 0:
            retries -= 1
            time.sleep(0.01)

        executed_doc = {u'e': "10", u'd': "8", u'f': "3"}
        for k, v in executed_doc.items():
            assert phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k)) == v

        self.async_cache.set('456', c)
        time.sleep(.04)

        get_return = self.async_cache.get('456')

        assert get_return is None

        retries = 100
        while not hasattr(c, 'called') and retries > 0:
            time.sleep(0.01)
            retries -= 1

        executed_doc = {u'e': "15", u'd': "12", u'f': "4"}
        for k, v in executed_doc.items():
            assert phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k)) == v

        phonon.connections.connection.client.flushdb()
        phonon.connections.connection.close()

    def test_cache_ends_multiprocess_expired_sessions(self):
        conn1 = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        conn2 = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        self.cache2 = LruCache(max_entries=5)

        a = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        c = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 1., 'e': 2., 'f': 3.}, hard_session=.005)

        self.cache.set('456', a)
        time.sleep(.04)
        set_return = self.cache.set('456', b)
        set_return_2 = self.cache2.set('456', c)

        assert set_return is None
        assert set_return_2 is True

        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 10.0, "d": 8.0, "f": 3.0}

        get_return_2 = self.cache2.get('456')
        assert get_return_2 is None

        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 2.0, "d": 1.0, "f": 3.0}

    def test_cache_ends_multiprocess_expired_sessions_conflict_free(self):
        ab_conn = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        c_conn = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        phonon.connections.connection = ab_conn
        self.cache2 = LruCache(max_entries=5)

        a = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        phonon.connections.connection = c_conn
        c = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 1., 'e': 2., 'f': 3.}, hard_session=.005)

        phonon.connections.connection = ab_conn
        self.cache.set('456', a)
        time.sleep(.04)
        set_return = self.cache.set('456', b)

        c_conn = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        phonon.connections.connection = c_conn
        set_return_2 = self.cache2.set('456', c)

        assert set_return is None
        assert set_return_2 is True

        executed_doc = {u'e': "10", u'd': "8", u'f': "3"}
        for k, v in executed_doc.items():
            assert phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k)) == v

        get_return_2 = self.cache2.get('456')
        assert get_return_2 is None

        executed_doc = {u'e': "12", u'd': "9", u'f': "6"}
        for k, expected in executed_doc.items():
            observed = phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k))
            assert observed == expected, "{} should have been {}".format(observed, expected)

    def test_cache_ends_multiprocess_expired_sessions_async(self):
        conn1 = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        conn2 = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        self.cache2 = LruCache(max_entries=5, async=True)

        a = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        c = UserUpdate(_id='456', database='test', collection='user',
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

        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 10.0, "d": 8.0, "f": 3.0}

        get_return_2 = self.cache2.get('456')
        assert get_return_2 is None

        retries = 100
        while not hasattr(c, 'called') and retries > 0:
            retries -= 1
            time.sleep(0.01)

        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 2.0, "d": 1.0, "f": 3.0}

        phonon.connections.connection.client.flushdb()
        phonon.connections.connection.close()

    def test_cache_ends_multiprocess_expired_sessions_async_conflict_free(self):
        conn1 = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        conn2 = phonon.connections.AsyncConn(redis_hosts=['localhost'])

        self.cache2 = LruCache(max_entries=5, async=True)

        a = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        c = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
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

        executed_doc = {u'e': "10", u'd': "8", u'f': "3"}
        for k, v in executed_doc.items():
            assert phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k)) == v

        get_return_2 = self.cache2.get('456')
        assert get_return_2 is None

        retries = 100
        while not hasattr(c, 'called') and retries > 0:
            retries -= 1
            time.sleep(0.01)

        executed_doc = {u'e': "12", u'd': "9", u'f': "6"}
        for k, v in executed_doc.items():
            assert phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k)) == v

        phonon.connections.connection.client.flushdb()
        phonon.connections.connection.close()

    def test_cache_ends_multiprocess_expired_sessions_async2(self):
        self.cache2 = LruCache(max_entries=5, async=True)

        a = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        c = UserUpdate(_id='456', database='test', collection='user',
                       spec={u'_id': 456}, doc={'d': 1., 'e': 2., 'f': 3.}, hard_session=.005)

        self.async_cache.set('456', a)
        time.sleep(.1)
        set_return = self.async_cache.set('456', b)
        set_return_2 = self.cache2.set('456', c)

        assert set_return is None
        assert set_return_2 is True

        retries = 100
        while not hasattr(a, 'called') and retries > 0:
            retries -= 1
            time.sleep(0.01)

        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 10.0, "d": 8.0, "f": 3.0}

        get_return_2 = self.cache2.get('456')
        assert get_return_2 is None

        retries = 100
        while not hasattr(c, 'called') and retries > 0:
            retries -= 1
            time.sleep(0.01)

        written = json.loads(phonon.connections.connection.client.get('{0}.write'.format(a.resource_id)))
        assert written['doc'] == {"e": 2.0, "d": 1.0, "f": 3.0}

        phonon.connections.connection.client.flushdb()
        phonon.connections.connection.close()

    def test_cache_ends_multiprocess_expired_sessions_async2_conflict_free(self):
        conn1 = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        conn2 = phonon.connections.AsyncConn(redis_hosts=['localhost'])

        self.cache2 = LruCache(max_entries=5, async=True)

        a = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 1.}, hard_session=.005)
        b = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
                                   spec={u'_id': 456}, doc={'d': 4., 'e': 5., 'f': 2.}, hard_session=.005)
        c = ConflictFreeUserUpdate(_id='456', database='test', collection='user',
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

        executed_doc = {u'e': "10", u'd': "8", u'f': "3"}
        for k, v in executed_doc.items():
            assert phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k)) == v

        get_return_2 = self.cache2.get('456')
        assert get_return_2 is None

        retries = 100
        while not hasattr(c, 'called') and retries > 0:
            retries -= 1
            time.sleep(0.01)

        executed_doc = {u'e': "12", u'd': "9", u'f': "6"}
        for k, v in executed_doc.items():
            assert phonon.connections.connection.client.get("{0}.write.{1}".format(a.resource_id, k)) == v
