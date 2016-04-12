import unittest

import phonon.exceptions
import phonon.connections
import phonon.model
import phonon.fields


class ModelTest(unittest.TestCase):

    def setUp(self):
        if phonon.connections.connection:
            phonon.connections.connection.client.flushall()
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        self.conn = phonon.connections.connection

        class Foo(phonon.model.Model):
            id = phonon.fields.ID()
            a = phonon.fields.Sum()

        self.Foo = Foo

    def test_init(self):
        foo = self.Foo(id=1, a=5)

        assert foo.id == 1
        assert foo.a == 5

        with self.assertRaisesRegexp(phonon.exceptions.ArgumentError, "id is a required field"):
            c = self.Foo(a=6)

    def test_merge(self):
        a = self.Foo(id=1, a=5)
        b = self.Foo(id=1, a=3)
        a.merge(b)

        assert a.id == 1
        assert a.a == 8

    def test_cache(self):
        a = self.Foo(id=1, a=5)
        a.cache()

        assert self.conn.client.get('Foo.1.a') == '5'
