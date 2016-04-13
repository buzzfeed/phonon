import unittest

import phonon.connections
import phonon.fields


class FieldTest(unittest.TestCase):

    def setUp(self):
        if phonon.connections.connection:
            phonon.connections.connection.client.flushall()
        phonon.connections.connection = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        self.conn = phonon.connections.connection

    def test_sum_init(self):
        int_sum_field = phonon.fields.Sum()

        assert int_sum_field.data_type is int
        assert int_sum_field.operation.__name__ == self.conn.client.incrby.__name__
        assert int_sum_field.default_value == 0

        float_sum_field = phonon.fields.Sum(data_type=float)
        assert float_sum_field.data_type is float
        assert float_sum_field.operation.__name__ == self.conn.client.incrbyfloat.__name__
        assert float_sum_field.default_value == 0.

    def test_sum_merge(self):
        b = phonon.fields.Sum()
        a = phonon.fields.Sum()

        assert a.merge(1, 2) == 3

    def test_sum_cache(self):
        a = phonon.fields.Sum()
        assert a.cache('Sum', 1, 'test_sum', 1)
        observed = int(self.conn.client.get('Sum.1.test_sum'))
        expected = 1
        assert observed == expected, observed

        assert a.cache('Sum', 1, 'test_sum', 3)
        assert int(self.conn.client.get('Sum.1.test_sum')) == 4

        b = phonon.fields.Sum(data_type=float)
        assert b.cache('Sum', 2, 'test_sum', 1.5)
        assert float(self.conn.client.get('Sum.2.test_sum')) == 1.5

        assert b.cache('Sum', 2, 'test_sum', 3.2)
        assert float(self.conn.client.get('Sum.2.test_sum')) == 4.7

    def test_diff_init(self):
        a = phonon.fields.Diff()
        assert a.data_type is int
        assert a.operation.__name__ == self.conn.client.incrby.__name__
        assert a.default_value == 0

        b = phonon.fields.Diff(data_type=float)
        assert b.data_type is float
        assert b.operation.__name__ == self.conn.client.incrbyfloat.__name__
        assert b.default_value == 0.

    def test_diff_merge(self):
        a = phonon.fields.Diff()
        b = phonon.fields.Diff()

        assert a.merge(3, 1) == 2

    def test_diff_cache(self):
        a = phonon.fields.Diff()
        assert a.cache('Diff', 1, 'test_diff', 1)
        assert int(self.conn.client.get('Diff.1.test_diff')) == -1

        assert a.cache('Diff', 1, 'test_diff', 2)
        assert int(self.conn.client.get('Diff.1.test_diff')) == -3

        b = phonon.fields.Diff(data_type=float)
        assert b.cache('Diff', 2, 'test_diff', 1.2)
        assert float(self.conn.client.get('Diff.2.test_diff')) == -1.2

        assert b.cache('Diff', 2, 'test_diff', 2.6)
        assert float(self.conn.client.get('Diff.2.test_diff')) == -3.8

    def test_list_init(self):
        a = phonon.fields.ListAppend()
        assert a.default_value == []
        assert a.conn is phonon.connections.connection

    def test_list_cache(self):
        a = phonon.fields.ListAppend()
        assert a.cache('ListAppend', 1, 'test_list', [1, 2, 3, 4, 5])
        observed = self.conn.client.lrange('ListAppend.1.test_list', 0, 5)
        expected = ['1', '2', '3', '4', '5']
        assert observed == expected, observed

    def test_list_merge(self):
        a = phonon.fields.ListAppend()
        assert a.merge([1, 2, 3], [3, 4, 5]) == [1, 2, 3, 3, 4, 5]

    def test_set_init(self):
        a = phonon.fields.SetAppend()
        assert a.default_value == set()
        assert a.conn is phonon.connections.connection

    def test_set_cache(self):
        a = phonon.fields.SetAppend()
        assert a.cache('SetAppend', 1, 'test_set', set([1, 2, 3, 4, 5]))
        assert self.conn.client.smembers('SetAppend.1.test_set') == set(['1', '2', '3', '4', '5'])

    def test_set_merge(self):
        a = phonon.fields.SetAppend()
        assert a.merge(set([1, 2, 3]), set([3, 4, 5])) == set([1, 2, 3, 4, 5])

    def test_windowedlist_init(self):
        a = phonon.fields.WindowedList()
        assert a.conn is phonon.connections.connection
        assert a.default_value == []

    def test_windowedlist_cache(self):
        a = phonon.fields.WindowedList()
        # Cache elements go score, element
        assert a.cache('WindowedList', 1, 'test_window', [(i, i * 10) for i in range(15)])
        expected = [str(i * 10) for i in range(15)][6:]
        observed = self.conn.client.zrangebyscore('WindowedList.1.test_window', 1, 100)
        assert expected == observed, observed

    def test_windowedlist_merge(self):
        a = phonon.fields.WindowedList()
        assert a.merge([(1, 10)], [(2, 20)]) == [(1, 10), (2, 20)]
