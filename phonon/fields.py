import itertools

import phonon.connections


class Field(object):

    def key(self, *args):
        return ".".join([str(a) for a in args])


class ID(Field):

    def __init__(self):
        pass

    def merge(self, a, b):
        assert a == b
        return a

    def cache(self, *args):
        return True


class Sum(Field):

    def __init__(self, dtype=int):
        self.conn = phonon.connections.connection
        self.dtype = dtype
        if dtype is int:
            self.operation = self.conn.client.incrby
            self.default_value = 0
        elif dtype is float:
            self.operation = self.conn.client.incrbyfloat
            self.default_value = 0.

    def merge(self, a, b):
        return a + b

    def cache(self, model_name, instance_id, field_name, field_value):
        key = self.key(model_name, instance_id, field_name)
        return self.operation(key, field_value) is not None


class Diff(Field):

    def __init__(self, dtype=int):
        self.conn = phonon.connections.connection
        self.dtype = dtype
        if dtype is int:
            self.operation = self.conn.client.incrby
            self.default_value = 0
        elif dtype is float:
            self.operation = self.conn.client.incrbyfloat
            self.default_value = 0.

    def merge(self, a, b):
        return a - b

    def cache(self, model_name, instance_id, field_name, field_value):
        key = self.key(model_name, instance_id, field_name)
        return self.operation(key, -field_value) is not None


class ListAppend(Field):

    def __init__(self):
        self.conn = phonon.connections.connection
        self.default_value = []

    def cache(self, model_name, instance_id, field_name, field_value):
        key = self.key(model_name, instance_id, field_name)
        return self.conn.client.rpush(key, *field_value) > 0

    def merge(self, a, b):
        return a + b


class SetAppend(Field):

    def __init__(self):
        self.conn = phonon.connections.connection
        self.default_value = set()

    def cache(self, model_name, instance_id, field_name, field_value):
        key = self.key(model_name, instance_id, field_name)
        return self.conn.client.sadd(key, *field_value) > 0

    def merge(self, a, b):
        return a.union(b)


class WindowedList(Field):

    def __init__(self, window_length=None):
        self.conn = phonon.connections.connection
        self.default_value = []
        self.window_length = window_length or 10

    def cache(self, model_name, instance_id, field_name, field_value):
        key = self.key(model_name, instance_id, field_name)
        return all([self.conn.client.zadd(key, *[v for v in itertools.chain(*field_value)]) is not None,
                    self.conn.client.zremrangebyrank(key, 0, -self.window_length) is not None])

    def merge(self, a, b):
        return a + b
