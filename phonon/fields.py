import operator

import phonon.connections


class Field(object):
    def cache(self):
        key = "{}.{}".format(self.__class__.__name__, self.id)
        self.operation(key, self.value)


class SumField(Field):
    def __init__(self, dtype=int):
        self.conn = phonon.connections.connection
        self.dtype = dtype
        if dtype is int:
            self.operation = self.conn.client.incrby
        elif dtype is float:
            self.operation = self.conn.client.incrbyfloat

    def merge(self, a, b):
        return a + b



class DiffField(Field):
    def __init__(self, dtype=int):
        self.conn = phonon.connections.connection
        self.dtype = dtype
        if dtype is int:
            self.operation = self.conn.client.decrby
        elif dtype is float:
            self.operation = self.conn.client.decrbyfloat

    def merge(self, a, b):
        return a - b



class ListAppendField(Field):
    def __init__(self):
        self.conn = phonon.connections.connection
        self.value = []
        self.operation = self.conn.client.rpush

    def merge(self, a, b):
        return a + b

class SetAppendField(Field):
    def __init__(self):
        self.conn = phonon.connections.connection
        self.value = set()
        self.operation = self.conn.client.sadd

    def merge(self, a, b):
        return a.union(b)
