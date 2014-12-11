from collections import OrderedDict
from disref import DisRefError 

class CacheError(DisRefError):
    pass

class LruCache(object):

    def __init__(self, max_entries=10000):
        self.max_entries = max_entries
        self.__cache = OrderedDict()
        self.__size = 0
        self.__failed = None

    def size(self):
        return self.__size

    def get_last_failed(self):
        return self.__failed

    def set(self, key, val):
        if key in self.__cache:
            update = self.__cache[key]
            del self.__cache[key]
            update.merge(val)
            self.__cache[key] = update
            return 0

        if self.__size + 1 > self.max_entries:
            self.expire_oldest()

        self.__cache[key] = val
        self.__size += 1
        return 1

    def get(self, key):
        return self.__cache[key] 

    def expire_oldest(self):
        key, expired = self.__cache.popitem(last=False)
        self.__size -= 1
        try:
            expired.end_session()
        except Exception, e:
            self.__failed = expired
            raise e 

    def expire(self, key):
        expired = self.__cache[key]
        del self.__cache[key]
        try:
            expired.end_session()
        except Exception, e:
            self.__failed = expired
            raise e 
        self.__size -= 1 

    def expire_all(self):
        while self.__size > 0:
            self.expire_oldest()

