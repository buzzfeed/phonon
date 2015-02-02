from collections import OrderedDict
from phonon import DisRefError

class CacheError(DisRefError):
    pass

class LruCache(object):

    def __init__(self, max_entries=10000):
        """
        Initializes the LRU (least recently used) cache. This cache handles
        shuffling elements around based on the last time they were accessed.
        What this does differently from other LRU cache implementations is it
        expects cache elements to implement an `end_session` method, which is
        called every time an element is expired. If you're not using that
        functionality you can use any-old LRU cache.

        :param int max_entries: The maximum number of entries to store at a
            given time.
        """
        self.max_entries = max_entries
        self.__cache = OrderedDict()
        self.__size = 0
        self.__failed = None

    def size(self):
        """
        :returns: The current number of elements in the cache.
        """
        return self.__size

    def get_last_failed(self):
        """
        If an exception was raised when the `end_session` method was called on
        one of your cache elements you'll find that element here.

        :returns: The last cache element to fail while trying to end it's
            session.
        """
        return self.__failed

    def set(self, key, val):
        """
        Adds an element to the cache. If the element is already in the cache
        the `merge` method will be called on the existing element to merge the
        new element with the existing one. The element will then be marked as
        recently updated. If this would result in more than `max_entries`
        existing in the cache; the oldes element in the cache will be expired.
        New elements will just be added to the cache, and the size of the cache
        will be incremented.

        :param mixed key: The key for the element. Best to use a str, unicode,
            or int type.
        :param phonon.update.Update val: The object to store at that location
            in the cache. The easiest thing to do is implement your object
            updates as a sub-class of the `phonon.update.Update` class.

        :returns: False if the size wasn't incremented, True otherwise.
        """
        if key in self.__cache:
            update = self.__cache[key]
            del self.__cache[key]
            update.merge(val)
            self.__cache[key] = update
            return False 

        if self.__size + 1 > self.max_entries:
            self.expire_oldest()

        self.__cache[key] = val
        self.__size += 1
        return True 

    def get(self, key):
        """
        Accesses an element on the cache. Time complexity is O(1). The element
        will be marked as recently updated.

        :param mixed key: The key for the element in the cache. Best to use
            str, unicode, or int

        :returns: The element in the cache at `key`.
        :raises: KeyError
        :rtype: phonon.update.Update
        """
        el = self.__cache[key]
        del self.__cache[key]
        self.__cache[key] = el
        return el 

    def expire_oldest(self):
        """
        Expires the last element in the cache, reducing the cache size
        appropriately. Ends the session for that element.
        """
        key, expired = self.__cache.popitem(last=False)
        self.__size -= 1
        try:
            expired.end_session()
        except Exception, e:
            self.__failed = expired
            raise e 

    def expire(self, key):
        """
        Expires an element at a particular key.

        :param mixed key: The key for the element in the cache to expire. Use
        str, unicode, int, etc.
        """
        expired = self.__cache[key]
        del self.__cache[key]
        try:
            expired.end_session()
        except Exception, e:
            self.__failed = expired
            raise e 
        self.__size -= 1 

    def expire_all(self):
        """
        Expires everything in the cache.

        """
        while self.__size > 0:
            self.expire_oldest()

