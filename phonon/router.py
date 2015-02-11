import zlib

class Router(object):

    def __init__(self, shards):
        self.__shards = shards

    def route(self, key):
        """
        :param str key: Given a key; this function returns the appropriate shard to route the associated request to.
        :rtype: :py:class:`phonon.shard.Shard`
        :returns: The shard to route this request to.
        """
        return self.__shards[zlib.crc32(key) % len(self.__shards)]

