import zlib

class Router(object):
    WRITES = set(['append', 'pexpireat', 'pfadd', 'bgrewriteaof', 'bgsave', 'pfmerge', 'bitop', 'psetex', 'blpop', 'brpop', 'brpoplpush', 'publish', 'client_pause', 'rename', 'client_setname', 'renamenx', 'restore', 'rpop', 'rpoplpush', 'rpush', 'rpushx', 'config_rewrite', 'sadd', 'config_set', 'save', 'config_resetstat', 'script_flush', 'debug_segfault', 'script_kill', 'decr', 'script_load', 'decrby', 'sdiff', 'del', 'sdiffstore', 'discard', 'select', 'set', 'setbit', 'eval', 'setex', 'evalsha', 'setnx', 'exec', 'setrange', 'shutdown', 'expire', 'sinter', 'expireat', 'sinterstore', 'flushall', 'flushdb', 'slaveof', 'slowlog', 'smove', 'getset', 'sort', 'hdel', 'spop', 'srem', 'hincrby', 'hincrbyfloat', 'sunion', 'sunionstore', 'sync', 'hmset', 'hset', 'hsetnx', 'unwatch', 'incr', 'watch', 'incrby', 'zadd', 'incrbyfloat', 'zincrby', 'zinterstore', 'linsert', 'lpop', 'lpush', 'zrem', 'lrem', 'zremrangebylex', 'lset', 'zremrangebyrank', 'ltrim', 'zremrangebyscore', 'monitor', 'move', 'mset', 'zunionscore', 'msetnx', 'multi', 'persist', 'pexpire'])
    READS = set(['auth', 'pfcount', 'bitcount', 'ping', 'bitpos', 'psubscribe', 'pubsub', 'pttl', 'client_list', 'quit', 'client_getname', 'randomkey', 'cluster_slots', 'command', 'role', 'command_count', 'command_getkeys', 'command_info', 'config_get', 'scard', 'dbsize', 'script_exists', 'debug_object', 'dump', 'echo', 'exists', 'sismember', 'get', 'getbit', 'smembers', 'getrange', 'hexists', 'srandmember', 'hget', 'hgetall', 'strlen', 'subscribe', 'hkeys', 'hlen', 'hmget', 'time', 'ttl', 'type', 'unsubscribe', 'hvals', 'zcard', 'info', 'zcount', 'keys', 'lastsave', 'lindex', 'zlexcount', 'zrange', 'llen', 'zrangebylex', 'zrevrangebylex', 'zrangebyscore', 'lpushx', 'zrank', 'lrange', 'mget', 'migrate', 'zrevrange', 'zrevrangebyscore', 'zrevrank', 'zscore', 'scan', 'sscan', 'object', 'hscan', 'zscan'])

    def __init__(self, shards):
        self.__shards = shards

    def route(self, key):
        """
        :param str key: Given a key; this function returns the appropriate shard to route the associated request to.
        :rtype: :py:class:`phonon.shard.Shard`
        :returns: The shard to route this request to.
        """
        return self.__shards[zlib.crc32(key) % len(self.__shards)].nodes()

