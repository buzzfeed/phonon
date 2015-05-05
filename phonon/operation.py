__author__ = 'andrew'

import time
import datetime
import zlib
import pickle

from phonon.client.config import LOCAL_TZ


class Operation(object):

    def keys(self, *args, **kwargs):
        return [args[0]], args[1:], kwargs

    def to_str(self):
        return zlib.compress(pickle.dumps(self), 9)

    @classmethod
    def from_str(self, s):
        if not s:
            return None
        return pickle.loads(zlib.decompress(s))

    KEYS = keys
    PRE_HOOKS = {}

    def __init__(self, call):
        """
        Defines an operation.

        :param int typ: Either Operation.READ, Operation.WRITE, or Operation.ADMIN, depending on the type of operation.
        :param callable keys: A callable that takes the args and kwargs for a redis operation and returns a list of effected keys.
        :param callable undo: A callable that takes the current value of the field as it's first argument, the args for the forward execution of this operation as the second argument, and the kwargs as the third. It should return the function along with the arguments required to undo this operation.
        """
        self.call = call
        self.__committed = False
        self.timestamp = time.mktime(datetime.datetime.now(LOCAL_TZ).timetuple())

    def pre_hooks(self, client):
        self.__meta = {}
        for k, hook in self.PRE_HOOKS.items():
            self.__meta[k] = hook(client, self)

    def keys(self):
        """
        Pass the parameter list passed to the redis client for this operation to get back a list of keys effected by
          the operation
        :param args: The arguments passed to the redis client for a given function call.
        :param kwargs: The keywork arguments passed to the client.
        :returns: A tuple of a list of effected keys as well as the appropriate args for executing the same command on a single redis node.
        :rtype: tuple( list( str ), tuple( mixed ) )
        """
        return self.KEYS(*(self.call.args), **(self.call.kwargs))
        # [TODO: Refactor keys function to take a phonon.client.Call]

    def commit(self):
        self.__committed = True

    def rollback(self):
        self.__committed = False

    def is_committed(self):
        return self.__committed

    def undo(self):
        """
        :param mixed pvalue: The value for this key, before the operation is completed.
        :param mixed rvalue: The value for this key, after the operation is completed.
        :param tuple args: The arguments to implement the forward execution of this operation
        :param tuple kwargs: The keyword arguments to implement the forward execution of this operation

        :rtype: list(str, tuple, dict)
        :returns: A function name, args, and kwargs that undo this operation, assuming it was the last applied.
        """
        #[TODO: Refactor undo functions to take a phonon.client.Call]
        return self.UNDO(self.__meta)

ADMIN = set([
    'bgrewriteaof',
    'bgsave',
])

READS = set(['auth', 'pfcount', 'bitcount', 'ping', 'bitpos', 'psubscribe', 'pubsub', 'pttl', 'client_list', 'quit', 'client_getname', 'randomkey', 'cluster_slots', 'command', 'role', 'command_count', 'command_getkeys', 'command_info', 'config_get', 'scard', 'dbsize', 'script_exists', 'debug_object', 'dump', 'echo', 'exists', 'sismember', 'get', 'getbit', 'smembers', 'getrange', 'hexists', 'srandmember', 'hget', 'hgetall', 'strlen', 'subscribe', 'hkeys', 'hlen', 'hmget', 'time', 'ttl', 'type', 'unsubscribe', 'hvals', 'zcard', 'info', 'zcount', 'keys', 'lastsave', 'lindex', 'zlexcount', 'zrange', 'llen', 'zrangebylex', 'zrevrangebylex', 'zrangebyscore', 'lpushx', 'zrank', 'lrange', 'mget', 'migrate', 'zrevrange', 'zrevrangebyscore', 'zrevrank', 'zscore', 'scan', 'sscan', 'object', 'hscan', 'zscan'])


class WriteOperation(Operation):
    pass


class ReadOperation(Operation):
    pass


class Set(WriteOperation):
    PRE_HOOKS = {'pvalue': lambda client, op: client.get(op.call.args[0])}
    UNDO = lambda op, meta: ['set', meta['pvalue']]


class Del(WriteOperation):
    PRE_HOOKS = {'pvalue': lambda client, op: client.get(op.call.args[0])}
    UNDO = lambda op, meta: ['set', meta['pvalue']]


class SetNX(WriteOperation):
    UNDO = lambda op, meta: ['del', op.call.args[0]]


class Incr(WriteOperation):
    UNDO = lambda op, meta: ['decr', op.call.args[0]]


class Decr(WriteOperation):
    UNDO = lambda op, meta: ['incr', op.call.args[0]]


class HSet(WriteOperation):
    PRE_HOOKS = {'pvalue': lambda client, op: client.hget(op.call.args[0], op.call.args[1])}
    UNDO = lambda op, meta: ['hset', op.call.args[0], op.call.args[1], meta['pvalue']]


class HDel(WriteOperation):
    PRE_HOOKS = {'pvalue': lambda client, op: client.hget(op.call.args[0], op.call.args[1])}
    UNDO = lambda op, meta: ['hset', op.call.args[0], op.call.args[1], meta['pvalue']]


class IncrByFloat(WriteOperation):
    PRE_HOOKS = {'pvalue': lambda client, op: client.get(op.call.args[0])}
    UNDO = lambda op, meta: ['set', op.call.args[0], meta['pvalue']]


class HIncrByFloat(WriteOperation):
    PRE_HOOKS = {'pvalue': lambda client, op: client.hget(op.call.args[0], op.call.args[1])}
    UNDO = lambda op, meta: ['set', op.call.args[0], op.call.args[1], meta['pvalue']]


class IncrBy(WriteOperation):
    UNDO = lambda op, meta: ['decrby', op.call.args[0], op.call.args[1]]


class DecrBy(WriteOperation):
    UNDO = lambda op, meta: ['incrby', op.call.args[0], op.call.args[1]]


class HIncrBy(WriteOperation):
    PRE_HOOKS = {'pvalue': lambda client, op: client.hget(op.call.args[0], op.call.args[1])}
    UNDO = lambda op, meta: ['set', op.call.args[0], op.call.args[1], meta['pvalue']]


class PExpire(WriteOperation):
    PRE_HOOKS = {'pvalue': lambda client, op: client.pttl(op.call.args[0])}
    UNDO = lambda op, meta: ['pexpire', op.call.args[0], meta['pvalue']]


class Expire(WriteOperation):
    PRE_HOOKS = {'pvalue': lambda client, op: client.pttl(op.call.args[0])}
    UNDO = lambda op, meta: ['pexpire', op.call.args[0], meta['pvalue'] / 1000.]


class PSetEx(WriteOperation):
    UNDO = lambda pv, rv, k, ms, v: ['psetex', k, pv.ms_exp(), pv.value]


class Append(WriteOperation):
    UNDO = lambda pv, rv, k, v: ['set', k, pv.value]


class BLPop(WriteOperation):
    KEYS = lambda *args, **kwargs: args if not isinstance(args[-1], int) else args[:-1]
    UNDO = lambda pv, rv, *keys_then_timeout: None


class Lock(WriteOperation):
    KEYS = lambda *args, **kwargs: tuple([kwargs['name'], args, {k: v for k, v in kwargs.items() if k != 'name'}])
    UNDO = lambda op, meta: ['del'] + op.keys()[0]

OPERATIONS = {
    # High priority
    'set': Set, 'del': Del, 'setnx': SetNX, 'incr': Incr,
    'decr': Decr, 'hset': HSet, 'hdel': HDel,
    'incrbyfloat': IncrByFloat, 'hincrbyfloat': HIncrByFloat,
    'incrby': IncrBy, 'decrby': DecrBy, 'hincrby': HIncrBy,
    'pexpire': PExpire, 'expire': Expire, 'psetex': PSetEx,
    'append': Append, 'blpop': BLPop, 'lock': Lock,

    'hsetnx': WriteOperation,
    'zincrby': WriteOperation,
    'decrby': WriteOperation,
    'setex': WriteOperation,
    'getset': WriteOperation,
    'hmset': WriteOperation,
    'lset': WriteOperation,
    'flushall': WriteOperation,
    'flushdb': WriteOperation,

    # Medium priority
    'rpop': WriteOperation,
    'rpoplpush': WriteOperation,
    'rpush': WriteOperation,
    'rpushx': WriteOperation,
    'lpush': WriteOperation,

    # Low priority

    'brpop': WriteOperation,
    'brpoplpush': WriteOperation,
    'publish': WriteOperation,
    'client_pause': WriteOperation,
    'rename': WriteOperation,
    'client_setname': WriteOperation,
    'renamenx': WriteOperation,
    'restore': WriteOperation,
    'config_rewrite': WriteOperation,
    'sadd': WriteOperation,
    'config_set': WriteOperation,
    'save': WriteOperation,
    'config_resetstat': WriteOperation,
    'script_flush': WriteOperation,
    'debug_segfault': WriteOperation,
    'script_kill': WriteOperation,
    'script_load': WriteOperation,
    'sdiff': WriteOperation,
    'sdiffstore': WriteOperation,
    'discard': WriteOperation,
    'select': WriteOperation,
    'setbit': WriteOperation,
    'eval': WriteOperation,
    'evalsha': WriteOperation,
    'setrange': WriteOperation,
    'shutdown': WriteOperation,
    'sinter': WriteOperation,
    'sinterstore': WriteOperation,
    'slaveof': WriteOperation,
    'slowlog': WriteOperation,
    'smove': WriteOperation,
    'sort': WriteOperation,
    'spop': WriteOperation,
    'srem': WriteOperation,
    'sunion': WriteOperation,
    'sunionstore': WriteOperation,
    'sync': WriteOperation,
    'unwatch': WriteOperation,
    'watch': WriteOperation,
    'zadd': WriteOperation,
    'zinterstore': WriteOperation,
    'linsert': WriteOperation,
    'lpop': WriteOperation,
    'zrem': WriteOperation,
    'lrem': WriteOperation,
    'zremrangebylex': WriteOperation,
    'zremrangebyrank': WriteOperation,
    'ltrim': WriteOperation,
    'zremrangebyscore': WriteOperation,
    'monitor': WriteOperation,
    'move': WriteOperation,
    'mset': WriteOperation,
    'zunionscore': WriteOperation,
    'msetnx': WriteOperation,
    'persist': WriteOperation,
    'bitop': WriteOperation,
    'pfadd': WriteOperation,
    'pfmerge': WriteOperation
}

OPERATIONS.update({k: ReadOperation for k in READS})
