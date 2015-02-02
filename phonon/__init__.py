import pytz
import math
import logging
import redis
import socket
import socket.error
import time
import sys

from collections import defaultdict
from operator import itemgetter

from redis.exceptions import ConnectionError, TimeoutError,
from phonon.exceptions import ConfigError, ArgumentError, EmptyResult, NoMajority

from phonon.router import Router
from phonon.config.node import Node
from phonon.config.shard import Shards, Shard

from phonon.logger import get_logger

LOCAL_TZ = pytz.utc
PHONON_NAMESPACE = "phonon"
TTL = 30 * 60 
SYSLOG_LEVEL = logging.WARNING
SHARDS = None

logger = get_logger(__name__)

def default_quorum_size(shard_size=None):
    """
    Figures out the minimum quorum size for a given shard size assuming the nodes are split evenly between regions.

    :param int shard_size: The size of the shard.

    :rtype: int
    :returns: The minimum viable quorum size.
    """
    if not shard_size or shard_size < 2:
        raise ArgumentError("Shard size is required")
    quorum_size = int(math.ceil(float(shard_size) / 2.))
    if shard_size % quorum_size == 0:
        return quorum_size + 1
    return quorum_size

def default_shard_size(config):
    """
    Figures out the smallest viable shard size based on the config passed. The size is determined is twice the number of nodes in the region with the smallest amount of nodes.

    :param dict config: The configuration of the form {"region_name": ["hostname", ...]}

    :returns: The default size for the shards based on the configuration passed.
    :rtype: int
    """
    region_sizes = []
    for region, hostnames in config.items():
        if isinstance(hostnames, list):
            region_sizes.append(len(hostnames))
    region_sizes.sort()
    return 2 * min(region_sizes)

def config_to_nodelist(config):
    """
    Converts a configuration of hostnames to the same data structure with the hostnames replaced by phonon.node.Node objects.

    :param dict config: The configuration of the form {"region_name": ["hostname", ...]}

    :returns: A nodelist. a dictionary of lists of nodes by region
    """
    nodelist = defaultdict(list)
    for region, hostnames in config.items():
        if isinstance(hostnames, list):
            for hostname in hostnames:
                nodelist[region].append(Node(hostname=hostname, region=region))
    return nodelist

def configure(config, quorum_size=None, shard_size=None, shards=100, log_level=logging.WARNING):
    """
    Configures the global topology. The configuration you pass should be well balanced by region. You can specify a number of shards, but it should be much more than you think you'll ever need for the foreseeable future. There's no reason not to specify a large number. Really, be generous. You'll have to live with this decision.

    :param dict config: The configuration of the form {"region_name": ["hostname", ...]}
    :param int quorum_size: If you want to enforce a quorum size greater than the default beware it's possible two regions could be paired that don't have enough nodes to establish a quorum together. This results in a deadlock for a subset of requests.
    :param int shard_size:
    :param int shards:
    :param int log_level:
    """
    global SYSLOG_LEVEL
    global SHARDS
    SYSLOG_LEVEL = log_level
    shard_size = shard_size or default_shard_size(config)
    quorum_size = quorum_size or default_quorum_size(shard_size)
    SHARDS = Shards(nodelist=config_to_nodelist(config),
                      shards=shards,
                      quorum_size=quorum_size,
                      shard_size=shard_size)

class Client(object):
    """
    The client provides an abstraction over the normal redis-py StrictRedis client. It implements the router to handle writing to shards for you.

    The major difference from this client and Redis or StrictRedis is this client will return a list of return values, one for each
    node in the shard the request was routed to.
    """
    WRITES = set(['append', 'pexpireat', 'pfadd', 'bgrewriteaof', 'bgsave', 'pfmerge', 'bitop', 'psetex', 'blpop', 'brpop', 'brpoplpush', 'publish', 'client_pause', 'rename', 'client_setname', 'renamenx', 'restore', 'rpop', 'rpoplpush', 'rpush', 'rpushx', 'config_rewrite', 'sadd', 'config_set', 'save', 'config_resetstat', 'script_flush', 'debug_segfault', 'script_kill', 'decr', 'script_load', 'decrby', 'sdiff', 'del', 'sdiffstore', 'discard', 'select', 'set', 'setbit', 'eval', 'setex', 'evalsha', 'setnx', 'exec', 'setrange', 'shutdown', 'expire', 'sinter', 'expireat', 'sinterstore', 'flushall', 'flushdb', 'slaveof', 'slowlog', 'smove', 'getset', 'sort', 'hdel', 'spop', 'srem', 'hincrby', 'hincrbyfloat', 'sunion', 'sunionstore', 'sync', 'hmset', 'hset', 'hsetnx', 'unwatch', 'incr', 'watch', 'incrby', 'zadd', 'incrbyfloat', 'zincrby', 'zinterstore', 'linsert', 'lpop', 'lpush', 'zrem', 'lrem', 'zremrangebylex', 'lset', 'zremrangebyrank', 'ltrim', 'zremrangebyscore', 'monitor', 'move', 'mset', 'zunionscore', 'msetnx', 'multi', 'persist', 'pexpire'])
    READS = set(['auth', 'pfcount', 'bitcount', 'ping', 'bitpos', 'psubscribe', 'pubsub', 'pttl', 'client_list', 'quit', 'client_getname', 'randomkey', 'cluster_slots', 'command', 'role', 'command_count', 'command_getkeys', 'command_info', 'config_get', 'scard', 'dbsize', 'script_exists', 'debug_object', 'dump', 'echo', 'exists', 'sismember', 'get', 'getbit', 'smembers', 'getrange', 'hexists', 'srandmember', 'hget', 'hgetall', 'strlen', 'subscribe', 'hkeys', 'hlen', 'hmget', 'time', 'ttl', 'type', 'unsubscribe', 'hvals', 'zcard', 'info', 'zcount', 'keys', 'lastsave', 'lindex', 'zlexcount', 'zrange', 'llen', 'zrangebylex', 'zrevrangebylex', 'zrangebyscore', 'lpushx', 'zrank', 'lrange', 'mget', 'migrate', 'zrevrange', 'zrevrangebyscore', 'zrevrank', 'zscore', 'scan', 'sscan', 'object', 'hscan', 'zscan'])
    MAX_CONNECTION_RETRIES = 10
    MAX_OPERATION_RETRIES = 5
    CONNECTION_INITIAL_WAIT = 0.5 # Seconds

    def __init__(self):
        global SHARDS
        self.__router = Router(SHARDS)
        self.__connections = {}

    def has_connection(self, hostname):
        return hostname in self.__connections

    def __connect(self, host, port, db):
        wait_period = self.CONNECTION_INITIAL_WAIT
        retries = 0
        while retries < self.MAX_CONNECTION_RETRIES:
            try:
                if retries > 0:
                    time.sleep(wait_period)
                self.__connections[host] = redis.StrictRedis(host=host, port=port, db=db)
                break
            except (ConnectionError, TimeoutError, socket.error), e:
                logger.warning("Failed to connect to {0}:{1}: {2}".format(host, port, e))
                retries += 1
                wait_period *= 2

    def __consensus(self, votes):
        """
        Determines the majority vote given a set of votes. Returns the indexes of the votes inconsistent with the majority.

        :raises: :py:class:`phonon.exceptions.NoMajority` if no vote achieves a majority.
        :raises: :py:class:`phonon.exceptions.EmptyResult` if no votes are found at all.
        :param list( mixed ) votes: A list of votes for the value of a read. The index of the vote is the node's index on the shard.
        :returns: The majority vote and a list of indexes for the nodes that returned values differing from the majority.
        """
        tally = defaultdict(int)
        for vote in votes:
            tally[vote] += 1
        ordered = sorted(tally.items(), key=itemgetter(1))
        if ordered:
            max_val = ordered[:-1][1]
            if max_val / sum(tally.values()) > 0.5:
                majority = ordered[:-1][0]
                if majority is None:
                    raise NoMajority("Majority was None")
                inconsistencies = [i for i, v in enumerate(votes) if v != majority]
                return majority, inconsistencies
            raise NoMajority("No majority found on shard for key")
        raise EmptyResult("No result at all from the shard.")

    def __correct_inconsistent(self, nodes, key, correct_value):
        # TODO: Add expiration based on max expiry of existing records.
        for node in nodes:
            if node.address not in self.__connections:
                self.__connect(node.address, node.port, 0)
            self.__connections[node.address].set(key, correct_value)

    def __getattr__(self, item):
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < self.MAX_OPERATION_RETRIES:
                try:
                    key = args[0] if args else kwargs.get('key')
                    nodes = self.__router.route(key)
                    rvalues = []
                    for node in nodes:
                        try:
                            if node.address not in self.__connections:
                                self.__connect(node.address, node.port, 0)
                            rvalues.append(getattr(self.__connections[node.address], item)(*args, **kwargs))
                        except Exception, e:
                            rvalues.append(None)

                    majority, inconsistencies = self.__consensus(rvalues)
                    if item in self.READS and inconsistencies:
                        self.__correct_inconsistent(inconsistencies, key, majority)
                    elif inconsistencies:
                        if majority: # Majority success
                            # TODO: Handle this case.
                        else: # Majority failure
                            # TODO: Handle this case.


                    return majority
                except (NoMajority, EmptyResult), e:
                    logger.warning("No nodes reachable or conflicts encountered during read operation. Attempting to fix inconsistencies: {0}".format(e))
                    retries += 1

        return wrapper


