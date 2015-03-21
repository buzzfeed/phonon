
__author__ = 'andrew'

import time
from operator import itemgetter
from collections import defaultdict
from socket import error as socket_error

import redis
from redis.exceptions import ConnectionError, TimeoutError

from phonon.exceptions import ClientError, EmptyResult, NoMajority, Rollback, ReadError, WriteError
from phonon.client import config
from phonon.client.router import Router
from phonon.operation import OPERATIONS
from phonon.operation import Operation
from phonon.operation import WriteOperation
from phonon.operation import ReadOperation
from phonon.logger import get_logger

logger = get_logger(__name__)


class Call(object):

    def __init__(self, func, args, kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs


class Client(object):

    """
    The client provides an abstraction over the normal redis-py StrictRedis client. It implements the router to handle writing to shards for you with strong guarantees.

    The major difference from this client and Redis or StrictRedis is this client writes to every node on a shard; in a 2-phase commit protocol for strong consistency.

    When you execute a `set` operation, for example, each node in the shard responsible for records corresponding to the input key will be set with the value and a data structure representing an uncommitted `set` operation. If those calls all succeed; the datastructure is updated to reflect the operation is committed. If ANY call fails during this process; all the nodes are "rolled back". That is; the "undo" operation for the data structure is executed and the previous operation replaces the data structure for the set operations, whatever it may be. It's state is marked as 'committed'.
    """
    MAX_CONNECTION_RETRIES = 10
    MAX_OPERATION_RETRIES = 5
    CONNECTION_INITIAL_WAIT = 0.5  # Seconds

    def __init__(self):
        self.__router = Router(config.SHARDS)
        self.__connections = {}

    def has_connection(self, node):
        return node.address in self.__connections

    def get_connection(self, node):
        if not self.has_connection(node):
            self.__connect(node)
        return self.__connections[node.address]

    def __connect(self, node):
        wait_period = self.CONNECTION_INITIAL_WAIT
        retries = 0
        while retries < self.MAX_CONNECTION_RETRIES:
            try:
                if retries > 0:
                    time.sleep(wait_period)
                self.__connections[node.address] = redis.StrictRedis(host=node.address, port=node.port, db=0)
                return None
            except (ConnectionError, TimeoutError, socket_error), e:
                logger.warning("Failed to connect to {0}:{1}: {2}".format(node.address, node.port, e))
                retries += 1
                wait_period *= 2
        raise ClientError("Failed to connect to {0}".format(node))

    def pipeline(self, node):
        """
        :param node: The node to return a pipeline for.
        :type node: :py:class:`phonon.node.Node`
        :returns: A pipeline to a particular node. This is a transaction context that will be executed locally.
        :rtype: redis.StrictRedis.pipeline
        """
        return self.get_connection(node).pipeline()

    def __get_consensus(self, op):
        """
        Arrives at a consensus for a read, if possible.
        :param op:
        :param args:
        :param kwargs:
        :return:
        """
        votes = []
        keys, args, kwargs = op.keys()
        for key in keys:
            nodes = self.__router.route(key)
            for node in nodes:
                if not self.has_connection(node):
                    self.__connect(node)
                try:
                    vote = getattr(self.get_connection(node), op.call.func)(*op.call.args, **op.call.kwargs)
                    votes.append(vote)
                except Exception, e:
                    logger.error("Got a read error while seeking consensus: {0}".format(e))
                    votes.append(ReadError("Bad response from node."))
                    # [TODO: Flag node a PFAIL]
        return self.__get_majority_and_inconsistencies(votes)

    def __get_majority_and_inconsistencies(self, votes):
        """
        Determines the majority vote given a set of votes. Returns the indexes of the votes inconsistent with the majority.

        :raises: :py:class:`phonon.exceptions.NoMajority` if no vote achieves a majority.
        :raises: :py:class:`phonon.exceptions.EmptyResult` if no votes are found at all.
        :param list( mixed ) votes: A list of votes for the value of a read. The index of the vote is the node's index on the shard.
        :returns: The majority vote and a list of indexes for the nodes that returned values differing from the majority.
        """
        if not votes:
            raise EmptyResult("No result at all from the shard.")

        tally = defaultdict(float)
        for vote in votes:
            tally[vote] += 1.

        ordered = sorted(tally.items(), key=itemgetter(1))
        max_val = ordered[-1][1]
        if max_val / sum(tally.values()) > 0.5:
            majority = ordered[-1][0]
            if isinstance(majority, ReadError):
                raise NoMajority("Majority of nodes failed on read.")
            return majority, [i for i, v in enumerate(votes) if v != majority]  # majority, inconsistent
        raise NoMajority("No majority found on shard for key")

    def __rollback(self, pending_op, inconsistent=None):
        # TODO: Add expiration based on max expiry of existing records.
        print "Rollback called."
        keys, args, kwargs = pending_op.keys()
        for key in keys:
            nodes = self.__router.route(key)
            for ind, node in enumerate(nodes):
                try:
                    if inconsistent and ind not in inconsistent:
                        continue

                    conn = self.get_connection(node)
                    currop = Operation.from_str(conn.get('{0}.oplog'.format(key)))
                    print "Getting operation for node", node
                    if currop and not currop.is_committed():
                        undo = currop.undo()
                        pipe = conn.pipeline()

                        # Execute inverse user operation.
                        getattr(pipe, undo[0])(*(keys + undo[1:]))

                        # If it exists; move the last committed operation into the current oplog slot.
                        if conn.exists("{0}.oplog.last".format(key)):
                            pipe.rename("{0}.oplog.last".format(key), "{0}.oplog".format(key))
                        else:
                            pipe.set("{0}.oplog".format(key), None)  # No previous operation. Just clear it out.

                        pipe.execute()
                    else:
                        print "No last op."
                except Exception, e:
                    logger.error("Error during rollback: {0}".format(e))

    def __write_oplog(self, pipeline, key, op):
        pipeline.set("{0}.oplog".format(key), op.to_str())

    def __ensure_committed(self, node, key):
        op = Operation.from_str(self.get_connection(node).get("{0}.oplog".format(key)))
        if not op:
            logger.info("No oplog for entry, must be initial set.")
            return
        if not op.is_committed():
            raise Rollback("Found previously uncommitted entry.")

    def __query_to_commit(self, op):
        try:
            self.move_last_op(op)
            keys, args, kwargs = op.keys()
            for key in keys:
                nodes = self.__router.route(key)
                for node in nodes:
                    pipeline = self.pipeline(node)
                    self.__write_oplog(pipeline, key, op)
                    getattr(pipeline, op.call.func)(key, *args, **kwargs)
                    rc = pipeline.execute()  # [TODO: Check for possible failure/success values. Can they all be evaluated naively as truthy?]
                    if not all(rc):  # Must be unanimous on a shard
                        raise Rollback("Failed to add op to pipeline.")
        except Exception, e:
            raise Rollback("{0}".format(e))  # [TODO: Flag node causing rollback as PFAIL]

    def __commit(self, op):
        try:
            op.commit()
            keys, args, kwargs = op.keys()
            for key in keys:
                nodes = self.__router.route(key)
                for node in nodes:
                    self.__write_oplog(self.get_connection(node), key, op)
            return True
        except Exception, e:
            op.rollback()
            raise Rollback("{0}".format(e))

    def move_last_op(self, newop):
        try:
            keys, args, kwargs = newop.keys()
            for key in keys:
                nodes = self.__router.route(key)
                for node in nodes:
                    self.__ensure_committed(node, key)
                    opkey = "{0}.oplog".format(key)
                    lastopkey = "{0}.oplog.last".format(key)
                    self.get_connection(node).rename(opkey, lastopkey)
        except Exception, e:
            raise Rollback("Failed to move last op.")

    def __getattr__(self, func):
        def wrapper(*args, **kwargs):
            redis_call = Call(func, args, kwargs)
            try:
                try:
                    pending_op = OPERATIONS[func](redis_call)
                except KeyError, e:
                    raise NotImplementedError('The operation, {0}, is not implemented. Please submit a PR to implement it :)'.format(func))

                if isinstance(pending_op, WriteOperation):
                    meta = pending_op.pre_hooks(self)
                    agreement = self.__query_to_commit(pending_op)
                    return self.__commit(pending_op)
                elif isinstance(pending_op, ReadOperation):
                    majority, inconsistencies = self.__get_consensus(pending_op)
                    if inconsistencies:
                        self.__rollback(pending_op, inconsistencies)  # Try to passively correct inconsistencies.
                    return majority
            except Rollback, e:
                logger.error("Caught error causing rollback: {0}".format(e))
                self.__rollback(pending_op)
            except NoMajority, e:
                logger.error("No majority found for key!")
            except EmptyResult, e:
                logger.warning("No nodes reachable or conflicts encountered during read operation. Attempting to fix inconsistencies: {0}".format(e))

            if isinstance(pending_op, WriteOperation):
                raise WriteError("Maximum retries exceeded.")
            raise ReadError("Maximum retries exceeded.")

        return wrapper
