import random
import zlib
from collections import defaultdict
from phonon.exceptions import ConfigError, ArgumentError
from phonon.config.node import Node


class Shard(object):
    """
    A shard is the most basic unit capable of reliable, fault tolerant work. Having many shards allows us to balance load across lots of machines while replicating work across each isolated shard.

    Each shard has half it's nodes in one data center and half in another. A quorum should be large enough that there is always at least one node in a different region that will be effected.
    """
    def __init__(self, name):
        """
        Initializes the shard. You should pass a name/label.

        :param int name: The label for this shard. The names are used for routing. It MUST be an int.
        """
        if not isinstance(name, (int, long)):
            raise ArgumentError("Shard names must be integers.")

        self.name = name
        self.__nodes = set([])
        self.regions = defaultdict(int)

    def add(self, node):
        """
        Assign a node to this shard. Nodes can only be added once. Once added the node will be marked as ASSIGNED. The region count is maintained, and the node will be added to the node list for this shard.

        :param phonon.node.Node node: The node to add to this shard. If the shard already has two regions the node should be in one of those regions.
        """
        if node in self.__nodes:
            raise ArgumentError("A node can only be added to a shard once.")
        self.__nodes.add(node)
        self.regions[node.region] += 1
        node.mark_as(Node.ASSIGNED)

    def remove(self, node):
        """
        Removes a node from the shard. The region count will be decremented for that region. The node will be marked once as UNASSIGNED.

        :param phonon.node.Node node: The node to remove from the shard.
        """
        self.__nodes.remove(node)
        self.regions[node.region] -= 1
        if self.regions[node.region] <= 0:
            del self.regions[node.region]
        node.mark_as(Node.UNASSIGNED)

    def nodes(self):
        """
        Return the working set of nodes for this shard.

        :returns: A list of nodes in this shard.
        :rtype: set(phonon.node.Node)
        """
        return self.__nodes

    def has_region(self, region_name):
        """
        Check whether this shard contains any nodes in the region `region_name`.

        :param str region_name: The name of the region we're interested in.
        :rtype: bool
        :returns: Whether or not the node is in the region.
        """
        return region_name in self.regions

class Shards(object):
    """
    Shards is a container for all the shards in the network configuration. Once established the shard configuration is submitted to redis. From that point on no nodes can re-submit a configuration by re-initializing.

    Shards is responsible for validating configuration and assigning a reasonable default configuration given a set of nodes and their regions. This class also handles submitting the configuration to be accepted as the global configuration, and it offers an API to modify the configuration at run-time.
    """

    def __init__(self, nodelist=None, shards=None, quorum_size=None, shard_size=None):
        if not isinstance(shards, int):
            raise ArgumentError("You must pass an integer number of shards")
        if not isinstance(quorum_size, int) or quorum_size < 2:
            raise ArgumentError("Error configuring quorum size. Must be an integer > 1.")
        if not nodelist:
            raise ArgumentError("You must pass a nodelist")
        if len(nodelist.keys()) < 2:
            raise ArgumentError("You must specify at least two data centers")
        for region, nodes in nodelist.items():
            if len(nodes) < 2:
                raise ArgumentError("Every region must contain at least two nodes.")

        self.__quorum_size = quorum_size
        self.__shard_size = shard_size
        self.__nodelist = nodelist

        unassigned = True
        while unassigned: # Generally shards >> regions or nodes ensuring a good balance
            self.__shards = [Shard(name=n) for n in xrange(shards)]
            for shard in self.__shards:
                a, b = tuple(random.sample(self.__nodelist.keys(), 2))
                ideal_balance = self.__shard_size/2
                nodelist = random.sample(self.__nodelist[a], ideal_balance)
                nodelist += random.sample(self.__nodelist[b], ideal_balance)
                for node in nodelist:
                    shard.add(node)
            unassigned = self.unassigned()

    def shards(self):
        return self.__shards

    def nodes(self):
        return {node for region, nodes in self.__nodelist.items() for node in nodes}

    def unassigned(self):
        unassigned = []
        for region, nodes in self.__nodelist.items():
            for node in nodes:
                if node.status is Node.UNASSIGNED:
                    unassigned.append(node)
        return unassigned

    def submit(self):
        """
        Attempts to lock and submit the topology defined for this worker machine to all workers. If another node has already submitted it's topology successfully; the registration for this topology is aborted and `conform` is called, to accept the winner.
        """
        pass

    def conform(self):
        """
        If another node has successfully registered it's topology; when `register` is aborted this method will be called to accept the winning topology.
        """
        pass

    def add_node(self, node, shard_name=None):
        """
        Adds a node to whichever shard has the lowest machine/request ratio if no shard is specified. Otherwise it adds a node to the shard specified.

        :param phonon.node.Node node: The node to add.
        :param int shard_name: The name of the shard to add `node` to.

        :returns: True if it was successful. False otherwise.
        :rtype: bool
        """
        pass

    def stats(self):
        """
        Returns the current statistics for the current configuration from the perspective of the host it's called on.

        :returns: The statistics for the shard configuration.
        :rtype: dict
        """
        stats = {'standby': 0, 'initializing': 0, 'ready': 0, 'assigned': 0, 'unassigned': 0,
                 'quorum_size': self.__quorum_size, 'shard_size': self.__shard_size}

        for region, nodes in self.__nodelist.items():
            for node in nodes:
                if node.status is Node.INITIALIZING:
                    stats['initializing'] += 1
                elif node.status is Node.READY:
                    stats['ready'] += 1
                elif node.status is Node.STANDBY:
                    stats['standby'] += 1
                elif node.status is Node.UNASSIGNED:
                    stats['unassigned'] += 1
                elif node.status is Node.ASSIGNED:
                    stats['assigned'] += 1

        return stats

