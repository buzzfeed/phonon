import random
from collections import defaultdict
from phonon.exceptions import ConfigError, ArgumentError
from phonon.config.node import Node


class Shard(object):

    def __init__(self, name):
        self.name = name
        self.__nodes = set([])
        self.regions = defaultdict(int)

    def add(self, node):
        """
        Assign a node to this shard.
        """
        if node in self.__nodes:
            raise ArgumentError("A node can only be added to a shard once.")

        self.__nodes.add(node)
        self.regions[node.region] += 1
        node.mark_as(Node.ASSIGNED)

    def remove(self, node):
        self.__nodes.remove(node)
        self.regions[node.region] -= 1
        if self.regions[node.region] <= 0:
            del self.regions[node.region]
        node.mark_as(Node.UNASSIGNED)

    def nodes(self):
        return self.__nodes

    def has_region(self, region_name):
        return region_name in self.regions

class Shards(object):

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

    def register(self):
        """
        Attempts to lock and submit the topology defined for this worker machine to all workers. If another node has already submitted it's topology successfully; the registration for this topology is aborted and `conform` is called, to accept the winner.
        """
        pass

    def conform(self):
        """
        If another node has successfully registered it's topology; when `register` is aborted this method will be called to accept the winning topology.
        """
        pass

    def stats(self):
        """
        Returns the numbers used for this configuration
        """
        stats = {'standby': 0, 'initializing': 0, 'ready': 0, 'assigned': 0, 'unassigned': 0,
                 'quorum_size': self.__quorum_size, 'shard_size': self.__shard_size}

        for region, nodes in self.__nodelist.items():
            for node in nodes:
                if node.status == Node.INITIALIZING:
                    stats['initializing'] += 1
                elif node.status == Node.READY:
                    stats['ready'] += 1
                elif node.status == Node.STANDBY:
                    stats['standby'] += 1
                elif node.status == Node.UNASSIGNED:
                    stats['unassigned'] += 1
                elif node.status == Node.ASSIGNED:
                    stats['assigned'] += 1

        return stats
