import random
from collections import defaultdict
from phonon.exceptions import ConfigError
from phonon.config.node import Node


class Shard(object):

    def __init__(self, name):
        self.name = name
        self.nodes = {}
        self.regions = defaultdict(int)

    def add(self, node):
        """
        Assign a node to this shard.
        """
        self.nodes[node.address] = node
        self.regions[node.region] += 1
        node.mark_as(Node.ASSIGNED)

    def remove(self, node):
        del self.nodes[node.address]
        self.regions[node.region] -= 1
        if self.regions[node.region] <= 0:
            del self.regions[node.region]
        node.mark_as(Node.UNASSIGNED)

    def nodes(self):
        return [node for nodes in self.nodes.values() for node in nodes]

    def has_region(self, region_name):
        return region_name in self.regions

class Shards(object):

    def __init__(self, nodelist=None, num=None, quorum_size=None):
        if not isinstance(num, int):
            raise ConfigError("You must pass an integer number of shards")
        if not isinstance(quorum_size):
            raise ConfigError("Error configuring quorum size.")

        self.__shards = [Shard(name=n) for n in xrange(num)]
        self.__quorum_size = quorum_size
        self.__shard_size = (quorum_size - 1) * 2
        self.__nodelist = nodelist

        for shard in self.__shards:
            a, b = tuple(random.sample(self.__nodelist.keys(), 2)) # Need to enforce all regions are used at least once
            nodelist = random.sample(self.__nodelist[a], self.__shard_size/2)
            nodelist += random.sample(self.__nodelist[b], self.__shard_size/2)
            for node in nodelist:
                shard.add(node)

        # Make sure all nodes were assigned at least once.
        unassigned = self.unassigned()
        while unassigned:
            for node in unassigned:
                while node.status is not Node.ASSIGNED:
                    shard = random.choice(self.__shards)
                    if shard.has_region(node.region):
                        to_replace = random.choice(shard.nodes())
                        if node.region == to_replace.region:
                            shard.remove(to_replace)
                            shard.add(node)
            unassigned = self.unassigned()

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
