from phonon.exceptions import ConfigError
from phonon.exceptions import ArgumentError

class Node(object):

    INITIALIZING = 0
    READY = 1
    STANDBY = 2
    ASSIGNED = 3
    UNASSIGNED = 4

    def __init__(self, hostname=None, ip=None, port=6379, region=None, status=None):
        """
        Initializes the node object.

        :param str hostname: The hostname for a redis master. Only master nodes should be used.
        :param str ip: The ip address for the redis master.
        :param int port: The port redis is operating on.
        :param str region: The region or data center this node operates in.
        :param int status: The status of the node. Possible values are Node.INITIALIZING, Node.READY, Node.STANDBY
        :raises: ConfigError if no hostname or ip is passed or if port is not an int.
        """
        if hostname is None and ip is None:
            raise ConfigError("Each node must have a hostname and an ip")
        if not isinstance(port, int):
            raise ConfigError("Port number must be an integer")
        if region is None:
            raise ConfigError("You must specify a region for all nodes.")

        self.address = hostname or ip
        self.port = port
        self.region = region
        self.assignments = 0
        self.status = status

        if status is None:
            self.status = Node.UNASSIGNED

    def mark_as(self, status=None):
        """
        Marks a node as a particular status. Valid arguments are Node.INITIALIZING, Node.READY, or Node.STANDBY

        :param int status: The status for this node.
        :raises: phonon.exceptions.ArgumentError if no status is passed.
        """
        if status is None:
            raise ArgumentError("You must pass a status.")

        if status is Node.ASSIGNED:
            self.assignments += 1
            self.status = Node.ASSIGNED
        elif status is Node.UNASSIGNED:
            if self.assignments > 0:
                self.assignments -= 1
            if self.assignments < 1:
                self.status = Node.UNASSIGNED
        else:
            self.status = status


