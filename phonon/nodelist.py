import datetime

from dateutil import parser

from phonon import get_logger, PHONON_NAMESPACE, LOCAL_TZ

class Nodelist(object):
    """
    Keeps track of the nodes currently holding a reference to a particular
    resource by maintaining a hash in redis with the node ids and time the
    session was last refreshed.
    """

    def __init__(self, process, resource_key):
        """
        :param Process process: The Process to which the instantiating
            reference belongs
        :param string resource_key: An identifier for the instantiating
            reference
        """
        self.resource_key = resource_key
        self.nodelist_key = "{0}_{1}.nodelist".format(PHONON_NAMESPACE, resource_key)
        self.__process = process
        self.refresh_session()

    def refresh_session(self, node_id=None):
        """
        Adds or refreshes a particular node in the nodelist, attributing the
        current time with the node_id.

        :param string node_id: optional, the process id of the node whose
        session should be refreshed
        """
        if not node_id:
            node_id = self.__process.id
        self.__process.client.hset(self.nodelist_key, node_id, datetime.datetime.now(LOCAL_TZ).isoformat())

    def find_expired_nodes(self, node_ids=None):
        """
        Detects processes that have held a reference for longer than its
        process_ttl without refreshing its session. This function does not
        actually removed them from the hash. (See remove_expired_nodes.)

        :param list node_ids: optional, a list of ids to check to see if they
            have expired.  If node_ids is not passed in, all nodes in the hash
            will be checked.
        """
        if node_ids:
            nodes = zip(node_ids, [parser.parse(dt) for dt in self.__process.client.hmget(self.nodelist_key, node_ids)])
        else:
            nodes = self.get_all_nodes().items()

        expiration_delta = datetime.timedelta(seconds=self.__process.process_ttl)
        now = datetime.datetime.now(LOCAL_TZ)
        return [node_id for (node_id, last_updated) in nodes if (now - last_updated) > expiration_delta]

    def remove_expired_nodes(self, node_ids=None):
        """
        Removes all expired nodes from the nodelist.  If a set of node_ids is
        passed in, those ids are checked to ensure they haven't been refreshed
        prior to a lock being acquired.

        Should only be run with a lock.

        :param list node_ids: optional, a list of node_ids to remove.  They
            will be verified to ensure they haven't been refreshed.

        """
        nodes = self.find_expired_nodes(node_ids)
        if nodes:
            self.__process.client.hdel(self.nodelist_key, *nodes)

    def remove_node(self, node_id=None):
        """
        Removes a particular node from the nodelist.

        Should only be run with a lock.

        :param string node_id: optional, the process id of the node to remove
        """
        if not node_id:
            node_id = self.__process.id

        self.__process.client.hdel(self.nodelist_key, node_id)

    def clear_nodelist(self):
        """
        Removes all nodes from a nodelist.

        Should only be run with a lock.
        """
        self.__process.client.delete(self.nodelist_key)

    def get_last_updated(self, node_id=None):
        """
        Returns the time a particular node has been last refreshed.

        :param string node_id: optional, the process id of the node to retrieve

        :rtype: None, datetime
        :returns: Returns a parsed datetime if exists, otherwise None
        """
        if not node_id:
            node_id = self.__process.id

        dt = self.__process.client.hget(self.nodelist_key, node_id)
        return parser.parse(dt) if dt else None

    def get_all_nodes(self):
        """
        Returns all nodes in the hash with the time they were last refreshed
        as a dictionary.

        :rtype: dict(string, datetime.datetime)
        :returns: A dictionary of strings and corresponding datetime objects

        """
        nodes = self.__process.client.hgetall(self.nodelist_key)
        return {node_id: parser.parse(dt) for (node_id, dt) in nodes.items()}

    def count(self):
        """
        :rtype: int
        :returns: The number of nodes in the nodelist
        """
        return self.__process.client.hlen(self.nodelist_key)
