import datetime
import json

from dateutil import parser

from phonon import get_logger, PHONON_NAMESPACE, LOCAL_TZ, TTL
from phonon.nodelist import Nodelist

logger = get_logger(__name__)


class Reference(object):
    """
    Represents a reference to some resource in the network. Handles reference
    counting, dereferencing, locking/ownership, and maintaining whether or not
    resources were externally modified.

    This class also provides a layer of abstraction on failover and cleanup. In
    the event of total process failure; a reference on that process will expire
    and be subsequently detected/recovered by this class to the extent updates
    overlap.

    More in-depth:

    When a process acquires a `Reference` to a resource it attempts to lock
    that resource. It will block by default for a total of Process.BLOCKING_TIMEOUT
    seconds (defaults to 500s) before raising a Reference.AlreadyLocked
    exception

    That process/reference will attempt to connect to the backend, and retry
    until either successful completion of the task or python's maximum
    recursion depth has been reached (100 retries), sleeping
    Process.RETRY_SLEEP seconds between each retry (defaults to 0.5s).

    Upon successfully locking; `Reference` will create a record with the key
    "{0}.reflist".format(pid) in redis where the key is the unique id for the
    process this code is running on and the value is the unix timestamp for the
    time the process last modified that entry. If a pid is in that list it is
    assumed they currently have an active session for whatever resource is
    locked. If the record was created longer ago than the session length it is
    assumed the process "fell over". It's reference will be cleaned up and the
    reference count will be decremented.

    Reference counts are calculated by the length of the json structure
    described above after process failures have been resolved.

    """

    def __init__(self, process, resource, block):
        """
        :param Process process: The Process to which this reference belongs
        :param str resource: An identifier for the resource. For example:
            Buzz.12345
        :param bool block: Optional. Whether or not to block when establishing
            locks.

        """

        self.resource_key = resource
        self.block = block
        self.nodelist = Nodelist(process, resource)
        self.times_modified_key = "{0}_{1}.times_modified".format(PHONON_NAMESPACE, resource)
        self.force_expiry = False
        self.__process = process
        self.refresh_session()

    def lock(self, block=None):
        """
        Issues a lock for this reference

        Usage:
            with reference.lock():
                pass

        :param bool block: Optional. Whether or not to block when establishing
            lock.
        """

        block = self.block if block is None else block
        return self.__process.lock(self.resource_key, block)

    def refresh_session(self):
        """
        Update the session for this node. Specifically; lock on the reflist,
        then update the time  this node acquired the reference.

        This method should only be called while the reference is locked.
        """
        expired_nodes = self.nodelist.find_expired_nodes()
        if expired_nodes:
            with self.lock():
                self.nodelist.remove_expired_nodes(expired_nodes)

        self.nodelist.refresh_session()

    def increment_times_modified(self):
        """
        Increments the number of times this resource has been modified by all
        processes.
        """
        client = self.__process.client
        key = self.times_modified_key
        rc = client.setnx(key, 1)
        if not rc:
            client.incr(key, 1)
        else:
            client.pexpire(key, TTL * 1000)  # ttl is in ms

    def get_times_modified(self):
        """
        :returns: The total number of times increment_times_modified has been called for this resource by all processes.
        :rtype: int
        """
        key = self.times_modified_key
        client = self.__process.client
        times_modified = client.get(key)
        if times_modified is None:
            return 0
        return int(times_modified)

    def count(self):
        """
        :returns: The total number of elements in the reference list.
        :rtype: int
        """
        return self.nodelist.count()

    def dereference(self, callback=None, args=None, kwargs=None, block=None):
        """
        This method should only be called while the reference is locked.

        Decrements the reference count for the resource. If this process holds
        the only reference at the time we finish dereferencing it; True is
        returned. Operating on the resource after it has been dereferenced is
        undefined behavior.

        Dereference queries the value stored in the backend, if any, iff (if
        and only if) this instance is the last reference to that resource. e.g.
        self.count() == 0

        :param function callback: A function to execute iff it's determined
            this process holds the only reference to the resource. When there
            is a failure communicating with the backend in the cleanup step the
            callback function will be called an additional time for that
            failure and each subsequent one thereafter. Ensure your callback
            handles this properly.
        :param tuple args: Positional arguments to pass your callback.
        :param dict kwargs: keyword arguments to pass your callback.

        :returns: Whether or not there are no more references among all
            processes. True if this was the last reference. False otherwise.
        :rtype: bool
        """
        pids = {}
        if args is None:
            args = tuple()
        if kwargs is None:
            kwargs = {}

        client = self.__process.client

        rc = False
        if self.force_expiry:
            rc = True

        with self.lock(block):
            if not rc:
                self.nodelist.remove_node(self.__process.id)
                self.nodelist.remove_expired_nodes()
                rc = self.nodelist.count() == 0

        try:
            if callback is not None and rc:
                callback(*args, **kwargs)
        finally:
            with self.lock(block):
                if rc:
                    client.delete(self.resource_key, self.nodelist.nodelist_key, self.times_modified_key)

            client.hdel(self.__process.registry_key, self.resource_key)

        return rc
