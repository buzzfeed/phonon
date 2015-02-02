import datetime
import json
import sys

from dateutil import parser

from phonon import get_logger, PHONON_NAMESPACE, LOCAL_TZ

logger = get_logger(__name__)

class Reference(object):
    """
    Represents a reference to some resource in the network. Handles reference
    counting, dereferencing, locking/ownership, and maintaining whether or not
    resources were externally modified.

    This class also provides a layer of abstraction on failover and cleanup. In
    the event of total process failure; a reference on that process will expire
    and be subsequenty detected/recovered by this class to the extent updates
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
        self.reflist_key = "{0}_{1}.reflist".format(PHONON_NAMESPACE, resource)
        self.times_modified_key = "{0}_{1}.times_modified".format(PHONON_NAMESPACE, resource)
        self.__lock = None
        self.__process = process

        with self.lock(self.block):
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

        return self.__process.lock(self.resource_key, block)

    def refresh_session(self):
        """
        Update the session for this node. Specifically; lock on the reflist,
        then update the time  this node acquired the reference.

        This method should only be called while the reference is locked.
        """
        client = self.__process.client
        reflist = {}
        s_reflist = client.get(self.reflist_key)

        if s_reflist is not None:
            try:
                reflist = self.remove_failed_processes(json.loads(s_reflist))
            except ValueError:
                logger.error("Expected some json but got {0} when parsing reflist".format(s_reflist))
                pass

        reflist[self.__process.id] = datetime.datetime.now(LOCAL_TZ).isoformat()

        client.set(self.reflist_key, json.dumps(reflist))


    def increment_times_modified(self):
        """
        Increments the number of times this resource has been modified by all
        processes.

        This method should only be called while the reference is locked.
        """
        client = self.__process.client
        key = self.times_modified_key
        rc = client.setnx(key, 1)
        if not rc:
            client.incr(key, 1)
        else:
            client.pexpire(key, self.__process.TTL * 1000) # ttl is in ms

    def get_times_modified(self):
        """
        This method should only be called while the reference is locked.

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
        This method should only be called while the reference is locked.
        
        :returns: The total number of elements in the reference list.
        :rtype: int
        """
        client = self.__process.client
        reflist = client.get(self.reflist_key)
        if reflist is None:
            return 0
        return len(json.loads(reflist))

    def remove_failed_process(self, pid):
        """
        Removes a particular process id from this reference's 
        reflist. This method should only be called while the reference is 
        locked.

        :param pid: A string representing the process id to remove.

        """
        client = self.__process.client
        s_reflist = client.get(self.reflist_key)
        reflist = json.loads(s_reflist)

        if pid in reflist:
            del reflist[pid]
            client.set(self.reflist_key, json.dumps(reflist))


    def remove_failed_processes(self, pids):
        """
        When a process has held a reference for longer than Reference.TTL
        without refreshing it's session; this method will detect, log, and
        prune that reference. This is a low-level method that doesn't do any
        querying. 

        :param pids: A dictionary of str -> str keyed on the process id, with
            the value being the last time the session was refreshed for that
            process.
        :type pids: dict( str, str )

        :returns: The input dict of pids with expired pids removed.
        :rtype: dict( str, str )
        """
        for pid, iso_date in pids.items():
            last_updated = parser.parse(iso_date)
            if (datetime.datetime.now(LOCAL_TZ) - last_updated) > datetime.timedelta(seconds=self.__process.session_length):
                del pids[pid]
        return pids

    def dereference(self, callback=None, args=None, kwargs=None):
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
        reflist = client.get(self.reflist_key)

        if reflist is not None:
            pids = json.loads(reflist)
            if self.__process.id in pids:  # It won't be here if a dereference previously failed at the delete step.
                del pids[self.__process.id]

        # Check for failed processes
        pids = self.remove_failed_processes(pids)
        rc = True
        if pids:
            rc = False # This is not the last process  

        try:
            val = json.dumps(pids)
            client.set(self.reflist_key, val)
            if callback is not None and rc:
                callback(*args, **kwargs)
        finally:
            if rc:
                client.delete(self.resource_key, self.reflist_key, self.times_modified_key)

        client.hdel(self.__process.registry_key, self.resource_key)

        return rc

