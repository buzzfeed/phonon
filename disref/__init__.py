import datetime
import time
import json

import pytz
import sherlock
import redis 
from dateutil import parser


LOCAL_TZ = pytz.utc 

class DisRefError(Exception):
    pass

class Reference(object):
    """
    Represents a reference to some resource in the network. Handles reference counting, dereferencing,
    locking/ownership, and maintaining whether or not resources were externally modified.

    This class also provides a layer of abstraction on failover and cleanup. In the event of total process failure; a
    reference on that process will expire and be subsequenty detected/recovered by this class to the extent updates overlap.

    More in-depth:

    When a process acquires a `Reference` to a resource it attempts to lock that resource. It will block by default
    for a total of Reference.TIMEOUT seconds (defaults to 500s) before raising a Reference.AlreadyLocked exception

    That process/reference will attempt to connect to the backend, and retry until either successful completion of the task or
    python's maximum recursion depth has been reached (100 retries), sleeping Reference.RETRY_SLEEP seconds
    between each retry (defaults to 0.5s).

    Upon successfully locking; `Reference` will create a record with the key "{0}.reflist".format(pid) in memcache
    where the key is the unique id for the process this code is running on and the value is the unix timestamp for the time
    the process last modified that entry. If a pid is in that list it is assumed they currently have an active session
    for whatever resource is locked. If the record was created longer ago than the session length it is assumed the process
    "fell over". It's reference will be cleaned up and the reference count will be decremented.

    Reference counts are calculated by the length of the json structure described above after process failures have been
    resolved.

    """
    RETRY_SLEEP = 0.5    # Second
    TIMEOUT = 500
    TTL = 30 * 60  # 30 minutes

    class AlreadyLocked(DisRefError):
        pass

    def __init__(self, pid, resource, block=True, session_length=int(0.5*TTL), host='localhost', port=6379, db=1):
        self.pid = unicode(pid)
        self.block = block
        self.session_length = session_length
        self.reflist_key = "{0}.reflist".format(resource)
        self.resource_key = "{0}.key".format(resource)
        self.times_modified_key = "{0}.times_modified".format(resource)
        self.__lock = None

        if not hasattr(Reference, 'client'):
            Reference.client = redis.Redis(host=host, port=port, db=db)
            sherlock.configure(backend=sherlock.backends.REDIS,
                    expire=self.TTL,
                    retry_interval=self.RETRY_SLEEP,
                    timeout=self.TIMEOUT,
                    client=Reference.client)

        self.refresh_session()


    def lock(self, block=None):
        """
        Locks the resource represented by this reference. 

        :param bool block: Whether or not to block while acquiring the lock.

        :returns: Whether or not the lock was successfully acquired.
        :rtype: bool
        """
        if block is None:
            block = self.block
        if self.__lock is None:
            self.__lock = sherlock.Lock(self.resource_key)

        return self.__lock.acquire(blocking=block)

    def release(self):
        if self.__lock is None:
            return True
        return self.__lock.release()

    def refresh_session(self):
        """
        Update the session for this node. Specifically; lock on the reflist, then update the time  this node acquired the reference.

        This method should only be called while the reference is locked.  
        """
        client = Reference.client
        reflist = {}
        s_reflist = client.get(self.reflist_key)  

        if s_reflist is not None:
            try:
                reflist = self.remove_failed_processes(json.loads(s_reflist))
            except ValueError:
                logger.error("Expected some json but got {0} when parsing reflist".format(s_reflist))
                pass

        reflist[self.pid] = datetime.datetime.now(LOCAL_TZ).isoformat()

        client.set(self.reflist_key, json.dumps(reflist)) 


    def increment_times_modified(self):
        """
        Increments the number of times this resource has been modified by all processes.

        This method should only be called while the reference is locked.
        """
        client = Reference.client
        key = self.times_modified_key 
        rc = client.setnx(key, 1)
        if not rc:
            client.incr(key, 1)
        else:
            client.pexpire(key, Reference.TTL * 1000) # ttl is in ms

        self.refresh_session()

    def get_times_modified(self):
        """
        This method should only be called while the reference is locked.

        :returns: The total number of times increment_times_modified has been called for this resource by all processes. 
        :rtype: int
        """
        key = self.times_modified_key 
        client = Reference.client
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
        client = Reference.client 
        reflist = client.get(self.reflist_key)
        if reflist is None:
            return 0
        return len(json.loads(reflist))

    def remove_failed_processes(self, pids):
        """
        When a process has held a reference for longer than Reference.TTL without refreshing it's session; this method will detect, log, and prune that reference. This is a low-level method that doesn't do any querying. 

        :param pids: A dictionary of str -> str keyed on the process id, with the value being the last time the session was refreshed for that process.
        :type pids: dict( str, str )

        :returns: The input dict of pids with expired pids removed.
        :rtype: dict( str, str )
        """
        for pid, iso_date in pids.items():
            last_updated = parser.parse(iso_date)
            if (datetime.datetime.now(LOCAL_TZ) - last_updated) > datetime.timedelta(seconds=self.session_length):
                del pids[pid]
        return pids

    def dereference(self, callback=None, args=None, kwargs=None):
        """
        This method should only be called while the reference is locked.

        Decrements the reference count for the resource. If this process holds the only reference at the time we finish dereferencing it; True is returned. Operating on the resource after it has been dereferenced is undefined behavior.

        Dereference queries the value stored in the backend, if any, iff (if and only if) this instance is the last reference
            to that resource. e.g. self.count() == 0

        :param function callback: A function to execute iff it's determined this process holds the only reference to the resource. When there is a failure communicating with the backend in the cleanup step the callback function will be called an additional time for that failure and each subsequent one thereafter. Ensure your callback handles this properly.
        :param tuple args: Positional arguments to pass your callback.
        :param dict kwargs: keyword arguments to pass your callback.

        :returns: Whether or not there are no more references among all processes. True if this was the last reference. False otherwise.
        :rtype: bool
        """
        pids = {}
        if args is None:
            args = tuple()
        if kwargs is None:
            kwargs = {}
        
        client = Reference.client
        reflist = client.get(self.reflist_key)

        if reflist is not None:
            pids = json.loads(reflist)
            if self.pid in pids:  # It won't be here if a dereference previously failed at the delete step.
                del pids[self.pid]

        # Check for failed processes
        pids = self.remove_failed_processes(pids)
        rc = True
        if pids:
            rc = False 

        try:
            val = json.dumps(pids)
            client.set(self.reflist_key, val)
            if callback is not None and rc:
                callback(*args, **kwargs)
        finally:
            if rc:
                client.delete(self.resource_key, self.reflist_key, self.times_modified_key)

        return rc 

