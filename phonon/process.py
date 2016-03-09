import redis
import uuid
import time
import threading
import math

from phonon import get_logger, PhononError, PHONON_NAMESPACE, TTL
from phonon.reference import Reference
from phonon.client import ShardedClient

logger = get_logger(__name__)


def get_ms():
    return int(time.time() * 1000.)


def s_to_ms(s):
    return int(s * 1000.)


class Process(object):
    """
    Represents a process on which a particular resource lives, identified by a
    unique id automatically assigned to it.  It establishes a connection to
    Redis, shared by all instances. All References should be added through a
    process instance.

    When finished with the process instance, call the stop function.

    """

    RETRY_SLEEP = 0.5    # Second
    BLOCKING_TIMEOUT = 500

    class Lock(object):

        def __init__(self, process, lock_key, block=True):
            """
            :param Process process: The Process which is issuing this lock
            :param str lock_key: The key at which to acquire a lock
            :param bool block: Optional. Whether or not to block when
                establishing the lock.
            """
            self.block = block
            lock_key = "{0}.lock".format(lock_key)
            self.lock_key = lock_key
            self.client = process.client
            self.__process = process
            self.__lock = None

        def __enter__(self):
            blocking_timeout = self.__process.BLOCKING_TIMEOUT if self.block else 0
            self.__lock = self.client.lock(self.lock_key, timeout=TTL,
                                           sleep=self.__process.RETRY_SLEEP, blocking_timeout=blocking_timeout)

            self.__lock.__enter__()
            if self.__lock.local.token:
                return self.__lock
            else:
                raise Process.AlreadyLocked(
                    "Could not acquire a lock. Possible deadlock for key: {0}".format(self.lock_key))

        def __exit__(self, type, value, traceback):
            self.__lock.__exit__(type, value, traceback)

    class AlreadyLocked(PhononError):
        pass

    def __init__(self, process_ttl=int(0.5 * TTL), host=None, hosts=None, port=6379, db=1, heartbeat_interval=10, recover_failed_processes=True):
        """
        :param process_ttl int: The time after which we consider a node to be
            unresponsive. This should be at most 1/2 the length of the TTL
            value.
        :param str host: The host to connect to redis over.
        :param str hosts: The hosts to connect in the sharded case.
        :param int port: The port to connect to redis on.
        :param int heartbeat_interval: The frequency in seconds with which to
            update the heartbeat for this process.
        :param bool recover_failed_processes: Determines whether this process
            attempt to recover references from other failed processes.

        """
        if host:
            logger.warning("`host` is deprecated and should no longer be used in favor of `hosts`")
            hosts = [host]
        if hosts is None:
            hosts = ['localhost']

        self.id = str(uuid.uuid4())
        self.process_ttl = process_ttl
        self.recover_failed_processes = recover_failed_processes
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_hash_name = "{0}_heartbeat".format(PHONON_NAMESPACE)
        self.__heartbeat_timer = None
        self.__heartbeat_ref = None

        if not hasattr(Process, 'client'):
            Process.client = ShardedClient(hosts=hosts, port=port, db=db)
        else:
            logger.warning("A connection already exists. You connection parameters are being ignored.")

        self.client = Process.client
        self.client.ping()

        self.registry_key = self.__get_registry_key(self.id)

        self.__heartbeat_ref = self.create_reference(self.heartbeat_hash_name)
        self.__update_heartbeat()

    def create_reference(self, resource, block=True):
        """
        Creates a Reference object owned by this process. This function is not
        thread-safe. 

        :param bool block: Optional. Whether or not to block when establishing
            locks.
        :param str resource: An identifier for the resource. For example:
            Buzz.12345

        :returns: The created Reference object
        """

        self.add_to_registry(resource)
        return Reference(self, resource, block)

    def add_to_registry(self, resource, registry_key=None):
        """
        Adds a particular resource key to a process registry. This function is
        not thread-safe.

        :param str resource: An identifier for the resource.
        :param str registry_key: Optional. The registry to which the
            resource should be added.  If not specific, the resource
            will be added to this process.

        """
        if registry_key is None:
            registry_key = self.registry_key

        return self.client.hset(self.registry_key, resource, 1)

    def remove_from_registry(self, resource, registry_key=None):
        """
        Removes a particular resource key from a process registry. This
        function is not thread-safe.

        :param str resource: An identifier for the resource.
        :param str registry_key: Optional. The registry from which the
            resource should be removed.  If not specific, the resource
            will be removed from this process.

        """
        if registry_key is None:
            registry_key = self.registry_key

        return self.client.hdel(self.registry_key, resource)

    def get_registry(self, registry_key=None):
        """
        Returns a list of all items in a process' registry, excluding any
        resources automatically created by each process.  As these are added,
        they should be added to the REGISTRY_EXCLUSIONS below.

        :param str registry_key: Optional. The registry for which to return
            the registry.  If not specified, returns this process's registry.

        """

        if registry_key is None:
            registry_key = self.registry_key

        registry_keys = self.client.hkeys(registry_key)

        return [s for s in registry_keys if s != self.heartbeat_hash_name]

    def lock(self, lock_key, block=True):
        """
        Issues a lock for a given key.
        Usage:
            with process.lock( some_key ):
                pass
        :param str lock_key: The key to lock
        :param bool block: Optional. Whether or not to block when establishing
            lock.
        """
        return Process.Lock(self, lock_key, block)

    def __get_registry_key(self, pid):
        """
        :param str pid: The id of a particular process

        :returns: The key at which that process' registry is stored in redis

        """
        return "{0}_{1}".format(PHONON_NAMESPACE, pid)

    def __update_heartbeat(self):
        """
        Records the timestamp at a configurable interval to ensure the process
        is still alive.
        """
        if self.__heartbeat_timer:
            self.__heartbeat_timer.cancel()
            self.__heartbeat_timer = None

        with self.lock(self.__heartbeat_ref.resource_key):
            self.client.hset(self.heartbeat_hash_name, self.id, get_ms())

        if self.recover_failed_processes:
            self.__recover_failed_processes()

        self.__heartbeat_timer = threading.Timer(self.heartbeat_interval, self.__update_heartbeat)
        self.__heartbeat_timer.daemon = True
        self.__heartbeat_timer.start()

    def __recover_failed_processes(self):
        """
        Checks the health of all processes by checking the last time each
        heartbeat was updated, and recovers the references for any process
        which has died.  This function is not thread-safe.

        """
        failed_pids = []
        heartbeats = self.client.hgetall(self.heartbeat_hash_name)
        for pid, heartbeat_time in heartbeats.items():
            if int(heartbeat_time) <= get_ms() - s_to_ms(5 * self.heartbeat_interval):
                failed_pids.append(pid)

        active_process_count = len(heartbeats) - len(failed_pids)

        for failed_pid in failed_pids:
            failed_process_registry_key = self.__get_registry_key(failed_pid)

            try:
                with self.lock(failed_process_registry_key):
                    if failed_pid == self.id:
                        # The failed process has come back to life.  Its old registry remains
                        # intact in redis under the old process_id and will be
                        # recovered by other processes.  By assigning a new id this process's
                        # registry begins fresh.
                        self.id = unicode(uuid.uuid4())
                        self.registry_key = self.__get_registry_key(self.id)
                    elif active_process_count:

                        failed_process_registry = self.client.hkeys(failed_process_registry_key)
                        recovering_references = failed_process_registry[0:int(math.ceil(float(len(failed_process_registry)) / active_process_count))]

                        for recovering_reference in recovering_references:
                            reference = self.create_reference(recovering_reference)
                            with reference.lock():
                                reference.nodelist.remove_node(failed_pid)

                        if self.remove_from_registry(recovering_references, failed_process_registry_key) == 0:
                            # No futher references to recover.
                            self.client.hdel(self.heartbeat_hash_name, failed_pid)
                    else:
                        logger.error("There is no active process with which to \
                            recover your references.")

            except Process.AlreadyLocked:
                logger.warning("Registry already locked. Remaining references \
                    will be recovered on next available heartbeat update.")

    def stop(self):
        """
        Preforms cleanup for the Process instance when it is to be terminated.
        """
        if self.__heartbeat_timer:
            self.__heartbeat_timer.cancel()

        if self.__heartbeat_ref:
            self.__heartbeat_ref.dereference()

    def __del__(self):
        self.stop()
