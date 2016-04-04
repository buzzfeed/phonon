import unittest
import logging
import redis

from phonon.process import Process
from phonon.locking import Lock, AlreadyLocked

logging.disable(logging.CRITICAL)

class ProcessTest(unittest.TestCase):

    def test_lock_wrapper(self):
        p1 = Process()
        lock = Lock(p1, "foo", True)
        lock2 = Lock(p1, "foo", False)

        assert lock.block is True
        assert lock2.block is False
        assert lock.lock_key == "foo.lock"
        assert lock.client is p1.client
        assert lock._Lock__process is p1
        assert lock._Lock__lock is None

        with lock as acquired_lock:
            assert lock._Lock__lock is acquired_lock
            assert(isinstance(acquired_lock, redis.lock.LuaLock))

            self.assertRaises(AlreadyLocked, lock2.__enter__)

        p1.stop()