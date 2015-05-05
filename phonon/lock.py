from phonon.exceptions import AlreadyLocked
from phonon.client.config import TTL
from phonon.client import Client


class Lock(object):

    TIMEOUT = 500
    RETRY_SLEEP = 0.5    # Second

    def __init__(self, process, lock_key, block=True):
        """
        :param Process process: The Process which is issuing this lock
        :param str lock_key: The key at which to acquire a lock
        :param bool block: Optional. Whether or not to block when
            establishing the lock.
        """
        self.block = block
        self.lock_key = "{0}.lock".format(lock_key)
        self.__lock = None
        self.__client = Client()

    def __enter__(self):
        blocking_timeout = Lock.TIMEOUT if self.block else 0
        self.__lock = self.__client.lock(name=self.lock_key, timeout=TTL,
                                         sleep=Lock.RETRY_SLEEP, blocking_timeout=blocking_timeout)

        self.__lock.__enter__()
        if self.__lock.local.token:
            return self.__lock
        else:
            raise AlreadyLocked(
                "Could not acquire a lock. Possible deadlock for key: {0}".format(self.lock_key))

    def __exit__(self, type, value, traceback):
        self.__lock.__exit__(type, value, traceback)
