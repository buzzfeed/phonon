from phonon import TTL, PhononError


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
            self.__process.locks[self.lock_key] = self.__lock
            return self.__lock
        else:
            raise AlreadyLocked(
                "Could not acquire a lock. Possible deadlock for key: {0}".format(self.lock_key))

    def __exit__(self, type, value, traceback):
        del self.__process.locks[self.lock_key]
        self.__lock.__exit__(type, value, traceback)


class AlreadyLocked(PhononError):
    pass
