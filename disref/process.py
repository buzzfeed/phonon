import sherlock
import redis
import uuid

from disref import get_logger
from disref.reference import Reference


logger = get_logger(__name__)


class Process(object):
    """
    Represents a process on which a particular resource lives, identified by a
    unique id automatically assigned to it.  It establishes a connection to
    Redis, shared by all instances. All References should be added through a
    process instance.

    """

    TTL = 30 * 60  # 30 minutes
    RETRY_SLEEP = 0.5    # Second
    TIMEOUT = 500

    def __init__(self, session_length=int(0.5*TTL), host='localhost', port=6379, db=1):
        """
        :param session_length int: The session length for the resource. e.g. If
            this represents an update for a User, the session_length would be
            the session length for that user. This should be at most 1/2 the
            length of the TTL for the Reference.
        :param str host: The host to connect to redis over.
        :param int port: The port to connect to redis on.

        """
        self.id = unicode(uuid.uuid4())
        self.session_length = session_length

        if not hasattr(Process, 'client'):
            Process.client = redis.StrictRedis(host=host, port=port, db=db)
            sherlock.configure(backend=sherlock.backends.REDIS,
                               expire=self.TTL,
                               retry_interval=self.RETRY_SLEEP,
                               timeout=self.TIMEOUT)
        else:
            connection_kwargs = Process.client.connection_pool.connection_kwargs
            if connection_kwargs['port'] != port or connection_kwargs['host'] != host or connection_kwargs['db'] != db:
                logger.warning("An existing Redis connection exists: host {0}, port {1}, db {2}.  Your connection paramters\
                                are being ignored."
                                .format(connection_kwargs['port'], connection_kwargs['host'], connection_kwargs['db']))

        self.client = Process.client


    def create_reference(self, resource, block=True):
        """
        :param bool block: Optional. Whether or not to block when establishing
            locks.
        :param str resource: An identifier for the resource. For example:
            Buzz.12345

        :returns: The created Reference object
        """
        return Reference(self, resource, block)
