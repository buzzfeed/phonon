import json

from disref import DISREF_NAMESPACE
from disref.reference import Reference


class Update(object):
    """
    It's common for a database backend to be a bottleneck when data is
    aggregated for access through an API. This method is intended to be used in
    the implementation of an efficient, distributed write-through cache.
    
    Let's say we are collecting impression events with NSQ, and our application
    implementation is on the consuming side of a PUB/SUB interface. The goal is
    to aggregate impressions per-user such that a user can be queried by ID,
    and all the pages they viewed can be accessed through the API.

    When process A receives the first update it will create a UserUpdate
    instance (subclassing this class). The session is set, on that node, to
    expire in 10 minutes. 2 minutes later process B receives an update for the
    same user. Process B creates a UserUpdate instance with the same
    resource_id as process A, and adds itself to the reflist in redis (along
    side process A).

    When another 8 minutes pass; process A dereferences. Since the reference
    count is > 1 and times_modified is 0; the `cache` method of this class is
    called, and the times_modified for the update is incremented. When process
    B dereferences; it notices the times_modified is > 0 and the reference
    count is 1. This class will pull what is cached in redis and use the
    `merge` method you define to combine that cached record with this instance.
    After that; instead of caching it will run the `execute` method of this
    class. After the `execute` method finishes; the resource will be removed
    from redis.
    """

    def __init__(self, process, _id, database='test', collection='test', spec=None, doc=None, init_cache=True, block=True):
        """
        :param Process process: The process object, unique to the node.
        :param str _id: The primary key for the record in the database.
        :param str database: The name of the database.
        :param dict spec: A specification to use in looking up records to
            update.
        :param dict doc: A dictionary of representing the data to update.
        :param bool init_cache: Optional. Determines whether the update should
            cache immediately.  While this will allow for more complete recovery
            of data in the event of a node failure, it may reduce performance.
        :param bool block: Optional. Whether or not to block when establishing
            locks.
        """
        self.resource_id = '{0}_Update.{1}.{2}'.format(DISREF_NAMESPACE, collection, _id)

        self.spec = spec
        self.doc = doc
        self.collection = collection
        self.database = database
        self.__process = process
        self.ref = self.__process.create_reference(resource=self.resource_id, block=block)

        if init_cache:
            self.__cache()

    def end_session(self, block=True):
        """
        Indicate to this update it's session has ended on the local machine.
        The implementation of your cache, merge, and execute methods will be
        used to write to redis or your database backend as efficiently as
        possible.
        """
        with self.ref.lock(block=block):
            if not self.ref.dereference(self.__execute):
                self.__cache()

    def __cache(self):
        """
        Handles deciding whether or not to get the resource from redis. Also
        implements merging cached records with this one (by implementing your
        `merge` method). Increments the number of times this record was
        modified if the cache method executes successfully (does not raise).
        """
        if self.ref.get_times_modified() > 0:
            cached = json.loads(self.__process.client.get(self.resource_id) or "{}")
            self.merge(cached)
        self.cache()
        self.ref.increment_times_modified()

    def __execute(self):
        """
        Handles deciding whether or not to get the resource from redis. Also
        implements merging cached records with this one (by implementing your
        `merge` method). Calls your `execute` method to write the record to the
        database. Recovering from a failed `execute` is up to you.
        """
        if self.ref.get_times_modified() > 0:
            cached = json.loads(self.__process.client.get(self.resource_id) or "{}")
            self.merge(cached)
        self.execute() 

    def cache(self):
        """
        You must override this method. This method handles writing the update
        to redis. The format should be stringified JSON, and some variant of
        `set` should be used.
        """
        raise NotImplemented("You must define a cache method that caches this \
            record to redis at the key {0}. Locking and such will be handled\
            for you.") 
        
    def execute(self):
        """
        You must override this method. This method handles writing the update
        to your database backend. Records in redis will be cleaned up
        regardless of whether this method raises an exception. Error handling
        should occur internally since this method will have the complete record
        to be written.
        """
        raise NotImplemented("You must define a execute method that writes this\
            record to the database. Locking and such will be handled for you")

    def merge(self, update):
        """
        You must override this method. This method should take another `Update`
        subclass as a `dict` and handle merging the records represented there
        into this record. This is for merging cached records pulled from redis
        before either re-caching the updated record or writing the aggregate
        record to the database.

        :param dict update: Exactly what you wrote in your `cache` method, but
            already parsed from JSON into a python `dict`.
        """
        raise NotImplemented("You must define a merge method that merges it's\
            argument with this object.")


