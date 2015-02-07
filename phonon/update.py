import json
import pickle

from phonon import PHONON_NAMESPACE
from phonon.reference import Reference


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

    def __init__(self, process, _id, database='test', collection='test', spec=None, doc=None, init_cache=False, block=True):
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
        self.resource_id = '{0}_Update.{1}.{2}'.format(PHONON_NAMESPACE, collection, _id)

        self.spec = spec
        self.doc = doc
        self.collection = collection
        self.database = database
        self.__process = process
        self.ref = self.__process.create_reference(resource=self.resource_id, block=block)
        self.init_cache = init_cache
        if self.init_cache:
            self.__cache()

    def process(self):
        """ Get underlying process variable

        :rtype: :class:`phonon.process.Process` class
        :returns: Process variable
        """
        return self.__process

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

    def __getstate__(self):
        default_state = {
            'resource_id': self.resource_id,
            'spec': self.spec,
            'doc': self.doc,
            'collection': self.collection,
            'database': self.database
        }
        user_defined_state = self.state()

        default_state.update(user_defined_state)

        return default_state

    def __setstate__(self, state):
        for k, v in state.items():
            setattr(self, k, v)

    def __cache(self):
        """
        Handles deciding whether or not to get the resource from redis. Also
        implements merging cached records with this one (by implementing your
        `merge` method). Increments the number of times this record was
        modified if the cache method executes successfully (does not raise).
        """
        if self.ref.get_times_modified() > 0:
            pickled = self.__process.client.get(self.resource_id)
            if pickled:
                cached = pickle.loads(pickled)
                self.merge(cached)
        self.cache()
        self.ref.increment_times_modified()

        if self.init_cache:
            self.__clear()

    def __execute(self):
        """
        Handles deciding whether or not to get the resource from redis. Also
        implements merging cached records with this one (by implementing your
        `merge` method). Calls your `execute` method to write the record to the
        database. Recovering from a failed `execute` is up to you.
        """
        if self.ref.get_times_modified() > 0:
            pickled = self.__process.client.get(self.resource_id)
            if pickled:
                cached = pickle.loads(pickled)
                self.merge(cached)
        self.execute() 

    def __clear(self):
        """
        If using failure recovery features (ie init_cache), after caching, data
        that will be executed to the database will be removed from the local update.
        """
        self.doc = {}
        self.__setstate__(self.clear())


    def cache(self):
        """
        This method caches the update to redis.
        """
        self.__process.client.set(self.resource_id,
                                  pickle.dumps(self))

    def state(self):
        """
        Return a dictionary of any attributes you manually set on the update. If
        you don't need it, don't override it.
        """
        return {}

    def clear(self):
        """
        If using failure recovery features, after caching, any data which will
        be executed to the database should be reset to an 'empty' state.
        Return a dictionary of any attributes you set on the update along with
        its base state.  If you aren't using any other attributes other than doc
        to execute or not using the failure functionality, don't override this.
        """
        return {}

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


