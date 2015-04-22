import pickle
import datetime
from collections import namedtuple

from phonon import PHONON_NAMESPACE, LOCAL_TZ, TTL


class BaseUpdate(object):
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

    def __init__(self, process, _id, database='test', collection='test',
                 spec=None, doc=None, init_cache=False, block=True, hard_session=TTL,
                 soft_session=.5*TTL):
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
        :param int hard_session: The maximum number of minutes an Update should
            live before being forced to execute. Generally speaking, this value
            should be larger than then soft_session.
        :param int soft_session: The maximum number of minutes an Update should
            live before being forced to execute.  However, unlike the hard_session,
            this value can is refreshed to extend the life of the session.
        """
        self.resource_id = '{0}_Update.{1}.{2}'.format(PHONON_NAMESPACE, collection, _id)

        self.spec = spec
        self.doc = doc if doc != None else {}
        self.collection = collection
        self.database = database
        self.block = block
        self.__process = process
        self.ref = self.__process.create_reference(resource=self.resource_id, block=block)
        self.init_cache = init_cache
        self.hard_session = hard_session
        self.soft_session = soft_session
        self.soft_expiration = datetime.datetime.now(LOCAL_TZ) + datetime.timedelta(seconds=self.soft_session)
        self.hard_expiration = datetime.datetime.now(LOCAL_TZ) + datetime.timedelta(seconds=self.hard_session)

    def process(self):
        """ Get underlying process variable

        :rtype: :class:`phonon.process.Process` class
        :returns: Process variable
        """
        return self.__process

    def is_expired(self):
        """
        :rtype: bool
        :returns: Indicates whether the Update has passed its expiration
        """
        current_time = datetime.datetime.now(LOCAL_TZ)
        return (current_time > self.hard_expiration or
                current_time > self.soft_expiration)

    def force_expiry(self):
        """
        Expires current and cached references related to current Update object
        then ends the session.
        """
        self.ref.force_expiry = True
        self.end_session()

    def _end_session(self, execute, cache, block=True):
        """
        Indicate to this update its session has ended on the local machine.
        The implementation of your cache, merge, and execute methods will be
        used to write to redis or your database backend as efficiently as
        possible.

        :param function execute: execute functinon from child class
        :param function cache: cache functinon from child class
        :param bool block: whether to block or not when locking
        """
        if not self.ref.dereference(execute, block=block):
            if self.is_expired():
                # If this update has expired but other active references
                # to the resource still exist, we force this update to
                # execute. We reset the time_modified_key and cached data
                # to prevent any other processes from executing the same
                # data.
                execute()
                with self.ref.lock(block=block):
                    self.__process.client.delete(self.ref.resource_key)
                    self.__process.client.set(self.ref.times_modified_key, 0)
            else:
                cache()

    def __getstate__(self):
        default_state = {
            'resource_id': self.resource_id,
            'spec': self.spec,
            'doc': self.doc,
            'collection': self.collection,
            'database': self.database,
            'hard_expiration': self.hard_expiration
        }
        user_defined_state = self.state()

        default_state.update(user_defined_state)

        return default_state

    def __setstate__(self, state):
        for k, v in state.items():
            setattr(self, k, v)

    def _clear(self):
        """
        If using failure recovery features (ie init_cache), after caching, data
        that will be executed to the database will be removed from the local update.
        """
        self.doc = {}
        self.__setstate__(self.clear())

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

    def refresh(self, update):
        """
        Given an instance of another update, merge it into this update and this
        update's soft session length.

        :param dict update: Exactly what you wrote in your `cache` method, but
            already parsed from JSON into a python `dict`.
        """
        self.soft_expiration = datetime.datetime.now(LOCAL_TZ) + datetime.timedelta(seconds=self.soft_session)
        self.merge(update)

    def execute(self):
        """
        You must override this method. This method handles writing the update
        to your database backend. Records in redis will be cleaned up
        regardless of whether this method raises an exception. Error handling
        should occur internally since this method will have the complete record
        to be written.
        """
        raise NotImplementedError("You must define a execute method that writes this\
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
        raise NotImplementedError("You must define a merge method that merges it's\
            argument with this object.")

class Update(BaseUpdate):

    def __init__(self, *args, **kwargs):
        super(Update, self).__init__(*args, **kwargs)
        if self.init_cache:
            self.__cache()

    def end_session(self, block=True):
        return self._end_session(self.__execute, self.__cache, block)

    def cache(self):
        """
        This method caches the update to redis.
        """
        self.process().client.set(self.resource_id,
                                  pickle.dumps(self))

    def __merge(self):
        """
        Handles how to extract a resource's data from redis and calls the user
        defined `merge` method.
        """
        pickled = self.process().client.get(self.resource_id)
        if pickled:
            cached = pickle.loads(pickled)
            self.merge(cached)

    def __cache(self, block=None):
        """
        Handles deciding whether or not to get the resource from redis. Also
        implements merging cached records with this one (by implementing your
        `merge` method). Increments the number of times this record was
        modified if the cache method executes successfully (does not raise).
        """
        if block is None:
            block = self.block

        with self.ref.lock(block=block):
            if self.ref.get_times_modified() > 0:
                self.__merge()
            self.cache()
            self.ref.increment_times_modified()

        if self.init_cache:
            self._clear()

    def __execute(self, block=None):
        """
        Handles deciding whether or not to get the resource from redis. Also
        implements merging cached records with this one (by implementing your
        `merge` method). Increments the number of times this record was
        modified if the cache method executes successfully (does not raise).
        """
        if block is None:
            block = self.block

        with self.ref.lock(block=block):
            if self.ref.get_times_modified() > 0:
                self.__merge()
            self.execute()


class ConflictFreeUpdate(BaseUpdate):
    """ 
    Update class to be used when looking to minimize the locking of resources
    during caching and execution of updates.  Accordingly, it should only be
    used when you can ensure that you are only performing atomic operations on
    both Redis and your database.

    A default caching and merging method is provided which should be sufficient
    when simply incrementing or decrementing fields. Otherwise, they must be
    overwritten to suit your needs.
    """
    def __init__(self, *args, **kwargs):
        super(ConflictFreeUpdate, self).__init__(*args, **kwargs)
        if self.init_cache:
            self.__cache()

    def end_session(self, block=True):
        return self._end_session(self.__execute, self.__cache, block)

    def get_cached_data(self):
        """
        Gets the cached data for the resource from redis and reconstructs it
        from a flattened structure into its original form.
        """
        cached_data = self.process().client.hgetall(self.resource_id)
        reconstructed_data = {}
        for k, v in cached_data.items():
            split = k.split(".")
            field, subfield = split if len(split) == 2 else (split[0], None)
            if subfield:
                reconstructed_data.setdefault(field, {})[subfield] = v
            else:
                reconstructed_data[field] = v

        empty_attributes = [k for k in self.__get_update_data().keys() if k not in reconstructed_data]
        for k in empty_attributes:
            reconstructed_data[k] = self.clear().get(k) or {}

        return reconstructed_data

    def cache(self):
        """
        Caches update data (stored within the doc or attributes specific in the
        overwritten state function) to redis within a hash named for the
        resource_id. Any attributes storing values in a dictionary are keyed by
        the [attribute_name].[key_in_dictionary]. To be used in situation where 
        field values are being incremented or decremented.  Otherwise, overwrite 
        to suite your specific needs.
        """
        for field, data in (self.__get_update_data() or {}).items():
            if type(data) == dict:
                for k, v in data.items():
                    self.process().client.hincrby(self.resource_id, "{}.{}".format(field, k), int(v))
            else:
                self.process().client.hincrby(self.resource_id, field, int(data))

    def merge(self, other):
        """
        Handles how to extract a resource's data from redis and calls the user
        defined `merge` method. To be used in situation where  field values are
        being incremented or decremented.  Otherwise, overwrite to suite your 
        specific needs.
        """
        for field in self.__get_update_data().keys():
            self_field = getattr(self, field)
            other_field = getattr(other, field)
            if type(self_field) == dict:
                for k, v in (other_field or {}).items():
                    if k not in self_field:
                        self_field[k] = int(v)
                    else:
                        self_field[k] += int(v)
            elif self_field and other_field:
                setattr(self, field, int(self_field) + int(other_field))
            elif other_field:
                setattr(self, field, int(other_field))

    def __merge(self):
        """
        Handles how to extract a resource's data from redis and calls the user
        defined `merge` method.
        """
        UpdateDoc = namedtuple("UpdateDoc", self.__get_update_data().keys())
        update_doc = UpdateDoc(**self.get_cached_data())
        self.merge(update_doc)

    def __cache(self, block=None):
        """
        Handles deciding whether or not to get the resource from redis. Also
        implements merging cached records with this one (by implementing your
        `merge` method). Increments the number of times this record was
        modified if the cache method executes successfully (does not raise).
        """
        self.cache()
        self._clear()

    def __execute(self, block=None):
        """
        Handles deciding whether or not to get the resource from redis. Also
        implements merging cached records with this one (by implementing your
        `merge` method). Increments the number of times this record was
        modified if the cache method executes successfully (does not raise).
        """
        self.__merge()
        self.execute()

    def __get_update_data(self):
        """
        :rtype: dict
        :returns: A dictionary of all data that is cached, combining any user
            specified attributes with 'doc'
        """
        state_data = self.state().copy()
        state_data.update({'doc': self.doc})
        return state_data