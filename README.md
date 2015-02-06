# Installation

You can install this package pretty easily with setup.py

```
python setup.py install
```

Or you can use git+ssh:

```
pip install git+ssh://git@github.com/buzzfeed/phonon.git 
```

But you can also use pip if you clone...

```
git clone git@github.com:buzzfeed/phonon.git ./phonon; cd phonon; pip install .
```

# Run the tests

This package uses the standard `setup.py` approach:

```
python setup.py test
```

# Getting Started

## References

The building blocks for this approach to concurrency is the `Reference` object. You can use `Reference` s for 

* locking on resources for exclusive reads or writes
* finding out how many processes are using a resource at a given time
* keeping track of how many processes have modified that resource
* executing a callback when a process is the last to finish using a resource

Here's an example:

```python
from phonon.reference import Reference
from phonon.process import Process

p1 = Process()
address_lookup_service = p1.create_reference(resource='me')
p2 = Process()
email_verification_service = p2.create_reference(resource='me')

def lookup_email_and_apply_to_record(record, reference):
    email = get_email(record)
    try:
        with email_verification_service.lock():
            update_record_with_email(record, email)
            if email_verification_service.count() == 1:
                write_record_to_database(record)
    except Process.AlreadyLocked, e:
        # Unable to acquire lock. Handle as needed.  
        pass

def verify_address_and_apply_to_record(record, reference):
    address = get_address(record)
    try:
        with address_lookup_service.lock():
            update_record_with_address(record, address)
            if address_lookup_service.count() == 1:
                write_record_to_database(record)
    except Process.AlreadyLocked, e:
        # Unable to acquire lock. Handle as needed.
        pass

t1 = threading.Thread(target=lookup_email_and_apply_to_record,
    args=('me', email_verification_service))
t2 = threading.Thread(target=verify_address_and_apply_to_record,
    args=('me', address_lookup_service))
t1.start()
t2.start()
t1.join()
t2.join()

p1.stop()
p2.stop()
```

Whoever is last to update the record in the cache will know since `count()` will return `1`. At that point we'll know the record is finished being updated, and is ready to be written to the database. 

## Updates

You can see the above example is pretty redundant in this case. It's much more useful to make use of a passive design, subscribing to incoming messages, and using sessions to decide when to write. That is what the `Update` class is intended to do. I'll just write a for loop to simulate incoming messages.

```python
from phonon.update import Update
from phonon.cache import LruCache

class UserUpdate(Update):
    def state(self):
        # Return a dict of only the attributes you set yourself. Didn't set any? Great. Don't override it. 
    def reset_states(self):
        # Return a dict of how to reset any fields you added and will execute on. Didn't set any or not using process recovery? Great. Don't override it. 
    def merge(self, other):
        # Merge an instance of this class with another, combining state.
    def execute(self):
        # Write this object to the database. don't worry about when to cache vs. execute, it's handled.

# Calls end_session upon removing from cache.
# Also finds collisions and calls merge instead of overwriting on set
lru_cache = LruCache(max_entries=10000) 

p = Process()
for user_update in user_updates:
    lru_cache.set(user_update.user_id, UserUpdate(process=p, doc=**user_update))

lru_cache.expire_all()
p.stop()
```
