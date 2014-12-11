# Installation

You can install this package pretty easily with setup.py

```
python setup.py install
```

Or you can use git+ssh:

```
pip install git+ssh://git@github.com/buzzfeed/disref.git 
```

But you can also use pip if you clone...

```
git clone git@github.com:buzzfeed/disref.git ./disref; cd disref; pip install .
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
from disref import Reference

address_lookup_service = Reference(pid=1, resource='me')
email_verification_service = Reference(pid=2, resource='me')

def lookup_email_and_apply_to_record(record, reference):
    email = get_email(record)
    try:
        reference.lock()
        update_record_with_email(record, email)
        if reference.count() == 1:
            write_record_to_database(record)
        reference.release()
    except disref.AlreadyLocked, e:
        raise Exception("Timed out acquiring lock!")

def verify_address_and_apply_to_record(record, reference):
    address = get_address(record)
    try:
        reference.lock()
        update_record_with_address(record, address)
        if reference.count() == 1:
            write_record_to_database(record)
        reference.release()
    except disref.AlreadyLocked, e:
        raise Exception("Timed out acquiring lock!")

t1 = threading.Thread(target=lookup_email_and_apply_to_record,
    args=('me', email_verification_service))
t2 = threading.Thread(target=verify_address_and_apply_to_record,
    args=('me', address_lookup_service))
t1.start()
t2.start()
t1.join()
t2.join()
```

Whoever is last to update the record in the cache will know since `count()` will return `1`. At that point we'll know the record is finished being updated, and is ready to be written to the database. 
