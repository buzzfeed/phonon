# Installation

Simplest way to get the project is through the Python Package Index

```
pip install phonon
```

You can install this package pretty easily with setup.py

```
python setup.py install
```

Or you can use git+ssh to get the bleeding edge:

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

