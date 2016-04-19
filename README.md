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

You should probably run that in a virtualenv. People should use virtualenvs.

# Getting Started

This latest version of `phonon` encourages a lock-free, asynchronous approach to aggregation through your redis cache. 
With this in mind; we _support_ but do not _encourage_ locking. With that said... 

## References

The building blocks for this approach to concurrency is the `Reference` object. You can use `Reference` s for 

* locking on resources for exclusive reads or writes
* finding out how many processes are using a resource at a given time
* keeping track of how many processes have modified that resource
* executing a callback when a process is the last to finish using a resource

### An example

Let's say we have a process that monitors events on a stream in NSQ, a popular message bus. Sometimes these can be VERY high volume!

If we want to aggregate locally, before writing to a cache, and ultimately a database; `phonon` makes that process easy.

```python
import nsq

import phonon.registry
import phonon.connections
import phonon.model
import phonon.field

class Session(phonon.model.Model):
    id = phonon.field.ID()
    impressions = phonon.field.SumField()
    clicks = phonon.field.SumField()
    
    def on_complete(self, msg):
        # Write the model to the database. You're guaranteed to have the global aggregate now.
        msg.finish()
    
def handle_message(msg):
    msg.enable_async()
    body = json.loads(msg.body)
    phonon.registry.register(Session(
        id=body['user_id'],
        impressions=int(body['type'] == 'unit_impression'),
        clicks=int(body['type'] == 'unit_click', msg)

if __name__ == '__main__':
    phonon.connections.connect(hosts=['redis01.example.com', 'redis02.example.com'])
    nsq.Reader(
        topic=CONVERSION_EVENTS_TOPIC,
        channel=QR_CLICKS_AND_IMPRESSIONS_AGGREGATOR,
        message_handler=handle_message,
        lookupd_http_addresses=['nsq01.example.com', 'nsq02.example.com'],
        max_in_flight=10
    )
    nsq.run()
    
```

By declaring a session with the `SumField` fields populated with the quantity represented by the individual message we can ensure they're aggregated in the cache in a way that is lock free and conflict free.

Be sure to check out `phonon.field.Fields` for more types you can aggregate!
