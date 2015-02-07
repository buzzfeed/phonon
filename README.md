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
    def clear(self):
        # Clear all the attributes of this structure that allow it to be merged appropriately. Didn't set any yourself? Great. Don't override it. 
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

This probably leaves you wondering how to create appropriate `state`, `clear`, `merge`, and `execute` methods. Below we'll give a few practical examples with explanation of how to create an appropriate subclass.

Let's say we want to aggregate clicks and impressions on on-site elements called `Widget`. Let's define `Widget` as follows:

```python
from django.db import models 

class Widget(models.Model):
    parent_post_id = models.IntegerField()
    target_post_id = models.IntegerField()
    impressions = models.IntegerField()
    clicks = models.IntegerField()
```

Our updates will come in one after the other from our on-site tracking code. We'll receive JSON documents already converted to Python `dict` objects. Let's assume they take the form

```python
{
    'parent_post_id': <int>,
    'target_post_id': <int>,
    'event_type': <str["clicks"|"impressions"]>
}
```

This data represents the bulk of what we wish to aggregate. Let's create an `Update` subclass called `WidgetUpdate` to work with.

```python
from phonon.update import Update

class WidgetUpdate(Update):
    pass
```

How should we instantiate it, then? The `Update` class was originally designed to work with MongoDB, but it can easily be adapted to any other type of database backend. Here we're using Django. Let's look at the adopted conventions and see how they translate to our new backend.

The `Update` initializer takes the following arguments used primarily for bookkeeping and to save you a little typing:

* **_id** --- *_id* In Django terms is simply the primary key *or* the unique key used to look up the object in your database. 
* **database** --- *database* is just the name of your database in Django. You can probably just leave this field empty unless you need to actually refer to your database in your other methods. 
* **collection** --- *collection* in Django-speak is analogous to your table name or your model's class name. Whichever you prefer. Again, you can leave this blank unless you need to refer to it later. A useful technique is to set the model definition as the collection.
* **spec** --- the *spec* is the query used to find objects in your database to update. In Django terms it's the `dict` you might pass in as the argument to `filter` with a `**` to turn it into keyword arguments.
* **doc** --- finally, the *doc* indicates the actual update to make. It's generally recommended your doc specify a dictionary that can be passed directly to your ORM's update method. For Django 1.7+ we strongly suggest providing a `dict` of your model attributes with the amount each should be incremented. If you're setting a value you will have to include the appropriate logic in your `merge` method. We'll see in a minute how this can be very flexible.

To instantiate our `WidgetUpdate` we'll take our JSON document converted to a `dict` and modify it slightly so it can be used simply in an `execute` method. Below we'll define the JSON document for clarity, but the typical use case is to consume this from some PUB/SUB interface.

```python
# Here's the update doc...
some_update = {
    'parent_post_id': 12345,
    'target_post_id': 67890,
    'event_type': "impressions" 
}

# Less typing to set variables...
pp_id = some_update.get('parent_post_id')
tp_id = some_update.get('target_post_id')
evt = some_update.get('event_type')

# Now we can actually create a widget update with appropriate vars
myupdate = WidgetUpdate(_id="{0}.{1}".format(pp_id, tp_id), 
    spec={'parent_post_id': pp_id, 'target_post_id': tp_id},
    doc={evt: 1}) 
```

### `state()`

We don't actually need to create a `state()` method for this object since we didn't set any attributes on the instance besides those passed to the initializer. Keeping track of state, then, is totally handled for us.

If we set some other attribute, though, we would need to. For example

```python
myupdate.foo = 1 
```

Would require a corresponding `state` function:

```python
class MyUpdate(Update):
    def state(self):
        return {'foo': self.foo}
```

where we've omitted the rest of the `MyUpdate` class definition for clarity.

### `clear()`

In our example we also don't need a clear function since, again, we didn't set any attributes besides those defined. In the `MyUpdate` example above, however, clear would take the form

```python
class MyUpdate(Update):
    def clear(self):
        self.foo = 0 
```
It's simply what's required to re-initialize the `Update` such that it's execution will result in no modification of any database record (while it can still look up the correct record based on the spec).

### `merge()`

This method will always need to be defined. It's simply what's required to add two `Update` objects together. In our primary example this would look like

```python
class WidgetUpdate(Update):

    def merge(self, other):
        for k, v in other.doc.items():
            if k in self.doc:
                self.doc += v
            else:
                self.doc[k] = v
```
You can rely on `other` being passed automatically, and it will always have the same type as `self`.

It may seem a bit annoying to modify the attributes on `self.doc` in such a way as to always preserve their ability to be immediately written to the database, but users are encouraged not to make use of "bookkeeping attributes". These tend to require bookkeeping of their own and quickly grow confusing.  

In our secondary example we would handle merging `foo` in the `merge` method as well. Since the types are the same it can be handled like

```python
class MyUpdate(Update):

    def merge(self, other):
        for k, v in other.doc.items():
            if k in self.doc:
                self.doc += v
            else:
                self.doc[k] = v
        self.foo += other.foo
```

### `execute()`

Finally the `execute` method is just what's required to write this record to the database backend. In our Django example this is very simple. Putting it all together we have...

```python
from django.db.models import F
from django.db import DatabaseError

class WidgetUpdate(Update):

    def merge(self, other):
        for k, v in other.doc.items():
            if k in self.doc:
                self.doc += v
            else:
                self.doc[k] = v

    def execute(self):
        try:
            widget = Widget.objects.get_or_create(**self.spec)
            for k, v in self.doc.items():
                setattr(widget, k, F(k) + v)
            widget.save()
        except DatabaseError, e:
            logger.error("Error updating widget: {0}".format(e))

```

There are many benefits to this framework. Primarily locking is handled, allowing merges of data structures that are not conflict free. When multiple updates are returned at the same time it's possible to employ logic resolving timeliness by adding that attrbute to the update object.

When data structures are conflict free, we still get great benefits to coordination by aggregating in the cache before writing out to the database in the event multiple updates are created for a single spec.

Added benefit comes about when multiple update types are used with similar interfaces since `merge` and `execute` patterns can often be re-used.

```python
class BaseUpdate(Update):
    
    def merge(self, other):
        for k, v in other.doc.items():
            if k in self.doc:
                self.doc += v
            else:
                self.doc[k] = v

    def execute(self):
        try:
            o = self.Model.objects.get_or_create(**self.spec)
            for k, v in self.doc.items():
                setattr(o, k, F(k) + v)
            o.save()
        except DatabaseError, e:
            logger.error("Error updating {0}: {1}".format(o.__class__.__name__, e))

class WidgetUpdate(BaseUpdate):
    Model = Widget

class UserUpdate(BaseUpdate):
    Model = User

class FooUpdate(BaseUpdate):
    Model = Foo

```
