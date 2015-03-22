Router
======

How it works
------------

The router handles assigning a shard to a particular key value. It checks the shard for node flags, and given the state of each node determines whether or not a read/write operation should be performed on that node.

Module Reference
----------------
.. automodule:: phonon.router
    :members:
