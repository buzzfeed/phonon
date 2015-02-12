.. phonon documentation master file, created by
   sphinx-quickstart on Tue Feb 10 21:23:13 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to phonon's documentation!
==================================

The main goals of the Phonon project are
- Provide a high-level abstraction for easy coordination in Python
- Fault tolerance for network partitions, single, and multi- node failures
- Failover for unreachable hosts
- High availability and linear scaling

The overall goal is simplicity on both the administration side and the application development side of things. To deploy Phonon all an administrator needs is a set of workers running a python application and a set of individual Redis master nodes.

You should check out the README for getting started (located on the github page, http://www.github.com/buzzfeed/phonon)

It has some examples. You can find low-level documentation here.

Contents:

.. toctree::
   :maxdepth: 2

   phonon
   reference
   cache 
   exceptions
   process
   update
   router

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

