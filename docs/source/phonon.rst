Phonon
======

The base namespace, `phonon`, offers the tools the end-user should interact with day-to-day. Namely; configuration management ( :py:func:`phonon.configure` function, and the :py:class:`phonon.Client` class.

Configuration
-------------

For reliability we require at least 2 nodes in at least 2 separate regions. This means the minimum setup will be 4 nodes.

In your application settings, wherever they may be, just import the phonon namespace. Pass the configuration like:

.. code-block:: python

    import phonon

    phonon.configure({
        "us-east-1": ["redis01.example.com", "redis02.example.com"],
        "us-west-1": ["redis03.example.com", "redis04.example.com"]
    })

The basic syntax is:

.. code-block:: python

    phonon.configure({
        "<region label>": ["<hostname1>", "<hostname2>", ...],
    })


Specifying the region allows us to make decisions for better failover in the case of a regional or data center failure. Don't care about those? Then just specify your regions, etc, like this:

.. code-block:: python

    import phonon

    phonon.configure({
        "first_half": ["<all>", "<my>", "<hosts>", "<go>"],
        "second_half" ["<here>", "<regardless>", "<of>", "<region>"]
    })


.. automodule:: phonon
    :members:
