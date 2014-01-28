.. asyncio_redis documentation master file, created by
   sphinx-quickstart on Thu Oct 31 08:50:13 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

asyncio_redis
=============

Redis client for PEP 3156. (Tulip)

.. _GitHub: https://github.com/jonathanslenders/asyncio-redis

Features
--------

- Works for the asyncio (PEP3156) event loop
- No dependencies
- Connection pooling and pipelining
- Automatic conversion from native Python types (unicode or bytes) to Redis types (bytes).
- Blocking calls and transactions supported
- Pubsub support
- Streaming of multi bulk replies
- Completely tested

Installation
------------

::

    pip install asyncio_redis


Start by taking a look at :ref:`some examples<redis-examples>`.


Author and License
------------------

The ``asyncio_redis`` package is written by Jonathan Slenders.  It's BSD
licensed and freely available. Feel free to improve this package and
`send a pull request`_.

.. _send a pull request: https://github.com/jonathanslenders/asyncio-redis


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. toctree::
   :maxdepth: 2

   pages/examples
   pages/reference
