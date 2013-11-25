Redis client for PEP3156 asyncio (tulip)
========================================

|Build Status|

Features
--------

- Works for the asyncio (PEP3156) event loop
- No dependencies
- Connection pooling
- Automatic conversion from unicode (Python) to bytes (inside Redis.)
- Completely tested
- Blocking calls and transactions supported
- Streaming of some multi bulk replies

Installation
------------

.. code::

    pip install asyncio_redis

Documentation
-------------

View documentation at `read-the-docs`_

.. _read-the-docs: http://asyncio-redis.readthedocs.org/en/latest/


Example using the Protocol class
--------------------------------

.. code:: python

    import asyncio
    from asyncio_redis import RedisProtocol

    @asyncio.coroutine
    def example():
        loop = asyncio.get_event_loop()

        # Create Redis connection
        transport, protocol = yield from loop.create_connection(
                    RedisProtocol, 'localhost', 6379)

        # Set a key
        yield from protocol.set('my_key', 'my_value')

        # Get a key
        result = yield from protocol.get('my_key')
        print(result)


The connection class
--------------------

``asyncio_redis.Connection`` takes care of connection pooling. Requests will
automatically be distributed among all connections.  If a connection is
blocking because of --for instance-- a blocking rpop, the other connections
will be used for new commands.

.. code:: python

    import asyncio
    from asyncio_redis import Connection

    @asyncio.coroutine
    def example():
        # Create Redis connection
        connection = yield from Connection.create(port=6379, poolsize=10)

        # Set a key
        yield from connection.set('my_key', 'my_value')

        # Get a key
        result = yield from connection.get('my_key')
        print(result)


Transactions
------------

Example:

.. code:: python

    import asyncio
    from asyncio_redis import Connection

    @asyncio.coroutine
    def example():
        # Create Redis connection
        connection = yield from Connection.create(port=6379, poolsize=10)

        # Create transaction
        transaction = yield from connection.multi()

        # Run commands in transaction (they return future objects)
        f1 = yield from transaction.set('key', 'value')
        f1 = yield from transaction.set('another_key', 'another_value')

        # Commit transaction
        yield from transaction.exec()

        # Retrieve results
        result1 = yield from f1
        result2 = yield from f2


It's recommended to use a large enough poolsize. A connection will be occupied
as long as there's a transaction running in there.


.. |Build Status| image:: https://travis-ci.org/jonathanslenders/asyncio-redis.png
    :target: https://travis-ci.org/jonathanslenders/asyncio-redis#
