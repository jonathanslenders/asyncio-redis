.. asyncio_redis documentation master file, created by
   sphinx-quickstart on Thu Oct 31 08:50:13 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

asyncio_redis
=============

Redis client for PEP 3156. (Tulip)  - `Github`_

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

Example using the :class:`asyncio_redis.RedisProtocol` class
------------------------------------------------------------

.. code:: python

    import asyncio
    import asyncio_redis

    @asyncio.coroutine
    def example():
        loop = asyncio.get_event_loop()

        # Create Redis connection
        transport, protocol = yield from loop.create_connection(
                    asyncio_redis.RedisProtocol, 'localhost', 6379)

        # Set a key
        yield from protocol.set('my_key', 'my_value')

        # Get a key
        result = yield from protocol.get('my_key')
        print(result)

    if __name__ == '__main__':
        asyncio.get_event_loop().run_until_complete(run())

See :ref:`the reference <redis-reference>` to learn more about how to call other
Redis commands.


The :class:`asyncio_redis.Connection` class
-------------------------------------------

The :class:`asyncio_redis.Connection` class will take care of your connection and
will automatically reconnect, using a new transport, when the connection
drops.

.. code:: python

    import asyncio
    import asyncio_redis

    @asyncio.coroutine
    def example():
        # Create Redis connection
        connection = yield from asyncio_redis.Connection.create(host='localhost', port=6379)

        # Set a key
        yield from connection.set('my_key', 'my_value')


Connection pooling
------------------

Requests will automatically be distributed among all connections in a
:class:`asyncio_redis.Pool`. If a connection is blocking because of --for
instance-- a blocking rpop, another connection will be used for new commands.


.. code:: python

    import asyncio
    import asyncio_redis

    @asyncio.coroutine
    def example():
        # Create Redis connection
        connection = yield from asyncio_redis.Pool.create(host='localhost', port=6379, poolsize=10)

        # Set a key
        yield from connection.set('my_key', 'my_value')


Transaction example
-------------------

.. code:: python

    import asyncio
    import asyncio_redis

    @asyncio.coroutine
    def example(loop):
        # Create Redis connection
        connection = yield from asyncio_redis.Connection.create(host='localhost', port=6379, poolsize=10)

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


Pubsub example
--------------

By calling :func:`asyncio_redis.RedisProtocol.start_subscribe` (either on the
protocol, through the ``Connection`` class or through the ``Pool`` class), you
can start a pubsub listener.

.. code:: python

    import asyncio
    import asyncio_redis

    @asyncio.coroutine
    def example():
        # Create connection (you can also use Connection.create)
        connection = yield from asyncio_redis.Connection.create(host='localhost', port=6379)

        # Create subscriber.
        subscriber = yield from connection.start_subscribe()

        # Subscribe to channel.
        yield from subscriber.subscribe([ 'our-channel' ])

        # Inside a while loop, wait for incoming events.
        while True:
            reply = yield from subscriber.get_next_published()
            print('Received: ', repr(reply.value), 'on channel', reply.channel)


LUA Scripting example
---------------------

.. code:: python

    import asyncio
    import asyncio_redis

    code = \
    """
    local value = redis.call('GET', KEYS[1])
    value = tonumber(value)
    return value * ARGV[1]
    """

    @asyncio.coroutine
    def example():
        connection = yield from asyncio_redis.Connection.create(host='localhost', port=6379)

        # Set a key
        yield from connection.set('my_key', '2')
        
        # Register script
        multiply = yield from connection.register_script(code)
        
        # Run script
        result = yield from multiply.run(keys=['my_key'], args=['5'])
        print(result) # prints 2 * 5


Reference
---------

:ref:`View the reference with all commands <redis-reference>`

View `the source code at GitHub`_

.. _the source code at GitHub: https://github.com/jonathanslenders/asyncio-redis


Author and License
==================

The ``asyncio_redis`` package is written by Jonathan Slenders.
It's BSD licensed and freely available.

Feel free to improve this package and `send a pull request`_.

.. _send a pull request: https://github.com/jonathanslenders/asyncio-redis


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. toctree::
   :maxdepth: 2

   pages/reference
   pages/replies
