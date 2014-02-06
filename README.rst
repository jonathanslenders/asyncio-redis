Redis client for PEP3156 asyncio (tulip)
========================================

|Build Status| |Build Status2| 

Features
--------

- Works for the asyncio (PEP3156) event loop
- No dependencies
- Connection pooling
- Automatic conversion from unicode (Python) to bytes (inside Redis.)
- Bytes and str protocols.
- Completely tested
- Blocking calls and transactions supported
- Streaming of some multi bulk replies
- Pubsub support


Installation
------------

.. code::

    pip install asyncio_redis

Documentation
-------------

View documentation at `read-the-docs`_

.. _read-the-docs: http://asyncio-redis.readthedocs.org/en/latest/


The connection class
--------------------

A ``asyncio_redis.Connection`` instance will take care of the connection and
will automatically reconnect, using a new transport when the connection drops.
This connection class also acts as a proxy to a ``asyncio_redis.RedisProtocol``
instance; any Redis command of the protocol can be called directly at the
connection.


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

Requests will automatically be distributed among all connections in a pool. If
a connection is blocking because of --for instance-- a blocking rpop, another
connection will be used for new commands.


.. code:: python

    import asyncio
    import asyncio_redis

    @asyncio.coroutine
    def example():
        # Create Redis connection
        connection = yield from asyncio_redis.Pool.create(host='localhost', port=6379, poolsize=10)

        # Set a key
        yield from connection.set('my_key', 'my_value')


Transactions example
--------------------

.. code:: python

    import asyncio
    import asyncio_redis

    @asyncio.coroutine
    def example():
        # Create Redis connection
        connection = yield from asyncio_redis.Pool.create(host='localhost', port=6379, poolsize=10)

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

.. code:: python

    import asyncio
    import asyncio_redis

    @asyncio.coroutine
    def example():
        # Create connection
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


Example using the Protocol class
--------------------------------

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
        asyncio.get_event_loop().run_until_complete(example())



.. |Build Status| image:: https://travis-ci.org/jonathanslenders/asyncio-redis.png
    :target: https://travis-ci.org/jonathanslenders/asyncio-redis#

.. |Build Status2| image:: https://drone.io/github.com/jonathanslenders/asyncio-redis/status.png
    :target: https://drone.io/github.com/jonathanslenders/asyncio-redis/latest
