Redis client for Python asyncio.
================================

|Build Status|

..
    # Don't show the Build status on drone.io. Update script first. It's still
    # using an older Redis version.
    # |Build Status2| 


Redis client for the `PEP 3156`_ Python event loop.

.. _PEP 3156: http://legacy.python.org/dev/peps/pep-3156/

This Redis library is a completely asynchronous, non-blocking client for a Redis server.
It depends on asyncio (PEP 3156) and requires Python 3.6 or greater. If you're new to
asyncio, it can be helpful to check out `the asyncio documentation`_ first.

.. _the asyncio documentation: http://docs.python.org/dev/library/asyncio.html


Maintainers needed!
-------------------

Right now, this library is working fine, but not actively maintained, due to
lack of time and shift of priorities on my side (Jonathan). Most of my time
doing open source goes to prompt_toolkt community.

I still merge pull request when they are fine, especially for bug/security
fixes. But for a while now, we don't have new features. If you are already
using it, then there's not really a need to worry, asyncio-redis will keep
working fine, and we fix bugs, but it's not really evolving.

If anyone is interested to seriously take over development, please let me know.
Also keep in mind that there is a competing library called `aioredis`, which
does have a lot of activity.

See issue https://github.com/jonathanslenders/asyncio-redis/issues/134 to
discuss.


Features
--------

- Works for the asyncio (PEP3156) event loop
- No dependencies except asyncio
- Connection pooling
- Automatic conversion from unicode (Python) to bytes (inside Redis.)
- Bytes and str protocols.
- Completely tested
- Blocking calls and transactions supported
- Streaming of some multi bulk replies
- Pubsub support


*Trollius support*: There is `a fork by Ben Jolitz`_ that has the necessary
changes for using this asyncio-redis library with Trollius.

.. _a fork by Ben Jolitz: https://github.com/benjolitz/trollius-redis


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
        connection = yield from asyncio_redis.Connection.create(host='127.0.0.1', port=6379)

        # Set a key
        yield from connection.set('my_key', 'my_value')

        # When finished, close the connection.
        connection.close()

    if __name__ == '__main__':
        loop = asyncio.get_event_loop()
        loop.run_until_complete(example())


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
        connection = yield from asyncio_redis.Pool.create(host='127.0.0.1', port=6379, poolsize=10)

        # Set a key
        yield from connection.set('my_key', 'my_value')

        # When finished, close the connection pool.
        connection.close()


Transactions example
--------------------

.. code:: python

    import asyncio
    import asyncio_redis

    @asyncio.coroutine
    def example():
        # Create Redis connection
        connection = yield from asyncio_redis.Pool.create(host='127.0.0.1', port=6379, poolsize=10)

        # Create transaction
        transaction = yield from connection.multi()

        # Run commands in transaction (they return future objects)
        f1 = yield from transaction.set('key', 'value')
        f2 = yield from transaction.set('another_key', 'another_value')

        # Commit transaction
        yield from transaction.exec()

        # Retrieve results
        result1 = yield from f1
        result2 = yield from f2

        # When finished, close the connection pool.
        connection.close()

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
        connection = yield from asyncio_redis.Connection.create(host='127.0.0.1', port=6379)

        # Create subscriber.
        subscriber = yield from connection.start_subscribe()

        # Subscribe to channel.
        yield from subscriber.subscribe([ 'our-channel' ])

        # Inside a while loop, wait for incoming events.
        while True:
            reply = yield from subscriber.next_published()
            print('Received: ', repr(reply.value), 'on channel', reply.channel)

        # When finished, close the connection.
        connection.close()


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
        connection = yield from asyncio_redis.Connection.create(host='127.0.0.1', port=6379)

        # Set a key
        yield from connection.set('my_key', '2')

        # Register script
        multiply = yield from connection.register_script(code)

        # Run script
        script_reply = yield from multiply.run(keys=['my_key'], args=['5'])
        result = yield from script_reply.return_value()
        print(result) # prints 2 * 5

        # When finished, close the connection.
        connection.close()


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
                    asyncio_redis.RedisProtocol, '127.0.0.1', 6379)

        # Set a key
        yield from protocol.set('my_key', 'my_value')

        # Get a key
        result = yield from protocol.get('my_key')
        print(result)

        # Close transport when finished.
        transport.close()

    if __name__ == '__main__':
        asyncio.get_event_loop().run_until_complete(example())



.. |Build Status| image:: https://travis-ci.org/jonathanslenders/asyncio-redis.png
    :target: https://travis-ci.org/jonathanslenders/asyncio-redis#

.. |Build Status2| image:: https://drone.io/github.com/jonathanslenders/asyncio-redis/status.png
    :target: https://drone.io/github.com/jonathanslenders/asyncio-redis/latest
