.. _redis-examples:

Examples
=========

The :class:`Connection <asyncio_redis.Connection>` class
--------------------------------------------------------

A :class:`Connection <asyncio_redis.Connection>` instance will take care of the
connection and will automatically reconnect, using a new transport when the
connection drops. This connection class also acts as a proxy to at 
:class:`RedisProtocol <asyncio_redis.RedisProtocol>` instance; any Redis
command of the protocol can be called directly at the connection.

.. code:: python

    import asyncio
    import asyncio_redis

    @asyncio.coroutine
    def example():
        # Create Redis connection
        connection = yield from asyncio_redis.Connection.create(host='localhost', port=6379)

        # Set a key
        yield from connection.set('my_key', 'my_value')

See :ref:`the reference <redis-reference>` to learn more about the other Redis
commands.


Connection pooling
------------------

Requests will automatically be distributed among all connections in a
:class:`Pool <asyncio_redis.Pool>`. If a connection is blocking because of
--for instance-- a blocking rpop, another connection will be used for new
commands.

.. note:: This is the recommended way to connect to the Redis server.

.. code:: python

    import asyncio
    import asyncio_redis

    @asyncio.coroutine
    def example():
        # Create Redis connection
        connection = yield from asyncio_redis.Pool.create(host='localhost', port=6379, poolsize=10)

        # Set a key
        yield from connection.set('my_key', 'my_value')


Transactions
------------

A transaction can be started by calling :func:`multi
<asyncio_redis.RedisProtocol.multi>`. This returns a :class:`Transaction
<asyncio_redis.Transaction>` instance which is in fact just a proxy to the
:class:`RedisProtocol <asyncio_redis.RedisProtocol>`, except that every Redis
method of the protocol now became a coroutine that returns a future. The
results of these futures can be retrieved after the transaction is commited
with :func:`exec <asyncio_redis.Transaction.exec>`.

.. code:: python

    import asyncio
    import asyncio_redis

    @asyncio.coroutine
    def example(loop):
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


Pubsub
------

By calling :func:`start_subscribe
<asyncio_redis.RedisProtocol.start_subscribe>` (either on the protocol, through
the :class:`Connection <asyncio_redis.Connection>` class or through the :class:`Pool
<asyncio_redis.Pool>` class), you can start a pubsub listener.

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
            reply = yield from subscriber.next_published()
            print('Received: ', repr(reply.value), 'on channel', reply.channel)


LUA Scripting
-------------

The :func:`register_script <asyncio_redis.RedisProtocol.register_script>`
function -- which can be used to register a LUA script -- returns a
:class:`Script <asyncio_redis.Script>` instance. You can call its :func:`run
<asyncio_redis.Script.run>` method to execute this script.


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
        script_reply = yield from multiply.run(keys=['my_key'], args=['5'])
        result = yield from script_reply.return_value()
        print(result) # prints 2 * 5


Raw bytes or UTF-8
------------------

The redis protocol only knows about bytes, but normally you want to use strings
in your Python code. ``asyncio_redis`` is helpful and installs an encoder that
does this conversion automatically, using the UTF-8 codec. However, sometimes
you want to access raw bytes. This is possible by passing a
:class:`BytesEncoder <asyncio_redis.encoders.BytesEncoder>` instance to the
connection, pool or protocol.

.. code:: python

    import asyncio
    import asyncio_redis

    from asyncio_redis.encoders import BytesEncoder

    @asyncio.coroutine
    def example():
        # Create Redis connection
        connection = yield from asyncio_redis.Connection.create(host='localhost', port=6379, encoder=BytesEncoder())

        # Set a key
        yield from connection.set(b'my_key', b'my_value')


Scanning for keys
-----------------

Redis has a few nice scanning utilities to discover keys in the database. They
are rather low-level, but ``asyncio_redis`` exposes a simple
:class:`~asyncio_redis.cursors.Cursor` class that allows you to iterate over
all the keys matching a certain pattern. Each call of the
:func:`~asyncio_redis.cursors.Cursor.fetchone` coroutine will return the next
match. You don't have have to worry about accessing the server every x pages.

The following example will print all the keys in the database:

.. code:: python

    import asyncio
    import asyncio_redis

    from asyncio_redis.encoders import BytesEncoder

    @asyncio.coroutine
    def example():
        cursor = yield from protocol.scan(match='*')
        while True:
            item = yield from cursor.fetchone()
            if item is None:
                break
            else:
                print(item)


See the scanning utilities: :func:`~asyncio_redis.RedisProtocol.scan`,
:func:`~asyncio_redis.RedisProtocol.sscan`,
:func:`~asyncio_redis.RedisProtocol.hscan` and
:func:`~asyncio_redis.RedisProtocol.zscan`


The :class:`RedisProtocol <asyncio_redis.RedisProtocol>` class
--------------------------------------------------------------

The most low level way of accessing the redis server through this library is
probably by creating a connection with the `RedisProtocol` yourself. You can do
it as follows:

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


.. note:: It is not recommended to use the Protocol class directly, because the
          low-level Redis implementation could change. Prefer the
          :class:`Connection <asyncio_redis.Connection>` or :class:`Pool
          <asyncio_redis.Pool>` class as demonstrated above if possible.
