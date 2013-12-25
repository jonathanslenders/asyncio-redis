.. _redis-reference:

Reference
=========

You can either use the :class:`RedisProtocol` class directly, use the
:class:`Connection` class, or use the :class:`Pool` wrapper which also offers
connection pooling.

- :class:`asyncio_redis.RedisProtocol`
- :class:`asyncio_redis.Connection`
- :class:`asyncio_redis.Pool`


.. autoclass:: asyncio_redis.RedisProtocol
    :members:
    :exclude-members: data_received, eof_received, connection_lost

.. autoclass:: asyncio_redis.RedisBytesProtocol

.. autoclass:: asyncio_redis.Connection
    :members:

.. autoclass:: asyncio_redis.Pool
    :members:

.. autoclass:: asyncio_redis.ZScoreBoundary
    :members:

.. autoclass:: asyncio_redis.Transaction
    :members:

.. autoclass:: asyncio_redis.Subscription
    :members:

.. autoclass:: asyncio_redis.Script
    :members:

.. autoclass:: asyncio_redis.ZAggregate
    :members:
