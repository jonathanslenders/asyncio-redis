.. _redis-reference:

Reference
=========

You can either use the :class:`RedisProtocol <asyncio_redis.RedisProtocol>`
class directly, use the :class:`Connection <asyncio_redis.Connection>` class,
or use the :class:`Pool <asyncio_redis.Pool>` wrapper which also offers
connection pooling.

The Protocol
------------

.. autoclass:: asyncio_redis.RedisProtocol
    :members:
    :undoc-members:
    :exclude-members: data_received, eof_received, connection_lost

.. autoclass:: asyncio_redis.RedisBytesProtocol


Encoders
----------

.. autoclass:: asyncio_redis.encoders.BaseEncoder
    :members:
    :undoc-members:

.. autoclass:: asyncio_redis.encoders.UTF8Encoder
    :members:

.. autoclass:: asyncio_redis.encoders.BytesEncoder
    :members:


Connection
----------

.. autoclass:: asyncio_redis.Connection
    :members:
    :exclude-members: register_script

.. autoclass:: asyncio_redis.BytesConnection

Connection pool
---------------

.. autoclass:: asyncio_redis.Pool
    :members:

.. autoclass:: asyncio_redis.BytesPool

Command replies
---------------

.. autoclass:: asyncio_redis.StatusReply
    :members:

.. autoclass:: asyncio_redis.DictReply
    :members:

.. autoclass:: asyncio_redis.ListReply
    :members:

.. autoclass:: asyncio_redis.SetReply
    :members:

.. autoclass:: asyncio_redis.ZRangeReply
    :members:

.. autoclass:: asyncio_redis.PubSubReply
    :members:

.. autoclass:: asyncio_redis.BlockingPopReply
    :members:

Utils
-----

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

