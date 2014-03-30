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

Connection pool
---------------

.. autoclass:: asyncio_redis.Pool
    :members:

Command replies
---------------

.. autoclass:: asyncio_redis.replies.StatusReply
    :members:

.. autoclass:: asyncio_redis.replies.DictReply
    :members:

.. autoclass:: asyncio_redis.replies.ListReply
    :members:

.. autoclass:: asyncio_redis.replies.SetReply
    :members:

.. autoclass:: asyncio_redis.replies.ZRangeReply
    :members:

.. autoclass:: asyncio_redis.replies.PubSubReply
    :members:

.. autoclass:: asyncio_redis.replies.BlockingPopReply
    :members:

.. autoclass:: asyncio_redis.replies.BlockingPopPushReply
    :members:

.. autoclass:: asyncio_redis.replies.InfoReply
    :members:

.. autoclass:: asyncio_redis.replies.ClientListReply
    :members:


Cursors
-------

.. autoclass:: asyncio_redis.cursors.Cursor
    :members:

.. autoclass:: asyncio_redis.cursors.SetCursor
    :members:

.. autoclass:: asyncio_redis.cursors.DictCursor
    :members:

.. autoclass:: asyncio_redis.cursors.ZCursor
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


Exceptions
----------

.. autoclass:: asyncio_redis.exceptions.TimeoutError
    :members:
