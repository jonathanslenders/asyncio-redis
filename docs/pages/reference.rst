.. _redis-reference:

Reference
=========

You can either use the `RedisProtocol` class directly, or use the `Connection`
wrapper which also offers connection pooling.

- :class:`asyncio_redis.RedisProtocol`
- :class:`asyncio_redis.Connection`


.. autoclass:: asyncio_redis.RedisProtocol
    :members:
    :exclude-members: data_received, eof_received, connection_lost


.. autoclass:: asyncio_redis.Connection
    :members:

.. autoclass:: asyncio_redis.StatusReply
    :members:

.. autoclass:: asyncio_redis.MultiBulkReply
    :members:

.. autoclass:: asyncio_redis.ZRangeResult
    :members:

.. autoclass:: asyncio_redis.ZScoreBoundary
    :members:

.. autoclass:: asyncio_redis.Transaction
    :members:
