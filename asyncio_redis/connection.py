import asyncio
from .protocol import RedisProtocol, RedisBytesProtocol, _all_commands
from .exceptions import RedisException

__all__ = ('Connection', 'BytesConnection')


class Connection:
    """
    Wrapper around the Redis protocol.
    Takes care of setting up the connection and connection pooling.

    When poolsize > 1 and some connections are in use because of transactions
    or blocking requests, the other are preferred.

    ::

        connection = yield from Connection.create(poolsize=10)
        connection.set('key', 'value')
    """
    protocol = RedisProtocol

    @classmethod
    @asyncio.coroutine
    def create(cls, host='localhost', port=6379, loop=None, poolsize=1, password=None, db=0):
        """
        Create a new connection instance.
        """
        if loop is None:
            loop = asyncio.get_event_loop()

        # Inherit protocol
        redis_protocol = type('RedisProtocol', (cls.protocol,), { 'password': password, 'db': db })

        self = cls()
        self._poolsize = poolsize

        # Create connections
        self._transport_protocol_pairs  = []

        for i in range(poolsize):
            transport, protocol = yield from asyncio.Task(loop.create_connection(redis_protocol, host, port))
            self._transport_protocol_pairs.append( (transport, protocol) )

        return self

    @property
    def poolsize(self):
        """ Number of parallel connections in the pool."""
        return self._poolsize

    @property
    def connections_in_use(self):
        """
        Return how many protocols are in use.
        """
        return sum([ 1 for transport, protocol in self._transport_protocol_pairs if protocol.in_use ])

    def _get_free_protocol(self):
        """
        Return the next protocol instance that's not in use.
        (A protocol in pubsub mode or doing a blocking request is considered busy,
        and can't be used for anything else.)
        """
        self._shuffle_protocols()

        for transport, protocol in self._transport_protocol_pairs:
            if not protocol.in_use:
                return protocol

    def _shuffle_protocols(self):
        """
        'shuffle' protocols. Make sure that we devide the load equally among the protocols.
        """
        self._transport_protocol_pairs = self._transport_protocol_pairs[1:] + self._transport_protocol_pairs[:1]

    def __getattr__(self, name): # Don't proxy everything, (no private vars, and use decorator to mark exceptions)
        """
        Proxy to a protocol. (This will choose a protocol instance that's not
        busy in a blocking request or transaction.)
        """
        # Only proxy commands.
        if name not in _all_commands:
            raise AttributeError

        protocol = self._get_free_protocol()

        if protocol:
            return getattr(protocol, name)
        else:
            raise RedisException('All connection in the pool are in use. Please increase the poolsize.')


class BytesConnection:
    """
    Connection that uses :class:`RedisBytesProtocol`
    """
    protocol = RedisBytesProtocol
