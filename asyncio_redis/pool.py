import asyncio
from .connection import Connection, BytesConnection
from .exceptions import NoAvailableConnectionsInPool


__all__ = ('Pool', )


class Pool:
    """
    Wrapper around the Redis protocol.
    Takes care of setting up the connection and connection pooling.

    When poolsize > 1 and some connections are in use because of transactions
    or blocking requests, the other are preferred.

    ::

        pool = yield from Pool.create(poolsize=10)
        connection.set('key', 'value')
    """
    connection_class = Connection

    @classmethod
    @asyncio.coroutine
    def create(cls, host='localhost', port=6379, loop=None, password=None, db=0, auto_reconnect=True, poolsize=1):
        """
        Create a new connection instance.
        """
        self = cls()
        self._host = host
        self._port = port
        self._poolsize = poolsize

        # Create connections
        self._connections = []

        for i in range(poolsize):
            connection = yield from cls.connection_class.create(host=host, port=port, loop=loop,
                            password=password, db=db, auto_reconnect=auto_reconnect)
            self._connections.append(connection)

        return self

    def __repr__(self):
        return 'Pool(host=%r, port=%r, poolsize=%r)' % (self._host, self._port, self._poolsize)

    @property
    def poolsize(self):
        """ Number of parallel connections in the pool."""
        return self._poolsize

    @property
    def connections_in_use(self):
        """
        Return how many protocols are in use.
        """
        return sum([ 1 for c in self._connections if c.protocol.in_use ])

    @property
    def connections_connected(self):
        """
        The amount of open TCP connections.
        """
        return sum([ 1 for c in self._connections if c.protocol.is_connected ])

    def _get_free_connection(self):
        """
        Return the next protocol instance that's not in use.
        (A protocol in pubsub mode or doing a blocking request is considered busy,
        and can't be used for anything else.)
        """
        self._shuffle_connections()

        for c in self._connections:
            if c.protocol.is_connected and not c.protocol.in_use:
                return c

    def _shuffle_connections(self):
        """
        'shuffle' protocols. Make sure that we devide the load equally among the protocols.
        """
        self._connections = self._connections[1:] + self._connections[:1]

    def __getattr__(self, name): # Don't proxy everything, (no private vars, and use decorator to mark exceptions)
        """
        Proxy to a protocol. (This will choose a protocol instance that's not
        busy in a blocking request or transaction.)
        """
        connection = self._get_free_connection()

        if connection:
            return getattr(connection, name)
        else:
            raise NoAvailableConnectionsInPool('No available connections in the pool: size=%s, in_use=%s, connected=%s' % (
                                self.poolsize, self.connections_in_use, self.connections_connected))

