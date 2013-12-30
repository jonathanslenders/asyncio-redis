from .connection import Connection, BytesConnection
from .exceptions import NoAvailableConnectionsInPoolError
from .protocol import RedisProtocol, RedisBytesProtocol, Script
from functools import wraps
import asyncio


__all__ = ('Pool', 'BytesPool', )


class Pool:
    """
    Pool of connections. Each
    Takes care of setting up the connection and connection pooling.

    When poolsize > 1 and some connections are in use because of transactions
    or blocking requests, the other are preferred.

    ::

        pool = yield from Pool.create(host='localhost', port=6379, poolsize=10)
        result = yield from connection.set('key', 'value')
    """

    protocol = RedisProtocol
    """
    The :class:`RedisProtocol` class to be used for each connection in this pool.
    """

    @classmethod
    def get_connection_class(cls):
        """
        Return the :class:`Connection` class to be used for every connection in
        this pool. Normally this is just a ``Connection`` using the defined ``protocol``.
        """
        class ConnectionClass(Connection):
            protocol = cls.protocol
        return ConnectionClass

    @classmethod
    @asyncio.coroutine
    def create(cls, host='localhost', port=6379, loop=None, password=None, db=0, poolsize=1, auto_reconnect=True):
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
            connection_class = cls.get_connection_class()
            connection = yield from connection_class.create(host=host, port=port, loop=loop,
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

    def __getattr__(self, name):
        """
        Proxy to a protocol. (This will choose a protocol instance that's not
        busy in a blocking request or transaction.)
        """
        connection = self._get_free_connection()

        if connection:
            return getattr(connection, name)
        else:
            raise NoAvailableConnectionsInPoolError('No available connections in the pool: size=%s, in_use=%s, connected=%s' % (
                                self.poolsize, self.connections_in_use, self.connections_connected))


    # Proxy the register_script method, so that the returned object will
    # execute on any available connection in the pool.
    @asyncio.coroutine
    @wraps(RedisProtocol.register_script)
    def register_script(self, script:str) -> Script:
        # Call register_script from the Protocol.
        script = yield from self.__getattr__('register_script')(script)
        assert isinstance(script, Script)

        # Return a new script instead that runs it on any connection of the pool.
        return Script(script.sha, script.code, lambda: self.evalsha)


class BytesPool:
    """
    Identical to :class:`Pool`, but uses :class:`RedisBytesProtocol` instead.
    """
    protocol = RedisBytesProtocol

