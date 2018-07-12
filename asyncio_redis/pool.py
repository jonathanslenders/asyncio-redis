from .connection import Connection
from .exceptions import NoAvailableConnectionsInPoolError
from .protocol import RedisProtocol, Script

from functools import wraps
import asyncio


__all__ = ('Pool', )


class Pool:
    """
    Pool of connections. Each
    Takes care of setting up the connection and connection pooling.

    When poolsize > 1 and some connections are in use because of transactions
    or blocking requests, the other are preferred.

    ::

        connection = yield from Pool.create(host='localhost', port=6379, poolsize=10)
        result = yield from connection.set('key', 'value')
    """
    @classmethod
    @asyncio.coroutine
    def create(cls, host='localhost', port=6379, *, password=None, db=0,
               encoder=None, poolsize=1, auto_reconnect=True, loop=None,
               protocol_class=RedisProtocol):
        """
        Create a new connection pool instance.

        :param host: Address, either host or unix domain socket path
        :type host: str
        :param port: TCP port. If port is 0 then host assumed to be unix socket path
        :type port: int
        :param password: Redis database password
        :type password: bytes
        :param db: Redis database
        :type db: int
        :param encoder: Encoder to use for encoding to or decoding from redis bytes to a native type.
        :type encoder: :class:`~asyncio_redis.encoders.BaseEncoder` instance.
        :param poolsize: The number of parallel connections.
        :type poolsize: int
        :param auto_reconnect: Enable auto reconnect
        :type auto_reconnect: bool
        :param loop: (optional) asyncio event loop.
        :type protocol_class: :class:`~asyncio_redis.RedisProtocol`
        :param protocol_class: (optional) redis protocol implementation
        """
        self = cls()
        self._host = host
        self._port = port
        self._poolsize = poolsize

        # Create connections
        self._connections = []

        for i in range(poolsize):
            connection = yield from Connection.create(host=host, port=port,
                            password=password, db=db, encoder=encoder,
                            auto_reconnect=auto_reconnect, loop=loop,
                            protocol_class=protocol_class)
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

    def close(self):
        """
        Close all the connections in the pool.
        """
        for c in self._connections:
            c.close()

        self._connections = []
