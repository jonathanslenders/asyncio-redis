import asyncio
import warnings
from functools import wraps

from .connection import Connection
from .exceptions import NoAvailableConnectionsInPoolError
from .protocol import RedisProtocol, Script


class Pool:
    """
    Pool of connections. Each
    Takes care of setting up the connection and connection pooling.

    When poolsize > 1 and some connections are in use because of transactions
    or blocking requests, the other are preferred.

    ::

        connection = await Pool.create(host='localhost', port=6379, poolsize=10)
        result = await connection.set('key', 'value')
    """

    @classmethod
    async def create(
        cls,
        host="localhost",
        port=6379,
        *,
        password=None,
        db=0,
        encoder=None,
        poolsize=1,
        auto_reconnect=True,
        loop=None,
        protocol_class=RedisProtocol,
    ):
        """
        Create a new connection pool instance.

        :param str host:
            Address, either host or unix domain socket path
        :param int port:
            TCP port. If port is 0 then host assumed to be unix socket path
        :param bytes password:
            Redis database password
        :param int db:
            Redis database
        :param encoder:
            Encoder to use for encoding to or decoding from redis bytes to a native type
        :type encoder:
            :class:`~asyncio_redis.encoders.BaseEncoder`
        :param int poolsize:
            The number of parallel connections.
        :param bool auto_reconnect:
            Enable auto reconnect
        :param protocol_class:
            (optional) redis protocol implementation
        :type protocol_class:
            :class:`~asyncio_redis.RedisProtocol`
        """
        self = cls()
        self._host = host
        self._port = port
        self._poolsize = poolsize

        if loop:
            warnings.warn("Deprecated parameter: loop", DeprecationWarning)

        # Create connections
        conn_coros = [
            Connection.create(
                host=host,
                port=port,
                password=password,
                db=db,
                encoder=encoder,
                auto_reconnect=auto_reconnect,
                protocol_class=protocol_class,
            )
            for _ in range(poolsize)
        ]
        self._connections = list(await asyncio.gather(*conn_coros))

        return self

    def __repr__(self):
        return (
            f"Pool(host='{self._host}', port={self._port}, poolsize={self._poolsize})"
        )

    @property
    def poolsize(self):
        """ Number of parallel connections in the pool."""
        return self._poolsize

    @property
    def connections_in_use(self):
        """
        Return how many protocols are in use.
        """
        return sum(int(c.protocol.in_use) for c in self._connections)

    @property
    def connections_connected(self):
        """
        The amount of open TCP connections.
        """
        return sum(int(c.protocol.is_connected) for c in self._connections)

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
        'shuffle' protocols. Make sure that we divide the load equally among the
        protocols.
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

        raise NoAvailableConnectionsInPoolError(
            f"No available connections in the pool: size={self.poolsize}, "
            f"in_use={self.connections_in_use}, connected={self.connections_connected}"
        )

    # Proxy the register_script method, so that the returned object will
    # execute on any available connection in the pool.
    @wraps(RedisProtocol.register_script)
    async def register_script(self, script: str) -> Script:
        # Call register_script from the Protocol.
        script = await self.__getattr__("register_script")(script)
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
