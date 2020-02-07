import asyncio
import logging
import warnings

from .log import logger
from .protocol import RedisProtocol, _all_commands


class Connection:
    """
    Wrapper around the protocol and transport which takes care of establishing
    the connection and reconnecting it.


    ::
        connection = await Connection.create(host='localhost', port=6379)
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
        auto_reconnect=True,
        loop=None,
        protocol_class=RedisProtocol,
    ):
        """
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
        :param bool auto_reconnect:
            Enable auto reconnect
        :param protocol_class:
            (optional) redis protocol implementation
        :type protocol_class:
            :class:`~asyncio_redis.RedisProtocol`
        """
        assert port >= 0, "Unexpected port value: %r" % (port,)
        if loop:
            warnings.warn("Deprecated parameter: loop", DeprecationWarning)

        connection = cls()

        connection.host = host
        connection.port = port
        connection._retry_interval = 0.5
        connection._closed = False
        connection._closing = False

        connection._auto_reconnect = auto_reconnect

        # Create protocol instance
        def connection_lost():
            if connection._auto_reconnect and not connection._closing:
                loop = asyncio.get_event_loop()
                loop.create_task(connection._reconnect())

        # Create protocol instance
        connection.protocol = protocol_class(
            password=password,
            db=db,
            encoder=encoder,
            connection_lost_callback=connection_lost,
        )

        # Connect
        if connection._auto_reconnect:
            await connection._reconnect()
        else:
            await connection._connect()

        return connection

    @property
    def transport(self):
        """ The transport instance that the protocol is currently using. """
        return self.protocol.transport

    def _get_retry_interval(self):
        """ Time to wait for a reconnect in seconds. """
        return self._retry_interval

    def _reset_retry_interval(self):
        """ Set the initial retry interval. """
        self._retry_interval = 0.5

    def _increase_retry_interval(self):
        """ When a connection failed. Increase the interval."""
        self._retry_interval = min(60, 1.5 * self._retry_interval)

    async def _connect(self):
        """
        Set up Redis connection.
        """
        loop = asyncio.get_event_loop()
        logger.log(logging.INFO, "Connecting to redis")
        if self.port:
            await loop.create_connection(lambda: self.protocol, self.host, self.port)
        else:
            await loop.create_unix_connection(lambda: self.protocol, self.host)

    async def _reconnect(self):
        """
        Set up Redis re-connection.
        """
        while True:
            try:
                await self._connect()
                self._reset_retry_interval()
                return
            except OSError:
                # Sleep and try again
                self._increase_retry_interval()
                interval = self._get_retry_interval()
                logger.log(
                    logging.INFO,
                    f"Connecting to redis failed. Retrying in {interval} seconds",
                )
                await asyncio.sleep(interval)

    def __getattr__(self, name):
        # Only proxy commands.
        if name not in _all_commands:
            raise AttributeError

        return getattr(self.protocol, name)

    def __repr__(self):
        return "Connection(host=%r, port=%r)" % (self.host, self.port)

    def close(self):
        """
        Close the connection transport.
        """
        self._closing = True

        if self.protocol.transport:
            self.protocol.transport.close()
