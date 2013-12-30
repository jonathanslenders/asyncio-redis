from .protocol import RedisProtocol, RedisBytesProtocol, _all_commands
from asyncio.log import logger
import asyncio
import logging


__all__ = ('Connection', 'BytesConnection')


class Connection:
    """
    Wrapper around the protocol and transport which takes care of establishing
    the connection and reconnecting it.

    ::

        connection = yield from Connection.create(host='localhost', port=6379)
        result = yield from connection.set('key', 'value')
    """
    protocol = RedisProtocol
    """
    The :class:`RedisProtocol` class to be used this connection.
    """

    @classmethod
    @asyncio.coroutine
    def create(cls, host='localhost', port=6379, loop=None, password=None, db=0, auto_reconnect=True):
        connection = cls()

        connection.host = host
        connection.port = port
        connection._loop = loop
        connection._retry_interval = .5

        # Create protocol instance
        protocol_factory = type('RedisProtocol', (cls.protocol,), { 'password': password, 'db': db })

        if auto_reconnect:
            class protocol_factory(protocol_factory):
                def connection_lost(self, exc):
                    super().connection_lost(exc)
                    asyncio.Task(connection._reconnect())

        connection.protocol = protocol_factory()

        # Connect
        yield from connection._reconnect()

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
        self._retry_interval = .5

    def _increase_retry_interval(self):
        """ When a connection failed. Increase the interval."""
        self._retry_interval = min(60, 1.5 * self._retry_interval)

    def _reconnect(self):
        """
        Set up Redis connection.
        """
        loop = self._loop or asyncio.get_event_loop()
        while True:
            try:
                logger.log(logging.INFO, 'Connecting to redis')
                yield from loop.create_connection(lambda:self.protocol, self.host, self.port)
                self._reset_retry_interval()
                return
            except OSError:
                # Sleep and try again
                self._increase_retry_interval()
                interval = self._get_retry_interval()
                logger.log(logging.INFO, 'Connecting to redis failed. Retrying in %i seconds' % interval)
                yield from asyncio.sleep(interval)

    def __getattr__(self, name): # Don't proxy everything, (no private vars, and use decorator to mark exceptions)
        # Only proxy commands.
        if name not in _all_commands:
            raise AttributeError

        return getattr(self.protocol, name)

    def __repr__(self):
        return 'Connection(host=%r, port=%r)' % (self.host, self.port)


class BytesConnection(Connection):
    """
    Identical to :class:`Connection`, but uses :class:`RedisBytesProtocol` instead.
    """
    protocol = RedisBytesProtocol
