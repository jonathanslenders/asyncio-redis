__all__ = (
        'Error',
        'TransactionError',
        'NotConnected',
        'ConnectionLost',
        'NoAvailableConnectionsInPool',
)


# See following link for the proper way to create user defined exceptions:
# http://docs.python.org/3.3/tutorial/errors.html#user-defined-exceptions


class Error(Exception):
    """ Base exception. """


class TransactionError(Error):
    """ Transaction failed. """


class NotConnected(Error):
    """ Protocol is not connected. """
    def __init__(self, message='Not connected'):
        super().__init__(message)


class ConnectionLost(NotConnected):
    """
    Connection lost during query.
    (Special case of NotConnected.)
    """
    def __init__(self, exc):
        pass


class NoAvailableConnectionsInPool(NotConnected):
    """
    When the connection pool has no available connections.
    """

