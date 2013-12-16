__all__ = (
        'Error',
        'TransactionError',
        'NotConnected',
)


# See following link for the proper way to create user defined exceptions:
# http://docs.python.org/3.3/tutorial/errors.html#user-defined-exceptions


class Error(Exception):
    """ Base exception. """


class TransactionError(Error):
    """ Transaction failed. """


class NotConnected(Error):
    """ Protocol is not connected. """
    def __init__(self):
        super().__init__('Not connected')


class ConnectionLost(NotConnected):
    """
    Connection lost during query.
    (Special case of NotConnected.)
    """
    def __init__(self, exc):
        pass

