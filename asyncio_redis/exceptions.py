__all__ = (
        'ConnectionLost',
        'Error',
        'ErrorReply',
        'NoAvailableConnectionsInPool',
        'NoRunningScriptError',
        'NotConnected',
        'ScriptKilledError',
        'TransactionError',
)


# See following link for the proper way to create user defined exceptions:
# http://docs.python.org/3.3/tutorial/errors.html#user-defined-exceptions


class Error(Exception):
    """ Base exception. """


class ErrorReply(Exception):
    """ Exception when the redis server returns an error. """


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

class ScriptKilledError(Error):
    """ Script was killed during an evalsha call. """


class NoRunningScriptError(Error):
    """ script_kill was called while no script was running. """
