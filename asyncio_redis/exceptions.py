__all__ = (
        'RedisException',
        'TransactionError',
        'NotConnected',
)


# See following link for the proper way to create user defined exceptions:
# http://docs.python.org/3.3/tutorial/errors.html#user-defined-exceptions


class RedisException(Exception):
    pass


class TransactionError(RedisException):
    pass


class NotConnected(RedisException):
    def __init__(self):
        super().__init__('Not connected')


class ConnectionLost(NotConnected):
    def __init__(self, exc):
        pass

