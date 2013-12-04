__all__ = (
        'RedisException',
        'TransactionError',
)

class RedisException(Exception):
    pass

class TransactionError(RedisException):
    pass
