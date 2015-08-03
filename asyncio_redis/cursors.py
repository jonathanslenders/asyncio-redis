import asyncio
from collections import deque

__all__ = (
    'Cursor',
    'DictCursor',
    'SetCursor',
    'ZCursor',
)

SCAN_COUNT_DEFAULT = 10


class Cursor:
    """
    Cursor for walking through the results of a :func:`scan
    <asyncio_redis.RedisProtocol.scan>` query.
    """
    def __init__(self, name, scanfunc):
        self._queue = deque()
        self._cursor = 0
        self._name = name
        self._scanfunc = scanfunc
        self._done = False

        # The preferred chunk size, passed to redis.
        # This can be changed before every call to ``fetchone`` or ``fetch_all``.
        self.count = SCAN_COUNT_DEFAULT

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self._name)

    @asyncio.coroutine
    def _fetch_more(self):
        """ Get next chunk of keys from Redis """
        if not self._done:
            chunk = yield from self._scanfunc(self._cursor, self.count)
            self._cursor = chunk.new_cursor_pos

            if chunk.new_cursor_pos == 0:
                self._done = True

            for i in chunk.items:
                self._queue.append(i)

    @asyncio.coroutine
    def fetchone(self):
        """
        Coroutines that returns the next item.
        It returns `None` after the last item.
        """
        # Make sure that we have at least some items in our queue, unless we're done.
        # Notice that we are using 'while' instead of 'if'. This is because
        # Redis can return a chunk of zero items, even when we're not yet finished.
        # See: https://github.com/jonathanslenders/asyncio-redis/issues/65#issuecomment-127026408
        while not self._queue and not self._done:
            yield from self._fetch_more()

        # Return the next item.
        if self._queue:
            return self._queue.popleft()

    @asyncio.coroutine
    def fetchall(self):
        """ Coroutine that reads all the items in one list. """
        results = []

        while not self._done:
            yield from self._fetch_more()
            results.extend(self._queue)
            self._queue.clear()

        return results


class SetCursor(Cursor):
    """
    Cursor for walking through the results of a :func:`sscan
    <asyncio_redis.RedisProtocol.sscan>` query.
    """
    @asyncio.coroutine
    def fetchall(self):
        result = yield from super().fetchall()
        return set(result)


class DictCursor(Cursor):
    """
    Cursor for walking through the results of a :func:`hscan
    <asyncio_redis.RedisProtocol.hscan>` query.
    """
    def _parse(self, key, value):
        return key, value

    @asyncio.coroutine
    def fetchone(self):
        """
        Get next { key: value } tuple
        It returns `None` after the last item.
        """
        key = yield from super().fetchone()
        value = yield from super().fetchone()

        if key is not None:
            key, value = self._parse(key, value)
            return { key: value }

    @asyncio.coroutine
    def fetchall(self):
        """ Coroutine that reads all the items in one dictionary. """
        results = {}

        while True:
            i = yield from self.fetchone()
            if i is None:
                break
            else:
                results.update(i)

        return results


class ZCursor(DictCursor):
    """
    Cursor for walking through the results of a :func:`zscan
    <asyncio_redis.RedisProtocol.zscan>` query.
    """
    def _parse(self, key, value):
        # Mapping { key: score_as_float }
        return key, float(value)
