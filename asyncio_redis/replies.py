import asyncio
from asyncio.queues import Queue
from asyncio.tasks import gather

__all__ = (
    'BlockingPopReply',
    'DictReply',
    'ListReply',
    'MultiBulkReply',
    'PubSubReply',
    'SetReply',
    'StatusReply',
    'SubscribeReply',
    'ZRangeReply',
)


class StatusReply:
    """
    Wrapper for Redis status replies.
    (for messages like OK, QUEUED, etc...)
    """
    def __init__(self, status):
        self.status = status

    def __repr__(self):
        return 'StatusReply(status=%r)' % self.status

    def __eq__(self, other):
        return self.status == other.status


class MultiBulkReply:
    """
    Container for a multi bulk reply.
    There are two ways of retrieving the content:

    1. you call ``yield from get_as_list()`` which returns when all the
       items arrived;

    2. or you iterate over it for every item, using this pattern:

    ::

        for f in multi_bulk_reply:
            item = yield from f
            print(item)
    """
    def __init__(self, count):
        self.queue = Queue()
        self.count = count

    def __iter__(self):
        for i in range(self.count):
            yield self.queue.get()

    def get_as_list(self):
        """
        Wait for all of the items of the multibulk reply to come in.
        and return it as a list.
        """
        return gather(*list(self))


class DictReply:
    """ Container for a dict reply. """
    def __init__(self, multibulk_reply):
        self._result = multibulk_reply

    def _parse(self, key, value):
        return key, value

    def __iter__(self):
        """ Yield a list of futures that yield {key: value } tuples. """
        i = iter(self._result)

        @asyncio.coroutine
        def getter(key_f, value_f):
            """ Coroutine which processes one item. """
            key, value = yield from gather(key_f, value_f)
            key, value = self._parse(key, value)
            return { key: value }

        while True:
            yield asyncio.Task(getter(next(i), next(i)))

    @asyncio.coroutine
    def get_as_dict(self):
        """
        Return the result of a sorted set query as dictionary.
        This is a mapping from the elements to their scores.
        """
        result = { }
        for f in self:
            result.update((yield from f))
        return result

    @asyncio.coroutine
    def get_keys_as_list(self):
        """ Return the keys as a list. """
        result = []
        for f in self:
            result += (yield from f).keys()
        return result


class ZRangeReply(DictReply):
    """
    Container for a zrange query result.
    """
    def _parse(self, key, value):
        # Mapping { key: score_as_float }
        return key, float(value)


class SetReply:
    """
    Redis set result.
    The content can be retrieved by calling ``get_as_set`` or by
    iterating over it:

    ::

        for f in set_reply:
            item = yield from f
            print(item)
    """
    def __init__(self, multibulk_reply):
        self._result = multibulk_reply

    def __iter__(self):
        """ Yield a list of futures. """
        return iter(self._result)

    @asyncio.coroutine
    def get_as_set(self):
        """ Return the result as a Python set.  """
        result = yield from self._result.get_as_list()
        return set(result)


class ListReply:
    def __init__(self, multibulk_reply):
        self._result = multibulk_reply

    def __iter__(self):
        """ Yield a list of futures. """
        return iter(self._result)

    def get_as_list(self):
        """ Return the result as a Python set.  """
        return self._result.get_as_list()


class BlockingPopReply:
    """ ``blpop`` or ``brpop`` reply """
    def __init__(self, list_name, value):
        self._list_name = list_name
        self._value = value

    @property
    def list_name(self):
        """ List name. """
        return self._list_name

    @property
    def value(self):
        """ Popped value """
        return self._value


class SubscribeReply:
    """ Answer to subscribe command. """
    def __init__(self, channel):
        self._channel = channel

    @property
    def channel(self):
        """ Channel name. """
        return self._channel


class PubSubReply:
    """ Received pubsub message. """
    def __init__(self, channel, value):
        self._channel = channel
        self._value = value

    @property
    def channel(self):
        """ Channel name """
        return self._channel

    @property
    def value(self):
        """ Received PubSub value """
        return self._value
