#!/usr/bin/env python3
import asyncio
import signal
import logging
import types

from asyncio.futures import Future
from asyncio.log import logger
from asyncio.queues import Queue
from asyncio.streams import StreamReader

from collections import deque
from functools import wraps
from inspect import getfullargspec, formatargspec, getcallargs

from .encoders import BaseEncoder, UTF8Encoder
from .exceptions import Error, ErrorReply, TransactionError, NotConnectedError, ConnectionLostError, NoRunningScriptError, ScriptKilledError
from .replies import BlockingPopReply, DictReply, ListReply, PubSubReply, SetReply, StatusReply, ZRangeReply, ConfigPairReply, InfoReply


from .cursors import Cursor, SetCursor, DictCursor, ZCursor

__all__ = (
    'RedisProtocol',
    'Transaction',
    'Subscription',
    'Script',

    'ZAggregate',
    'ZScoreBoundary',
)

NoneType = type(None)


class ZScoreBoundary:
    """
    Score boundary for a sorted set.
    for queries like zrangebyscore and similar

    :param value: Value for the boundary.
    :type value: float
    :param exclude_boundary: Exclude the boundary.
    :type exclude_boundary: bool
    """
    def __init__(self, value, exclude_boundary=False):
        assert isinstance(value, float) or value in ('+inf', '-inf')
        self.value = value
        self.exclude_boundary = exclude_boundary

    def __repr__(self):
        return 'ZScoreBoundary(value=%r, exclude_boundary=%r)' % (
                    self.value, self.exclude_boundary)

ZScoreBoundary.MIN_VALUE = ZScoreBoundary('-inf')
ZScoreBoundary.MAX_VALUE = ZScoreBoundary('+inf')

class ZAggregate: # TODO: use the Python 3.4 enum type.
    """
    Aggregation method for zinterstore and zunionstore.
    """

    #: Sum aggregation.
    SUM = 'SUM'

    #: Min aggregation.
    MIN = 'MIN'

    #: Max aggregation.
    MAX = 'MAX'


class PipelinedCall:
    """ Track record for call that is being executed in a protocol. """
    __slots__ = ('cmd', 'is_blocking')

    def __init__(self, cmd, is_blocking):
        self.cmd = cmd
        self.is_blocking = is_blocking


class MultiBulkReply:
    """
    Container for a multi bulk reply.
    """
    def __init__(self, protocol, count):
        self.queue = Queue()
        self.protocol = protocol
        self.count = int(count)

    def iter_raw(self):
        """
        Iterate over all multi bulk packets. This yields futures that won't
        decode bytes yet.
        """
        for i in range(self.count):
            yield self.queue.get()

    def __iter__(self):
        """
        Iterate over the reply. This yields coroutines of the decoded packets.
        It decodes bytes automatically using protocol.decode_to_native.
        """
        @asyncio.coroutine
        def auto_decode(f):
            result = yield from f
            if isinstance(result, (StatusReply, int, float, MultiBulkReply)):
                # Note that MultiBulkReplies can be nested. e.g. in the 'scan' operation.
                return result
            elif isinstance(result, bytes):
                return self.protocol.decode_to_native(result)
            else:
                raise AssertionError('Invalid type: %r' % type(result))

        for f in self.iter_raw():
            # We should immediately wrap this coroutine in async(), to be sure
            # that the order of the queue remains, even if we wrap it in
            # gather:
            #    f1 = next(multibulk)
            #    f2 = next(multibulk)
            #    r1, r2 = gather(f1, f2)
            yield asyncio.async(auto_decode(f))

    def __repr__(self):
        return 'MultiBulkReply(protocol=%r, count=%r)' % (self.protocol, self.count)


class _ScanPart:
    """ Internal: result chunk of a scan operation. """
    def __init__(self, new_cursor_pos, items):
        self.new_cursor_pos = new_cursor_pos
        self.items = items


class _PostProcessor:
    @asyncio.coroutine
    def multibulk_as_list(protocol, result):
        assert isinstance(result, MultiBulkReply)
        return ListReply(result)

    @asyncio.coroutine
    def multibulk_as_set(protocol, result):
        assert isinstance(result, MultiBulkReply)
        return SetReply(result)

    @asyncio.coroutine
    def multibulk_as_dict(protocol, result):
        assert isinstance(result, MultiBulkReply)
        return DictReply(result)

    @asyncio.coroutine
    def multibulk_as_zrangeresult(protocol, result):
        assert isinstance(result, MultiBulkReply)
        return ZRangeReply(result)

    @asyncio.coroutine
    def multibulk_as_blocking_pop_reply(protocol, result):
        assert isinstance(result, MultiBulkReply)
        list_name, value = yield from ListReply(result).get_as_list()
        return BlockingPopReply(list_name, value)

    @asyncio.coroutine
    def multibulk_as_configpair(protocol, result):
        assert isinstance(result, MultiBulkReply)
        parameter, value = yield from ListReply(result).get_as_list()
        return ConfigPairReply(parameter, value)

    @asyncio.coroutine
    def multibulk_as_scanpart(protocol, result):
        """
        Process scanpart result.
        This is a multibulk reply of length two, where the first item is the
        new cursor position and the second item is a nested multi bulk reply
        containing all the elements.
        """
        # Get outer multi bulk reply.
        assert isinstance(result, MultiBulkReply)
        new_cursor_pos, items_bulk = yield from ListReply(result).get_as_list()
        assert isinstance(items_bulk, MultiBulkReply)

        # Read all items for scan chunk in memory. This is fine, because it's
        # transmitted in chunks of about 10.
        items = yield from ListReply(items_bulk).get_as_list()
        return _ScanPart(int(new_cursor_pos), items)

    @asyncio.coroutine
    def bytes_to_info(protocol, result):
        assert isinstance(result, bytes)
        return InfoReply(result)

    @asyncio.coroutine
    def int_to_bool(protocol, result):
        assert isinstance(result, int)
        return bool(result) # Convert int to bool

    @asyncio.coroutine
    def bytes_to_native(protocol, result):
        assert isinstance(result, bytes)
        return protocol.decode_to_native(result)

    @asyncio.coroutine
    def bytes_to_str(protocol, result):
        assert isinstance(result, bytes)
        return result.decode('ascii')

    @asyncio.coroutine
    def bytes_to_native_or_none(protocol, result):
        if result is None:
            return result
        else:
            assert isinstance(result, bytes)
            return protocol.decode_to_native(result)

    @asyncio.coroutine
    def bytes_to_float_or_none(protocol, result):
        if result is None:
            return result
        assert isinstance(result, bytes)
        return float(result)

    @asyncio.coroutine
    def bytes_to_float(protocol, result):
        assert isinstance(result, bytes)
        return float(result)


class ListOf:
    def __init__(self, type_):
        self.type = type_


class NativeType:
    """
    Constant which represents the native Python type that's used.
    """
    def __new__(cls):
        raise Exception('NativeType is not meant to be initialized.')


# List of all command methods.
_all_commands = []

def _command(method):
    """
    Wrapper for all redis commands.
    This will also keep track of all the available commands and do type
    checking.
    """
    # Register command.
    _all_commands.append(method.__name__)

    # Read specs
    specs = getfullargspec(method)
    return_type = specs.annotations.get('return', None)
    params = { k:v for k, v in specs.annotations.items() if k != 'return' }

    def get_real_type(protocol, type_):
        # If NativeType was given, replace it with the type of the protocol
        # itself.
        if isinstance(type_, tuple):
            return tuple(get_real_type(protocol, t) for t in type_)

        if type_ == NativeType:
            return protocol.native_type
        elif isinstance(type_, ListOf):
            return (list, types.GeneratorType) # We don't check the content of the list.
        else:
            return type_

    def typecheck_input(protocol, *a, **kw):
        if protocol.enable_typechecking and params:
            for name, value in getcallargs(method, None, *a, **kw).items():
                if name in params:
                    real_type = get_real_type(protocol, params[name])
                    if not isinstance(value, real_type):
                        raise TypeError('RedisProtocol.%s received %r, expected %r' %
                                        (method.__name__, type(value).__name__, real_type))

    def typecheck_return(protocol, result):
        if protocol.enable_typechecking and return_type:
            expected_type = get_real_type(protocol, return_type)
            if not isinstance(result, expected_type):
                raise TypeError('Got unexpected return type %r in RedisProtocol.%s, expected %r' %
                                (type(result).__name__, method.__name__, expected_type))

    # Wrap it into a check which allows this command to be run either directly
    # on the protocol, outside of transactions or from the transaction object.
    @wraps(method)
    def wrapper(self, *a, **kw):
        if not self._is_connected:
            raise NotConnectedError

        # When calling from a transaction, the first arg is the transaction object.
        if self.in_transaction and (len(a) == 0 or a[0] != self._transaction):
            raise Error('Cannot run command inside transaction')
        elif self.in_transaction:
            # In case of a transaction, we receive a Future
            typecheck_input(self, *a[1:], **kw)
            future = yield from method(self, *a[1:], **kw)

            # Typecheck the future when the result is available.
            @future.add_done_callback
            def callback(result):
                typecheck_return(self, result.result())

            return future
        # When calling from a pubsub context
        elif self.in_pubsub and (len(a) == 0 or a[0] != self._subscription):
            raise Error('Cannot run command inside pubsub subscription.')
        elif self.in_pubsub:
            typecheck_input(self, *a[1:], **kw)
            result = yield from method(self, *a[1:], **kw)
            typecheck_return(self, result)
            return result
        else:
            typecheck_input(self, *a, **kw)
            result = yield from method(self, *a, **kw)
            typecheck_return(self, result)
            return result

    # Append the real signature as the first line in the docstring.
    # (This will make the sphinx docs show the real signature instead of
    # (*a, **kw) of the wrapper.)
    # (But don't put the anotations inside the copied signature, that's rather
    # ugly in the docs.)
    signature = formatargspec(* specs[:6])

    # Use function annotations to generate param documentation.

    def get_name(type_):
        """ Turn type annotation into doc string. """
        try:
            return {
                BlockingPopReply: ":class:`BlockingPopReply <asyncio_redis.BlockingPopReply>`",
                ConfigPairReply: ":class:`ConfigPairReply <asyncio_redis.ConfigPairReply>`",
                DictReply: ":class:`DictReply <asyncio_redis.DictReply>`",
                InfoReply: ":class:`InfoReply <asyncio_redis.InfoReply>`",
                ListReply: ":class:`ListReply <asyncio_redis.ListReply>`",
                MultiBulkReply: ":class:`MultiBulkReply <asyncio_redis.MultiBulkReply>`",
                NativeType: "Native Python type, as defined by :attr:`encoder.native_type <asyncio_redis.encoders.BaseEncoder.native_type>`",
                NoneType: "None",
                SetReply: ":class:`SetReply <asyncio_redis.SetReply>`",
                StatusReply: ":class:`StatusReply <asyncio_redis.StatusReply>`",
                ZRangeReply: ":class:`ZRangeReply <asyncio_redis.ZRangeReply>`",
                ZScoreBoundary: ":class:`ZScoreBoundary <asyncio_redis.ZScoreBoundary>`",
                Cursor: ":class:`Cursor <asyncio_redis.cursors.Cursor>`",
                SetCursor: ":class:`SetCursor <asyncio_redis.cursors.SetCursor>`",
                DictCursor: ":class:`DictCursor <asyncio_redis.cursors.DictCursor>`",
                ZCursor: ":class:`ZCursor <asyncio_redis.cursors.ZCursor>`",
                int: 'int',
                bool: 'bool',
                dict: 'dict',
                float: 'float',
                str: 'str',
                bytes: 'bytes',
            }[type_]
        except KeyError:
            if isinstance(type_, ListOf):
                return "List or iterable of %s" % get_name(type_.type)

            elif isinstance(type_, tuple):
                return ' or '.join(get_name(t) for t in type_)
            else:
                raise Exception('Unknown annotation %s' % type_.__name__)
                #return "``%s``" % type_.__name__

    def get_param(k, v):
        return ':param %s: %s\n' % (k, get_name(v))

    params_str = [ get_param(k, v) for k, v in params.items() ]
    returns = ':returns: (Future of) %s\n' % get_name(return_type) if return_type else ''

    wrapper.__doc__ = '%s%s\n%s\n\n%s%s' % (
            method.__name__, signature,
            method.__doc__,
            ''.join(params_str),
            returns
            )

    return wrapper


class RedisProtocol(asyncio.Protocol):
    """
    The Redis Protocol implementation.

    ::

        self.loop = asyncio.get_event_loop()
        transport, protocol = yield from loop.create_connection(RedisProtocol, 'localhost', 6379)

    :param password: Redis database password
    :type password: bytes
    :param encoder: Encoder to use for encoding to or decoding from redis bytes to a native type.
    :type encoder: :class:`asyncio_redis.encoders.BaseEncoder` instance.
    :param db: Redis database
    :type db: int
    :param enable_typechecking: When ``True``, check argument types for all
                                redis commands. Normally you want to have this
                                enabled.
    :type enable_typechecking: bool
    """
    def __init__(self, password=None, db=0, encoder=None, connection_lost_callback=None, enable_typechecking=True):
        if encoder is None:
            encoder = UTF8Encoder()

        assert isinstance(db, int)
        assert isinstance(encoder, BaseEncoder)
        assert encoder.native_type, 'Encoder.native_type not defined'
        assert not password or isinstance(password, encoder.native_type)

        self.password = password
        self.db = db
        self._connection_lost_callback = connection_lost_callback

        # Take encode / decode settings from encoder
        self.encode_from_native = encoder.encode_from_native
        self.decode_to_native = encoder.decode_to_native
        self.native_type = encoder.native_type
        self.enable_typechecking = enable_typechecking

        self.transport = None
        self._queue = deque() # Input parser queues
        self._messages_queue = None # Pubsub queue
        self._is_connected = False # True as long as the underlying transport is connected.

        # Pubsub state
        self._in_pubsub = False
        self._subscription = None
        self._pubsub_channels = set() # Set of channels
        self._pubsub_patterns = set() # Set of patterns

        # Transaction related stuff.
        self._in_transaction = False
        self._transaction = None
        self._transaction_response_queue = None # Transaction answer queue

        self._line_received_handlers = {
            b'+': self._handle_status_reply,
            b'-': self._handle_error_reply,
            b'$': self._handle_bulk_reply,
            b'*': self._handle_multi_bulk_reply,
            b':': self._handle_int_reply,
        }

    def connection_made(self, transport):
        self.transport = transport
        self._is_connected = True
        logger.log(logging.INFO, 'Redis connection made')

        # Pipelined calls
        self._pipelined_calls = set() # Set of all the pipelined calls.

        # Start parsing reader stream.
        self._reader = StreamReader()
        self._reader.set_transport(transport)
        self._reader_f = asyncio.async(self._reader_coroutine())

        @asyncio.coroutine
        def initialize():
            # If a password or database was been given, first connect to that one.
            if self.password:
                yield from self.auth(self.password)

            if self.db:
                yield from self.select(self.db)

            #  If we are in pubsub mode, send channel subscriptions again.
            if self._in_pubsub:
                if self._pubsub_channels:
                    yield from self._subscribe(self._subscription, list(self._pubsub_channels)) # TODO: unittest this

                if self._pubsub_patterns:
                    yield from self._psubscribe(self._subscription, list(self._pubsub_patterns))

        asyncio.Task(initialize())

    def data_received(self, data):
        """ Process data received from Redis server.  """
        self._reader.feed_data(data)

    def _encode_int(self, value:int) -> bytes:
        """ Encodes an integer to bytes. (always ascii) """
        return str(value).encode('ascii')

    def _encode_float(self, value:float) -> bytes:
        """ Encodes a float to bytes. (always ascii) """
        return str(value).encode('ascii')

    def _encode_zscore_boundary(self, value:ZScoreBoundary) -> str:
        """ Encodes a zscore boundary. (always ascii) """
        if isinstance(value.value, str):
            return str(value.value).encode('ascii') # +inf and -inf
        elif value.exclude_boundary:
            return str("(%f" % value.value).encode('ascii')
        else:
            return str("%f" % value.value).encode('ascii')

    def eof_received(self):
        logger.log(logging.INFO, 'EOF received in RedisProtocol')
        self._reader.feed_eof()

    def connection_lost(self, exc):
        if exc is None:
            self._reader.feed_eof()
        else:
            self._reader.set_exception(exc)

        if self._reader_f:
            self._reader_f.cancel()

        self._is_connected = False
        self.transport = None
        self._reader = None
        self._reader_f = None

        # Raise exception on all waiting futures.
        while self._queue:
            f = self._queue.popleft()
            f.set_exception(ConnectionLostError(exc))

        logger.log(logging.INFO, 'Redis connection lost')

        # Call connection_lost callback
        if self._connection_lost_callback:
            self._connection_lost_callback()

    # Request state

    @property
    def in_blocking_call(self):
        """ True when waiting for answer to blocking command. """
        return any(c.is_blocking for c in self._pipelined_calls)

    @property
    def in_pubsub(self):
        """ True when the protocol is in pubsub mode. """
        return self._in_pubsub

    @property
    def in_transaction(self):
        """ True when we're inside a transaction. """
        return self._in_transaction

    @property
    def in_use(self):
        """ True when this protocol is in use. """
        return self.in_blocking_call or self.in_pubsub or self.in_transaction

    @property
    def is_connected(self):
        """ True when the underlying transport is connected. """
        return self._is_connected

    # Handle replies

    @asyncio.coroutine
    def _reader_coroutine(self):
        """
        Coroutine which reads input from the stream reader and processes it.
        """
        def item_callback(item):
            f = self._queue.popleft()

            if isinstance(item, Exception):
                f.set_exception(item)
            else:
                f.set_result(item)

        while True:
            try:
                yield from self._handle_item(item_callback)
            except ConnectionLostError:
                return

    @asyncio.coroutine
    def _handle_item(self, cb):
        c = yield from self._reader.read(1)
        yield from self._line_received_handlers[c](cb)

    @asyncio.coroutine
    def _handle_status_reply(self, cb):
        line = (yield from self._reader.readline()).rstrip(b'\r\n')
        cb(StatusReply(line.decode('ascii')))

    @asyncio.coroutine
    def _handle_int_reply(self, cb):
        line = (yield from self._reader.readline()).rstrip(b'\r\n')
        cb(int(line))

    @asyncio.coroutine
    def _handle_error_reply(self, cb):
        line = (yield from self._reader.readline()).rstrip(b'\r\n')
        cb(ErrorReply(line.decode('ascii')))

    @asyncio.coroutine
    def _handle_bulk_reply(self, cb):
        length = int((yield from self._reader.readline()).rstrip(b'\r\n'))
        if length == -1:
            # None bulk reply
            cb(None)
        else:
            # Read data
            data = yield from self._reader.read(length)
            cb(data)

            # Ignore trailing newline.
            remaining = yield from self._reader.readline()
            assert remaining.rstrip(b'\r\n') == b''

    @asyncio.coroutine
    def _handle_multi_bulk_reply(self, cb):
                # NOTE: the reason for passing the callback `cb` in here is
                #       mainly because we want to return the result object
                #       especially in this case before the input is read
                #       completely. This allows a streaming API.
        count = int((yield from self._reader.readline()).rstrip(b'\r\n'))

        # Handle multi-bulk none.
        # (Used when a transaction exec fails.)
        if count == -1:
            cb(None)
            return

        reply = MultiBulkReply(self, count)

        # Return the empty queue immediately as an answer.
        if self._in_pubsub:
            asyncio.async(self._handle_pubsub_multibulk_reply(reply))
        else:
            cb(reply)

        # Wait for all multi bulk reply content.
        for i in range(count):
            yield from self._handle_item(reply.queue.put_nowait)

    @asyncio.coroutine
    def _handle_pubsub_multibulk_reply(self, multibulk_reply):
        result = yield from ListReply(multibulk_reply).get_as_list()
        assert result[0] in ('message', 'subscribe', 'unsubscribe', 'psubscribe', 'punsubscribe')

        if result[0] == 'message':
            channel, value = result[1], result[2]
            yield from self._subscription._messages_queue.put(PubSubReply(channel, value))
        else:
            # In case of 'subscribe'/'unsubscribe', we already converted the
            # MultiBulkReply to a list, so we just hand over the list.
            self._push_answer(result)

    # Redis operations.

    def _send_command(self, args):
        """
        Send Redis request command.
        `args` should be a list of bytes to be written to the transport.
        """
        # Create write buffer.
        data = []

        # NOTE: First, I tried to optimize by also flushing this buffer in
        # between the looping through the args. However, I removed that as the
        # advantage was really small. Even when some commands like `hmset`
        # could accept a generator instead of a list/dict, we would need to
        # read out the whole generator in memory in order to write the number
        # of arguments first.

        # Serialize and write header (number of arguments.)
        data.append((u'*%i\r\n' % len(args)).encode('ascii'))

        # Write arguments.
        for arg in args:
            data += [ (u'$%i\r\n' % len(arg)).encode('ascii'), arg, b'\r\n' ]

        # Flush the last part
        self.transport.write(b''.join(data))

    @asyncio.coroutine
    def _get_answer(self, answer_f, _bypass=False, post_process_func=None, call=None):
        """
        Return an answer to the pipelined query.
        (Or when we are in a transaction, return a future for the answer.)
        """
        # Wait for the answer to come in
        result = yield from answer_f

        if self._in_transaction and not _bypass:
            # When the connection is inside a transaction, the query will be queued.
            if result != StatusReply('QUEUED'):
                raise Error('Expected to receive QUEUED for query in transaction, received %r.' % result)

            # Return a future which will contain the result when it arrives.
            f = Future()
            self._transaction_response_queue.append( (f, post_process_func, call) )
            return f
        else:
            if post_process_func:
                result = yield from post_process_func(self, result)
            if call:
                self._pipelined_calls.remove(call)
            return result

    def _push_answer(self, answer):
        """
        Answer future at the queue.
        """
        f = self._queue.popleft()

        if isinstance(answer, Exception):
            f.set_exception(answer)
        else:
            f.set_result(answer)

    @asyncio.coroutine
    def _query(self, *args, _bypass=False, post_process_func=None, set_blocking=False):
        """ Wrapper around both _send_command and _get_answer. """
        call = PipelinedCall(args[0], set_blocking)
        self._pipelined_calls.add(call)

        # Add a new future to our answer queue.
        answer_f = Future()
        self._queue.append(answer_f)

        # Send command
        self._send_command(args)

        # Receive answer.
        result = yield from self._get_answer(answer_f, _bypass=_bypass, post_process_func=post_process_func, call=call)
        return result

    # Internal

    @_command
    def auth(self, password:NativeType) -> StatusReply:
        """ Authenticate to the server """
        self.password = password
        return self._query(b'auth', self.encode_from_native(password))

    @_command
    def select(self, db:int) -> StatusReply:
        """ Change the selected database for the current connection """
        self.db = db
        return self._query(b'select', self._encode_int(db))

    # Strings

    @_command
    def set(self, key:NativeType, value:NativeType) -> StatusReply:
        """ Set the string value of a key """
        return self._query(b'set', self.encode_from_native(key), self.encode_from_native(value))

    @_command
    def setex(self, key:NativeType, seconds:int, value:NativeType) -> StatusReply:
        """ Set the string value of a key with expire """
        return self._query(b'setex', self.encode_from_native(key),
                           self._encode_int(seconds), self.encode_from_native(value))

    @_command
    def setnx(self, key:NativeType, value:NativeType) -> bool:
        """ Set the string value of a key if it does not exist.
        Returns True if value is successfully set """
        return self._query(b'setnx', self.encode_from_native(key), self.encode_from_native(value),
                           post_process_func=_PostProcessor.int_to_bool)

    @_command
    def get(self, key:NativeType) -> (NativeType, NoneType):
        """ Get the value of a key """
        return self._query(b'get', self.encode_from_native(key),
                post_process_func=_PostProcessor.bytes_to_native_or_none)

    @_command
    def mget(self, keys:ListOf(NativeType)) -> ListReply:
        """ Returns the values of all specified keys. """
        return self._query(b'mget', *map(self.encode_from_native, keys),
                post_process_func=_PostProcessor.multibulk_as_list)

    @_command
    def strlen(self, key:NativeType) -> int:
        """ Returns the length of the string value stored at key. An error is
        returned when key holds a non-string value.  """
        return self._query(b'strlen', self.encode_from_native(key))

    @_command
    def append(self, key:NativeType, value:NativeType) -> int:
        """ Append a value to a key """
        return self._query(b'append', self.encode_from_native(key), self.encode_from_native(value))

    @_command
    def getset(self, key:NativeType, value:NativeType) -> (NativeType, NoneType):
        """ Set the string value of a key and return its old value """
        return self._query(b'getset', self.encode_from_native(key), self.encode_from_native(value),
                post_process_func=_PostProcessor.bytes_to_native_or_none)

    @_command
    def incr(self, key:NativeType) -> int:
        """ Increment the integer value of a key by one """
        return self._query(b'incr', self.encode_from_native(key))

    @_command
    def incrby(self, key:NativeType, increment:int) -> int:
        """ Increment the integer value of a key by the given amount """
        return self._query(b'incrby', self.encode_from_native(key), self._encode_int(increment))

    @_command
    def decr(self, key:NativeType) -> int:
        """ Decrement the integer value of a key by one """
        return self._query(b'decr', self.encode_from_native(key))

    @_command
    def decrby(self, key:NativeType, increment:int) -> int:
        """ Decrement the integer value of a key by the given number """
        return self._query(b'decrby', self.encode_from_native(key), self._encode_int(increment))

    @_command
    def randomkey(self) -> NativeType:
        """ Return a random key from the keyspace """
        return self._query(b'randomkey', post_process_func=_PostProcessor.bytes_to_native)

    @_command
    def exists(self, key:NativeType) -> bool:
        """ Determine if a key exists """
        return self._query(b'exists', self.encode_from_native(key),
                post_process_func=_PostProcessor.int_to_bool)

    @_command
    def delete(self, keys:ListOf(NativeType)) -> int:
        """ Delete a key """
        return self._query(b'del', *map(self.encode_from_native, keys))

    @_command
    def move(self, key:NativeType, database:int) -> int:
        """ Move a key to another database """
        return self._query(b'move', self.encode_from_native(key), self.encode_from_native(destination)) # TODO: test

    @_command
    def rename(self, key:NativeType, newkey:NativeType) -> StatusReply:
        """ Rename a key """
        return self._query(b'rename', self.encode_from_native(key), self.encode_from_native(newkey))

    @_command
    def renamenx(self, key:NativeType, newkey:NativeType) -> int:
        """ Rename a key, only if the new key does not exist
        (Returns 1 if the key was successfully renamed.) """
        return self._query(b'renamenx', self.encode_from_native(key), self.encode_from_native(newkey))

    @_command
    def getbit(self, key:NativeType, offset):
        """ Returns the bit value at offset in the string value stored at key """
        raise NotImplementedError

    @_command
    def bitop_and(self, destkey:NativeType, srckeys:ListOf(NativeType)) -> int:
        """ Perform a bitwise AND operation between multiple keys. """
        return self._bitop(b'and', destkey, srckeys)

    @_command
    def bitop_or(self, destkey:NativeType, srckeys:ListOf(NativeType)) -> int:
        """ Perform a bitwise OR operation between multiple keys. """
        return self._bitop(b'or', destkey, srckeys)

    @_command
    def bitop_xor(self, destkey:NativeType, srckeys:ListOf(NativeType)) -> int:
        """ Perform a bitwise XOR operation between multiple keys. """
        return self._bitop(b'xor', destkey, srckeys)

    def _bitop(self, op, destkey, srckeys):
        return self._query(b'bitop', op, self.encode_from_native(destkey), *map(self.encode_from_native, srckeys))

    @_command
    def bitop_not(self, destkey:NativeType, key:NativeType) -> int:
        """ Perform a bitwise NOT operation between multiple keys. """
        return self._query(b'bitop', b'not', self.encode_from_native(destkey), self.encode_from_native(key))

    @_command
    def bitcount(self, key:NativeType, start:int=0, end:int=-1):
        """ Count the number of set bits (population counting) in a string. """
        return self._query(b'bitcount', self.encode_from_native(key), self._encode_int(start), self._encode_int(end))

    @_command
    def getbit(self, key:NativeType, offset:int) -> bool:
        """ Returns the bit value at offset in the string value stored at key """
        return self._query(b'getbit', self.encode_from_native(key), self._encode_int(offset),
                     post_process_func=_PostProcessor.int_to_bool)

    @_command
    def setbit(self, key:NativeType, offset:int, value:bool) -> bool:
        """ Sets or clears the bit at offset in the string value stored at key """
        return self._query(b'setbit', self.encode_from_native(key), self._encode_int(offset),
                    self._encode_int(int(value)), post_process_func=_PostProcessor.int_to_bool)

    # Keys

    @_command
    def keys(self, pattern:NativeType) -> ListReply:
        """
        Find all keys matching the given pattern.

        .. note:: Also take a look at :func:`~asyncio_redis.RedisProtocol.scan`.
        """
        return self._query(b'keys', self.encode_from_native(pattern),
                post_process_func=_PostProcessor.multibulk_as_list)

    @_command
    def dump(self, key:NativeType):
        """ Return a serialized version of the value stored at the specified key. """
        # Dump does not work yet. It shouldn't be decoded using utf-8'
        raise NotImplementedError('Not supported.')

    @_command
    def expire(self, key:NativeType, seconds:int) -> int:
        """ Set a key's time to live in seconds """
        return self._query(b'expire', self.encode_from_native(key), self._encode_int(seconds))

    @_command
    def pexpire(self, key:NativeType, milliseconds:int) -> int:
        """ Set a key's time to live in milliseconds """
        return self._query(b'pexpire', self.encode_from_native(key), self._encode_int(milliseconds))

    @_command
    def expireat(self, key:NativeType, timestamp:int) -> int:
        """ Set the expiration for a key as a UNIX timestamp """
        return self._query(b'expireat', self.encode_from_native(key), self._encode_int(timestamp))

    @_command
    def pexpireat(self, key:NativeType, milliseconds_timestamp:int) -> int:
        """ Set the expiration for a key as a UNIX timestamp specified in milliseconds """
        return self._query(b'pexpireat', self.encode_from_native(key), self._encode_int(milliseconds_timestamp))

    @_command
    def persist(self, key:NativeType) -> int:
        """ Remove the expiration from a key """
        return self._query(b'persist', self.encode_from_native(key))

    @_command
    def ttl(self, key:NativeType) -> int:
        """ Get the time to live for a key """
        return self._query(b'ttl', self.encode_from_native(key))

    @_command
    def pttl(self, key:NativeType) -> int:
        """ Get the time to live for a key in milliseconds """
        return self._query(b'pttl', self.encode_from_native(key))

    # Set operations

    @_command
    def sadd(self, key:NativeType, members:ListOf(NativeType)) -> int:
        """ Add one or more members to a set """
        return self._query(b'sadd', self.encode_from_native(key), *map(self.encode_from_native, members))

    @_command
    def srem(self, key:NativeType, members:ListOf(NativeType)) -> int:
        """ Remove one or more members from a set """
        return self._query(b'srem', self.encode_from_native(key), *map(self.encode_from_native, members))

    @_command
    def spop(self, key:NativeType) -> NativeType:
        """ Removes and returns a random element from the set value stored at key. """
        return self._query(b'spop', self.encode_from_native(key), post_process_func=_PostProcessor.bytes_to_native)

    @_command
    def srandmember(self, key:NativeType, count:int=1) -> SetReply:
        """ Get one or multiple random members from a set
        (Returns a list of members, even when count==1) """
        return self._query(b'srandmember', self.encode_from_native(key), self._encode_int(count),
                post_process_func=_PostProcessor.multibulk_as_set)

    @_command
    def sismember(self, key:NativeType, value:NativeType) -> bool:
        """ Determine if a given value is a member of a set """
        return self._query(b'sismember', self.encode_from_native(key), self.encode_from_native(value),
                post_process_func=_PostProcessor.int_to_bool)

    @_command
    def scard(self, key:NativeType) -> int:
        """ Get the number of members in a set """
        return self._query(b'scard', self.encode_from_native(key))

    @_command
    def smembers(self, key:NativeType) -> SetReply:
        """ Get all the members in a set """
        return self._query(b'smembers', self.encode_from_native(key),
                post_process_func=_PostProcessor.multibulk_as_set)

    @_command
    def sinter(self, keys:ListOf(NativeType)) -> SetReply:
        """ Intersect multiple sets """
        return self._query(b'sinter', *map(self.encode_from_native, keys),
                post_process_func=_PostProcessor.multibulk_as_set)

    @_command
    def sinterstore(self, destination:NativeType, keys:ListOf(NativeType)) -> int:
        """ Intersect multiple sets and store the resulting set in a key """
        return self._query(b'sinterstore', self.encode_from_native(destination), *map(self.encode_from_native, keys))

    @_command
    def sdiff(self, keys:ListOf(NativeType)) -> SetReply:
        """ Subtract multiple sets """
        return self._query(b'sdiff', *map(self.encode_from_native, keys),
                post_process_func=_PostProcessor.multibulk_as_set)

    @_command
    def sdiffstore(self, destination:NativeType, keys:ListOf(NativeType)) -> int:
        """ Subtract multiple sets and store the resulting set in a key """
        return self._query(b'sdiffstore', self.encode_from_native(destination),
                *map(self.encode_from_native, keys))

    @_command
    def sunion(self, keys:ListOf(NativeType)) -> SetReply:
        """ Add multiple sets """
        return self._query(b'sunion', *map(self.encode_from_native, keys),
                post_process_func=_PostProcessor.multibulk_as_set)

    @_command
    def sunionstore(self, destination:NativeType, keys:ListOf(NativeType)) -> int:
        """ Add multiple sets and store the resulting set in a key """
        return self._query(b'sunionstore', self.encode_from_native(destination), *map(self.encode_from_native, keys))

    @_command
    def smove(self, source:NativeType, destination:NativeType, value:NativeType) -> int:
        """ Move a member from one set to another """
        return self._query(b'smove', self.encode_from_native(source), self.encode_from_native(destination), self.encode_from_native(value))

    # List operations

    @_command
    def lpush(self, key:NativeType, values:ListOf(NativeType)) -> int:
        """ Prepend one or multiple values to a list """
        return self._query(b'lpush', self.encode_from_native(key), *map(self.encode_from_native, values))

    @_command
    def lpushx(self, key:NativeType, value:NativeType) -> int:
        """ Prepend a value to a list, only if the list exists """
        return self._query(b'lpushx', self.encode_from_native(key), self.encode_from_native(value))

    @_command
    def rpush(self, key:NativeType, values:ListOf(NativeType)) -> int:
        """ Append one or multiple values to a list """
        return self._query(b'rpush', self.encode_from_native(key), *map(self.encode_from_native, values))

    @_command
    def rpushx(self, key:NativeType, value:NativeType) -> int:
        """ Append a value to a list, only if the list exists """
        return self._query(b'rpushx', self.encode_from_native(key), self.encode_from_native(value))

    @_command
    def llen(self, key:NativeType) -> int:
        """ Returns the length of the list stored at key. """
        return self._query(b'llen', self.encode_from_native(key))

    @_command
    def lrem(self, key:NativeType, count:int=0, value='') -> int:
        """ Remove elements from a list """
        return self._query(b'lrem', self.encode_from_native(key), self._encode_int(count), self.encode_from_native(value))

    @_command
    def lrange(self, key, start:int=0, stop:int=-1) -> ListReply:
        """ Get a range of elements from a list. """
        return self._query(b'lrange', self.encode_from_native(key), self._encode_int(start), self._encode_int(stop),
                post_process_func=_PostProcessor.multibulk_as_list)

    @_command
    def ltrim(self, key:NativeType, start:int=0, stop:int=-1) -> StatusReply:
        """ Trim a list to the specified range """
        return self._query(b'ltrim', self.encode_from_native(key), self._encode_int(start), self._encode_int(stop))

    @_command
    def lpop(self, key:NativeType) -> (NativeType, NoneType):
        """ Remove and get the first element in a list """
        return self._query(b'lpop', self.encode_from_native(key),
                post_process_func=_PostProcessor.bytes_to_native_or_none)

    @_command
    def rpop(self, key:NativeType) -> (NativeType, NoneType):
        """ Remove and get the last element in a list """
        return self._query(b'rpop', self.encode_from_native(key),
                post_process_func=_PostProcessor.bytes_to_native_or_none)

    @_command
    def rpoplpush(self, source:NativeType, destination:NativeType) -> NativeType:
        """ Remove the last element in a list, append it to another list and return it """
        return self._query(b'rpoplpush', self.encode_from_native(source), self.encode_from_native(destination),
                    post_process_func=_PostProcessor.bytes_to_native)

    @_command
    def lindex(self, key:NativeType, index:int) -> (NativeType, NoneType):
        """ Get an element from a list by its index """
        return self._query(b'lindex', self.encode_from_native(key), self._encode_int(index),
                post_process_func=_PostProcessor.bytes_to_native_or_none)

    @_command
    def blpop(self, keys:ListOf(NativeType), timeout:int=0) -> BlockingPopReply:
        """ Remove and get the first element in a list, or block until one is available. """
        return self._blocking_pop(keys, timeout=timeout, right=False)

    @_command
    def brpop(self, keys:ListOf(NativeType), timeout:int=0) -> BlockingPopReply:
        """ Remove and get the last element in a list, or block until one is available. """
        return self._blocking_pop(keys, timeout=timeout, right=True)

    @_command
    def brpoplpush(self, source:NativeType, destination:NativeType, timeout:int=0) -> NativeType:
        """ Pop a value from a list, push it to another list and return it; or block until one is available """
        return self._query(b'brpoplpush', self.encode_from_native(source), self.encode_from_native(destination),
                    self._encode_int(timeout), set_blocking=True, post_process_func=_PostProcessor.bytes_to_native)

    def _blocking_pop(self, keys, timeout:int=0, right:bool=False):
        command = b'brpop' if right else b'blpop'
        return self._query(command, *([ self.encode_from_native(k) for k in keys ] + [self._encode_int(timeout)]),
                post_process_func=_PostProcessor.multibulk_as_blocking_pop_reply, set_blocking=True)

    @_command
    def lset(self, key:NativeType, index:int, value:NativeType) -> StatusReply:
        """ Set the value of an element in a list by its index. """
        return self._query(b'lset', self.encode_from_native(key), self._encode_int(index), self.encode_from_native(value))

    @_command
    def linsert(self, key:NativeType, pivot:NativeType, value:NativeType, before=False) -> int:
        """ Insert an element before or after another element in a list """
        return self._query(b'linsert', self.encode_from_native(key), (b'BEFORE' if before else b'AFTER'),
                self.encode_from_native(pivot), self.encode_from_native(value))

    # Sorted Sets

    @_command
    def zadd(self, key:NativeType, values:dict) -> int:
        """
        Add one or more members to a sorted set, or update its score if it already exists

        ::

            yield protocol.zadd('myzset', { 'key': 4, 'key2': 5 })
        """
        data = [ ]
        for k,score in values.items():
            assert isinstance(k, self.native_type)
            assert isinstance(score, (int, float))

            data.append(self._encode_float(score))
            data.append(self.encode_from_native(k))

        return self._query(b'zadd', self.encode_from_native(key), *data)

    @_command
    def zrange(self, key:NativeType, start:int=0, stop:int=-1) -> ZRangeReply:
        """
        Return a range of members in a sorted set, by index.

        You can do the following to receive the slice of the sorted set as a
        python dict (mapping the keys to their scores):

        ::

            result = yield protocol.zrange('myzset', start=10, stop=20)
            my_dict = yield result.get_as_dict()

        or the following to retrieve it as a list of keys:

        ::

            result = yield protocol.zrange('myzset', start=10, stop=20)
            my_dict = yield result.get_as_list()
        """
        return self._query(b'zrange', self.encode_from_native(key),
                    self._encode_int(start), self._encode_int(stop), b'withscores',
                    post_process_func=_PostProcessor.multibulk_as_zrangeresult)

    @_command
    def zrangebyscore(self, key:NativeType,
                min:ZScoreBoundary=ZScoreBoundary.MIN_VALUE,
                max:ZScoreBoundary=ZScoreBoundary.MAX_VALUE) -> ZRangeReply:
        """ Return a range of members in a sorted set, by score """
        return self._query(b'zrangebyscore', self.encode_from_native(key),
                    self._encode_zscore_boundary(min), self._encode_zscore_boundary(max),
                    b'withscores',
                    post_process_func=_PostProcessor.multibulk_as_zrangeresult)

    @_command
    def zrevrangebyscore(self, key:NativeType,
                max:ZScoreBoundary=ZScoreBoundary.MAX_VALUE,
                min:ZScoreBoundary=ZScoreBoundary.MIN_VALUE) -> ZRangeReply:
        """ Return a range of members in a sorted set, by score, with scores ordered from high to low """
        return self._query(b'zrevrangebyscore', self.encode_from_native(key),
                    self._encode_zscore_boundary(max), self._encode_zscore_boundary(min),
                    b'withscores',
                    post_process_func=_PostProcessor.multibulk_as_zrangeresult)

    @_command
    def zremrangebyscore(self, key:NativeType,
                min:ZScoreBoundary=ZScoreBoundary.MIN_VALUE,
                max:ZScoreBoundary=ZScoreBoundary.MAX_VALUE) -> int:
        """ Remove all members in a sorted set within the given scores """
        return self._query(b'zremrangebyscore', self.encode_from_native(key),
                    self._encode_zscore_boundary(min), self._encode_zscore_boundary(max))

    @_command
    def zremrangebyrank(self, key:NativeType, min:int=0, max:int=-1) -> int:
        """ Remove all members in a sorted set within the given indexes """
        return self._query(b'zremrangebyrank', self.encode_from_native(key),
                    self._encode_int(min), self._encode_int(max))

    @_command
    def zcount(self, key:NativeType, min:ZScoreBoundary, max:ZScoreBoundary) -> int:
        """ Count the members in a sorted set with scores within the given values """
        return self._query(b'zcount', self.encode_from_native(key),
                    self._encode_zscore_boundary(min), self._encode_zscore_boundary(max))

    @_command
    def zscore(self, key:NativeType, member:NativeType) -> (float, NoneType):
        """ Get the score associated with the given member in a sorted set """
        return self._query(b'zscore', self.encode_from_native(key), self.encode_from_native(member),
                           post_process_func=_PostProcessor.bytes_to_float_or_none)

    @_command
    def zunionstore(self, destination:NativeType, keys:ListOf(NativeType), weights:(NoneType,ListOf(float))=None,
                                    aggregate=ZAggregate.SUM) -> int:
        """ Add multiple sorted sets and store the resulting sorted set in a new key """
        return self._zstore(b'zunionstore', destination, keys, weights, aggregate)

    @_command
    def zinterstore(self, destination:NativeType, keys:ListOf(NativeType), weights:(NoneType,ListOf(float))=None,
                                    aggregate=ZAggregate.SUM) -> int:
        """ Intersect multiple sorted sets and store the resulting sorted set in a new key """
        return self._zstore(b'zinterstore', destination, keys, weights, aggregate)

    def _zstore(self, command, destination, keys, weights, aggregate):
        """ Common part for zunionstore and zinterstore. """
        numkeys = len(keys)
        if weights is None:
            weights = [1] * numkeys

        return self._query(*
                [ command, self.encode_from_native(destination), self._encode_int(numkeys) ] +
                list(map(self.encode_from_native, keys)) +
                [ b'weights' ] +
                list(map(self._encode_float, weights)) +
                [ b'aggregate' ] +
                [ {
                        ZAggregate.SUM: b'SUM',
                        ZAggregate.MIN: b'MIN',
                        ZAggregate.MAX: b'MAX' }[aggregate]
                ] )

    @_command
    def zcard(self, key:NativeType) -> int:
        """ Get the number of members in a sorted set """
        return self._query(b'zcard', self.encode_from_native(key))

    @_command
    def zrank(self, key:NativeType, member:NativeType) -> (int, NoneType):
        """ Determine the index of a member in a sorted set """
        return self._query(b'zrank', self.encode_from_native(key), self.encode_from_native(member))

    @_command
    def zrevrank(self, key:NativeType, member:NativeType) -> (int, NoneType):
        """ Determine the index of a member in a sorted set, with scores ordered from high to low """
        return self._query(b'zrevrank', self.encode_from_native(key), self.encode_from_native(member))

    @_command
    def zincrby(self, key:NativeType, increment:float, member:NativeType) -> float:
        """ Increment the score of a member in a sorted set """
        return self._query(b'zincrby', self.encode_from_native(key),
                    self._encode_float(increment), self.encode_from_native(member),
                    post_process_func=_PostProcessor.bytes_to_float)

    @_command
    def zrem(self, key:NativeType, members:ListOf(NativeType)) -> int:
        """ Remove one or more members from a sorted set """
        return self._query(b'zrem', self.encode_from_native(key), *map(self.encode_from_native, members))

    # Hashes

    @_command
    def hset(self, key:NativeType, field:NativeType, value:NativeType) -> int:
        """ Set the string value of a hash field """
        return self._query(b'hset', self.encode_from_native(key), self.encode_from_native(field), self.encode_from_native(value))

    @_command
    def hmset(self, key:NativeType, values:dict) -> StatusReply:
        """ Set multiple hash fields to multiple values """
        data = [ ]
        for k,v in values.items():
            assert isinstance(k, self.native_type)
            assert isinstance(v, self.native_type)

            data.append(self.encode_from_native(k))
            data.append(self.encode_from_native(v))

        return self._query(b'hmset', self.encode_from_native(key), *data)

    @_command
    def hsetnx(self, key:NativeType, field:NativeType, value:NativeType) -> int:
        """ Set the value of a hash field, only if the field does not exist """
        return self._query(b'hsetnx', self.encode_from_native(key), self.encode_from_native(field), self.encode_from_native(value))

    @_command
    def hdel(self, key:NativeType, fields:ListOf(NativeType)) -> int:
        """ Delete one or more hash fields """
        return self._query(b'hdel', self.encode_from_native(key), *map(self.encode_from_native, fields))

    @_command
    def hget(self, key:NativeType, field:NativeType) -> (NativeType, NoneType):
        """ Get the value of a hash field """
        return self._query(b'hget', self.encode_from_native(key), self.encode_from_native(field),
                post_process_func=_PostProcessor.bytes_to_native_or_none)

    @_command
    def hexists(self, key:NativeType, field:NativeType) -> bool:
        """ Returns if field is an existing field in the hash stored at key. """
        return self._query(b'hexists', self.encode_from_native(key), self.encode_from_native(field),
                post_process_func=_PostProcessor.int_to_bool)

    @_command
    def hkeys(self, key:NativeType) -> SetReply:
        """ Get all the keys in a hash. (Returns a set) """
        return self._query(b'hkeys', self.encode_from_native(key),
                post_process_func=_PostProcessor.multibulk_as_set)

    @_command
    def hvals(self, key:NativeType) -> ListReply:
        """ Get all the values in a hash. (Returns a list) """
        return self._query(b'hvals', self.encode_from_native(key),
                post_process_func=_PostProcessor.multibulk_as_list)

    @_command
    def hlen(self, key:NativeType) -> int:
        """ Returns the number of fields contained in the hash stored at key. """
        return self._query(b'hlen', self.encode_from_native(key))

    @_command
    def hgetall(self, key:NativeType) -> DictReply:
        """ Get the value of a hash field """
        return self._query(b'hgetall', self.encode_from_native(key),
                    post_process_func=_PostProcessor.multibulk_as_dict)

    @_command
    def hmget(self, key:NativeType, fields:ListOf(NativeType)) -> ListReply:
        """ Get the values of all the given hash fields """
        return self._query(b'hmget', self.encode_from_native(key), *map(self.encode_from_native, fields),
                post_process_func=_PostProcessor.multibulk_as_list)

    @_command
    def hincrby(self, key:NativeType, field:NativeType, increment) -> int:
        """ Increment the integer value of a hash field by the given number
        Returns: the value at field after the increment operation. """
        assert isinstance(increment, int)
        return self._query(b'hincrby', self.encode_from_native(key), self.encode_from_native(field), self._encode_int(increment))

    @_command
    def hincrbyfloat(self, key:NativeType, field:NativeType, increment:(int,float)) -> float:
        """ Increment the float value of a hash field by the given amount
        Returns: the value at field after the increment operation. """
        return self._query(b'hincrbyfloat', self.encode_from_native(key), self.encode_from_native(field), self._encode_float(increment),
                post_process_func=_PostProcessor.bytes_to_float)

    # Pubsub
    # (subscribe, unsubscribe, etc... should be called through the Subscription class.)

    @_command
    @asyncio.coroutine
    def start_subscribe(self): # -> Subscription:
        """
        Start a pubsub listener.

        ::

            # Create subscription
            subscription = yield from protocol.start_subscribe()
            yield from subscription.subscribe(['key'])
            yield from subscription.psubscribe(['pattern*'])

            while True:
                result = yield from subscription.get_next_published()
                print(result)

        :returns: :class:`asyncio_redis.Subscription`
        """
        if self.in_use:
            raise Error('Cannot start pubsub listener when a protocol is in use.')

        subscription = Subscription(self)

        self._in_pubsub = True
        self._subscription = subscription
        return subscription

    @_command
    def _subscribe(self, channels:ListOf(NativeType)):
        """ Listen for messages published to the given channels """
        self._pubsub_channels |= set(channels)
        return self._pubsub_method('subscribe', channels)

    @_command
    def _unsubscribe(self, channels:ListOf(NativeType)): # TODO: unittest
        """ Stop listening for messages posted to the given channels """
        self._pubsub_channels -= set(channels)
        return self._pubsub_method('unsubscribe', channels)

    @_command
    def _psubscribe(self, patterns:ListOf(NativeType)): # TODO: unittest
        """ Listen for messages published to channels matching the given patterns """
        self._pubsub_patterns |= set(patterns)
        return self._pubsub_method('psubscribe', patterns)

    @_command
    def _punsubscribe(self, patterns:ListOf(NativeType)): # TODO: unittest
        """ Stop listening for messages posted to channels matching the given patterns """
        self._pubsub_patterns -= set(channels)
        return self._pubsub_method('punsubscribe', patterns)

    def _pubsub_method(self, method, params):
        if not self._in_pubsub:
            raise Error('Cannot call pubsub methods without calling start_subscribe')

        # Send
        result = yield from self._query(method.encode('ascii'), *map(self.encode_from_native, params))

        # Note that in pubsub mode, this reply is processed by
        # '_handle_pubsub_multibulk_reply', the result is directly unpacked as
        # a list, so `result` here is a list.

        # Returns something like [ 'subscribe', 'our_channel', 1]
        assert result[0] == method

    @_command
    def publish(self, channel:NativeType, message:NativeType) -> int:
        """ Post a message to a channel
        (Returns the number of clients that received this message.) """
        return self._query(b'publish', self.encode_from_native(channel), self.encode_from_native(message))

    # Server

    @_command
    def ping(self) -> StatusReply:
        """ Ping the server (Returns PONG) """
        return self._query(b'ping')

    @_command
    def echo(self, string:NativeType) -> NativeType:
        """ Echo the given string """
        return self._query(b'echo', self.encode_from_native(string),
                    post_process_func=_PostProcessor.bytes_to_native)

    @_command
    def save(self) -> StatusReply:
        """ Synchronously save the dataset to disk """
        return self._query(b'save')

    @_command
    def bgsave(self) -> StatusReply:
        """ Asynchronously save the dataset to disk """
        return self._query(b'bgsave')

    @_command
    def lastsave(self) -> int:
        """ Get the UNIX time stamp of the last successful save to disk """
        return self._query(b'lastsave')

    @_command
    def dbsize(self) -> int:
        """ Return the number of keys in the currently-selected database. """
        return self._query(b'dbsize')

    @_command
    def flushall(self) -> StatusReply:
        """ Remove all keys from all databases """
        return self._query(b'flushall')

    @_command
    def flushdb(self) -> StatusReply:
        """ Delete all the keys of the currently selected DB. This command never fails. """
        return self._query(b'flushdb')

    @_command
    def object(self, subcommand, args):
        """ Inspect the internals of Redis objects """
        raise NotImplementedError

    @_command
    def type(self, key:NativeType) -> StatusReply:
        """ Determine the type stored at key """
        return self._query(b'type', self.encode_from_native(key))

    @_command
    def config_set(self, parameter:str, value:str) -> StatusReply:
        """ Set a configuration parameter to the given value """
        return self._query(b'config', b'set', self.encode_from_native(parameter),
                        self.encode_from_native(value))

    @_command
    def config_get(self, parameter:str) -> ConfigPairReply:
        """ Get the value of a configuration parameter """
        return self._query(b'config', b'get', self.encode_from_native(parameter),
                post_process_func=_PostProcessor.multibulk_as_configpair)

    @_command
    def config_rewrite(self) -> StatusReply:
        """ Rewrite the configuration file with the in memory configuration """
        return self._query(b'config', b'rewrite')

    @_command
    def config_resetstat(self) -> StatusReply:
        """ Reset the stats returned by INFO """
        return self._query(b'config', b'resetstat')

    @_command
    def info(self, section:(NativeType, NoneType)=None) -> InfoReply:
        """ Get information and statistics about the server """
        if section is None:
            return self._query(b'info', post_process_func=_PostProcessor.bytes_to_info)
        else:
            return self._query(b'info', self.encode_from_native(section),
                    post_process_func=_PostProcessor.bytes_to_info)

    # LUA scripting

    @_command
    def register_script(self, script:str): # -> Script:
        """
        Register a LUA script.

        ::

            script = yield from protocol.register_script(lua_code)
            result = yield from script.run(keys=[...], args=[...])

        :returns: :class:`asyncio_redis.Script`
        """
        # The register_script APi was made compatible with the redis.py library:
        # https://github.com/andymccurdy/redis-py
        sha = yield from self.script_load(script)
        return Script(sha, script, lambda:self.evalsha)

    @_command
    def script_exists(self, shas:ListOf(str)) -> ListOf(bool):
        """ Check existence of scripts in the script cache. """
        @asyncio.coroutine
        def post_process(protocol, result):
            # Turn the array of integers into booleans.
            assert isinstance(result, MultiBulkReply)
            values = yield from ListReply(result).get_as_list()
            return [ bool(v) for v in values ]

        return self._query(b'script', b'exists', *[ sha.encode('ascii') for sha in shas ],
                    post_process_func=post_process)

    @_command
    def script_flush(self) -> StatusReply:
        """ Remove all the scripts from the script cache. """
        return self._query(b'script', b'flush')

    @_command
    def script_kill(self) -> StatusReply:
        """ Kill the script currently in execution. """
        try:
            return (yield from self._query(b'script', b'kill'))
        except ErrorReply as e:
            if 'NOTBUSY' in e.args[0]:
                raise NoRunningScriptError
            else:
                raise

    @_command
    def evalsha(self, sha:str,
                        keys:(ListOf(NativeType), NoneType)=None,
                        args:(ListOf(NativeType), NoneType)=None):
        """
        Evaluates a script cached on the server side by its SHA1 digest.
        Scripts are cached on the server side using the SCRIPT LOAD command.

        The return type/value depends on the script.
        """
        try:
            result = yield from self._query(b'evalsha', sha.encode('ascii'),
                        self._encode_int(len(keys)),
                        *map(self.encode_from_native, keys + args))

            # In case that we receive bytes, decode to string.
            if isinstance(result, bytes):
                result = self.decode_to_native(result)

            return result
        except ErrorReply as e:
            raise ScriptKilledError

    @_command
    def script_load(self, script:str) -> str:
        """ Load script, returns sha1 """
        return self._query(b'script', b'load', script.encode('ascii'),
                    post_process_func=_PostProcessor.bytes_to_str)

    # Scanning

    @_command
    def scan(self, match:NativeType='*') -> Cursor:
        """
        Walk through the keys space. You can either fetch the items one by one
        or in bulk.

        ::

            cursor = yield from protocol.scan(match='*')
            while True:
                item = yield from cursor.fetchone()
                if item is None:
                    break
                else:
                    print(item)

        ::

            cursor = yield from protocol.scan(match='*')
            items = yield from cursor.fetchall()

        Also see: :func:`~asyncio_redis.RedisProtocol.sscan`,
        :func:`~asyncio_redis.RedisProtocol.hscan` and
        :func:`~asyncio_redis.RedisProtocol.zscan`

        Redis reference: http://redis.io/commands/scan
        """
        # (Make coroutine. @asyncio.coroutine breaks documentation. It uses
        # @functools.wraps to make a generator for this function. But _command
        # will no longer be able to read the signature.)
        if False: yield

        def scanfunc(cursor):
            return self._scan(cursor, match)

        return Cursor(name='scan(match=%r)' % match, scanfunc=scanfunc)

    def _scan(self, cursor, match):
        return self._query(b'scan', self._encode_int(cursor),
                    b'match', self.encode_from_native(match),
                    post_process_func=_PostProcessor.multibulk_as_scanpart)

    @_command
    def sscan(self, key:NativeType, match:NativeType='*') -> SetCursor:
        """
        Incrementally iterate set elements

        Also see: :func:`~asyncio_redis.RedisProtocol.scan`
        """
        if False: yield
        name = 'sscan(key=%r match=%r)' % (key, match)
        return SetCursor(name=name, scanfunc=self._scan_key_func(b'sscan', key, match))

    @_command
    def hscan(self, key:NativeType, match:NativeType='*') -> DictCursor:
        """
        Incrementally iterate hash fields and associated values
        Also see: :func:`~asyncio_redis.RedisProtocol.scan`
        """
        if False: yield
        name = 'hscan(key=%r match=%r)' % (key, match)
        return DictCursor(name=name, scanfunc=self._scan_key_func(b'hscan', key, match))

    @_command
    def zscan(self, key:NativeType, match:NativeType='*') -> DictCursor:
        """
        Incrementally iterate sorted sets elements and associated scores
        Also see: :func:`~asyncio_redis.RedisProtocol.scan`
        """
        if False: yield
        name = 'zscan(key=%r match=%r)' % (key, match)
        return ZCursor(name=name, scanfunc=self._scan_key_func(b'zscan', key, match))

    def _scan_key_func(self, verb:bytes, key:NativeType, match:NativeType):
        def scan(cursor):
            return self._query(verb, self.encode_from_native(key),
                        self._encode_int(cursor),
                        b'match', self.encode_from_native(match),
                        post_process_func=_PostProcessor.multibulk_as_scanpart)
        return scan

    # Transaction

    @_command
    @asyncio.coroutine
    def multi(self, watch:(ListOf(NativeType),NoneType)=None):
        """
        Start of transaction.

        ::

            transaction = yield from protocol.multi()

            # Run commands in transaction
            f1 = yield from transaction.set('key', 'value')
            f1 = yield from transaction.set('another_key', 'another_value')

            # Commit transaction
            yield from transaction.exec()

            # Retrieve results (you can also use asyncio.tasks.gather)
            result1 = yield from f1
            result2 = yield from f2

        :returns: A :class:`asyncio_redis.Transaction` instance.
        """
        if (self._in_transaction):
            raise Error('Multi calls can not be nested.')

        # Call watch
        if watch is not None:
            for k in watch:
                result = yield from self._query(b'watch', self.encode_from_native(k))
                assert result == StatusReply('OK')

        # Call multi
        result = yield from self._query(b'multi')
        assert result == StatusReply('OK')

        self._in_transaction = True
        self._transaction_response_queue = deque()

        # Create transaction object.
        t = Transaction(self)
        self._transaction = t
        return t

    @asyncio.coroutine
    def _exec(self):
        """
        Execute all commands issued after MULTI
        """
        if not self._in_transaction:
            raise Error('Not in transaction')

        futures_and_postprocessors = self._transaction_response_queue
        self._transaction_response_queue = None

        # Get transaction answers.
        multi_bulk_reply = yield from self._query(b'exec', _bypass=True)

        if multi_bulk_reply is None:
            # We get None when a transaction failed.
            raise TransactionError('Transaction failed.')
        else:
            assert isinstance(multi_bulk_reply, MultiBulkReply)

        for f in multi_bulk_reply.iter_raw():
            answer = yield from f
            f2, post_process_func, call = futures_and_postprocessors.popleft()

            if isinstance(answer, Exception):
                f2.set_exception(answer)
            else:
                if post_process_func:
                    answer = yield from post_process_func(self, answer)
                if call:
                    self._pipelined_calls.remove(call)

                f2.set_result(answer)

        self._transaction_response_queue = deque()
        self._in_transaction = False
        self._transaction = None

    @asyncio.coroutine
    def _discard(self):
        """
        Discard all commands issued after MULTI
        """
        if not self._in_transaction:
            raise Error('Not in transaction')

        self._transaction_response_queue = deque()
        self._in_transaction = False
        self._transaction = None
        result = yield from self._query(b'discard')
        assert result == StatusReply('OK')

    @asyncio.coroutine
    def _unwatch(self):
        """
        Forget about all watched keys
        """
        if not self._in_transaction:
            raise Error('Not in transaction')

        result = yield from self._query(b'unwatch')
        assert result == StatusReply('OK')


class Script:
    """ Lua script. """
    def __init__(self, sha, code, get_evalsha_func):
        self.sha = sha
        self.code = code
        self.get_evalsha_func = get_evalsha_func

    def run(self, keys=[], args=[]):
        """
        Returns a coroutine that executes the script.

        ::

            yield from script.run(keys=[], args=[])
        """
        return self.get_evalsha_func()(self.sha, keys, args)


class Transaction:
    """
    Transaction context. This is a proxy to a :class:`.RedisProtocol` instance.
    Every redis command called on this object will run inside the transaction.
    The transaction can be finished by calling either ``discard`` or ``exec``.

    More info: http://redis.io/topics/transactions
    """
    def __init__(self, protocol):
        self._protocol = protocol

    def __getattr__(self, name):
        """
        Proxy to a protocol.
        """
        # Only proxy commands.
        if name not in _all_commands:
            raise AttributeError

        method = getattr(self._protocol, name)

        # Wrap the method into something that passes the transaction object as
        # first argument.
        @wraps(method)
        def wrapper(*a, **kw):
            if self._protocol._transaction != self:
                raise Error('Transaction already finished or invalid.')

            return method(self, *a, **kw)
        return wrapper

    def discard(self):
        """
        Discard all commands issued after MULTI
        """
        return self._protocol._discard()

    def exec(self):
        """
        Execute transaction.
        """
        return self._protocol._exec()

    def unwatch(self): # XXX: test
        """
        Forget about all watched keys
        """
        return self._protocol._unwatch()


class Subscription:
    """
    Pubsub subscription
    """
    def __init__(self, protocol):
        self.protocol = protocol
        self._messages_queue = Queue() # Pubsub queue

    @wraps(RedisProtocol._subscribe)
    def subscribe(self, channels):
        return self.protocol._subscribe(self, channels)

    @wraps(RedisProtocol._unsubscribe)
    def unsubscribe(self, channels):
        return self.protocol._unsubscribe(self, channels)

    @wraps(RedisProtocol._psubscribe)
    def psubscribe(self, patterns):
        return self.protocol._psubscribe(self, patterns)

    @wraps(RedisProtocol._punsubscribe)
    def punsubscribe(self, patterns):
        return self.protocol._punsubscribe(self, patterns)

    def get_next_published(self):
        """
        Coroutine which waits for next pubsub message to be received and
        returns it.

        :returns: instance of :class:`PubSubReply <asyncio_redis.PubSubReply>`
        """
        return self._messages_queue.get()
