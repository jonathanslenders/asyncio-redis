#!/usr/bin/env python3
import asyncio
import logging
import types

from asyncio.futures import Future
from asyncio.queues import Queue
from asyncio.streams import StreamReader

try:
    import hiredis
except ImportError:
    hiredis = None

from collections import deque
from functools import wraps
from inspect import getfullargspec, formatargspec, getcallargs

from .encoders import BaseEncoder, UTF8Encoder
from .exceptions import (
        ConnectionLostError,
        Error,
        ErrorReply,
        NoRunningScriptError,
        NotConnectedError,
        ScriptKilledError,
        TimeoutError,
        TransactionError,
)
from .log import logger
from .replies import (
        BlockingPopReply,
        ClientListReply,
        ConfigPairReply,
        DictReply,
        EvalScriptReply,
        InfoReply,
        ListReply,
        PubSubReply,
        SetReply,
        StatusReply,
        ZRangeReply,
)


from .cursors import Cursor, SetCursor, DictCursor, ZCursor

__all__ = (
    'RedisProtocol',
    'HiRedisProtocol',
    'Transaction',
    'Subscription',
    'Script',

    'ZAggregate',
    'ZScoreBoundary',
)

NoneType = type(None)

# In Python 3.4.4, `async` was renamed to `ensure_future`.
try:
    ensure_future = asyncio.ensure_future
except AttributeError:
    ensure_future = getattr(asyncio, "async")

class _NoTransactionType(object):
    """
    Instance of this object can be passed to a @_command when it's not part of
    a transaction. We need this because we need a singleton which is different
    from None. (None could be a valid input for a @_command, so there is no way
    to see whether this would be an extra 'transaction' value.)
    """
_NoTransaction = _NoTransactionType()

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
    def __init__(self, protocol, count, loop=None):
        self._loop = loop or asyncio.get_event_loop()

        #: Buffer of incoming, undelivered data, received from the parser.
        self._data_queue = []

        #: Incoming read queries.
        #: Contains (read_count, Future, decode_flag, one_only_flag) tuples.
        self._f_queue = deque()

        self.protocol = protocol
        self.count = int(count)

    def _feed_received(self, item):
        """
        Feed entry for the parser.
        """
        # Push received items on the queue
        self._data_queue.append(item)
        self._flush()

    def _flush(self):
        """
        Answer read queries when we have enough data in our multibulk reply.
        """
        # As long as we have more data in our queue then we require for a read
        # query -> answer queries.
        while self._f_queue and self._f_queue[0][0] <= len(self._data_queue):
            # Pop query.
            count, f, decode, one_only = self._f_queue.popleft()

            # Slice data buffer.
            data, self._data_queue = self._data_queue[:count], self._data_queue[count:]

            # When the decode flag is given, decode bytes to native types.
            if decode:
                data = [ self._decode(d) for d in data ]

            # When one_only flag has been given, don't return an array.
            if one_only:
                assert len(data) == 1
                f.set_result(data[0])
            else:
                f.set_result(data)

    def _decode(self, result):
        """ Decode bytes to native Python types. """
        if isinstance(result, (StatusReply, int, float, MultiBulkReply)):
            # Note that MultiBulkReplies can be nested. e.g. in the 'scan' operation.
            return result
        elif isinstance(result, bytes):
            return self.protocol.decode_to_native(result)
        elif result is None:
            return result
        else:
            raise AssertionError('Invalid type: %r' % type(result))

    def _read(self, decode=True, count=1, _one=False):
        """ Do read operation on the queue. Return future. """
        f = Future(loop=self.protocol._loop)
        self._f_queue.append((count, f, decode, _one))

        # If there is enough data on the queue, answer future immediately.
        self._flush()

        return f

    def iter_raw(self):
        """
        Iterate over all multi bulk packets. This yields futures that won't
        decode bytes yet.
        """
        for i in range(self.count):
            yield self._read(decode=False, _one=True)

    def __iter__(self):
        """
        Iterate over the reply. This yields coroutines of the decoded packets.
        It decodes bytes automatically using protocol.decode_to_native.
        """
        for i in range(self.count):
            yield self._read(_one=True)

    def __repr__(self):
        return 'MultiBulkReply(protocol=%r, count=%r)' % (self.protocol, self.count)


class _ScanPart:
    """ Internal: result chunk of a scan operation. """
    def __init__(self, new_cursor_pos, items):
        self.new_cursor_pos = new_cursor_pos
        self.items = items


class PostProcessors:
    """
    At the protocol level, we only know about a few basic classes; they
    include: bool, int, StatusReply, MultiBulkReply and bytes.
    This will return a postprocessor function that turns these into more
    meaningful objects.

    For some methods, we have several post processors. E.g. a list can be
    returned either as a ListReply (which has some special streaming
    functionality), but also as a Python list.
    """
    @classmethod
    def get_all(cls, return_type):
        """
        Return list of (suffix, return_type, post_processor)
        """
        default = cls.get_default(return_type)
        alternate = cls.get_alternate_post_processor(return_type)

        result = [ ('', return_type, default) ]
        if alternate:
            result.append(alternate)
        return result

    @classmethod
    def get_default(cls, return_type):
        """ Give post processor function for return type. """
        return {
                ListReply: cls.multibulk_as_list,
                SetReply: cls.multibulk_as_set,
                DictReply: cls.multibulk_as_dict,

                float: cls.bytes_to_float,
                (float, NoneType): cls.bytes_to_float_or_none,
                NativeType: cls.bytes_to_native,
                (NativeType, NoneType): cls.bytes_to_native_or_none,
                InfoReply: cls.bytes_to_info,
                ClientListReply: cls.bytes_to_clientlist,
                str: cls.bytes_to_str,
                bool: cls.int_to_bool,
                BlockingPopReply: cls.multibulk_as_blocking_pop_reply,
                ZRangeReply: cls.multibulk_as_zrangereply,

                StatusReply: cls.bytes_to_status_reply,
                (StatusReply, NoneType): cls.bytes_to_status_reply_or_none,
                int: None,
                (int, NoneType): None,
                ConfigPairReply: cls.multibulk_as_configpair,
                ListOf(bool): cls.multibulk_as_boolean_list,
                _ScanPart: cls.multibulk_as_scanpart,
                EvalScriptReply: cls.any_to_evalscript,

                NoneType: None,
        }[return_type]

    @classmethod
    def get_alternate_post_processor(cls, return_type):
        """ For list/set/dict. Create additional post processors that return
        python classes rather than ListReply/SetReply/DictReply """
        original_post_processor = cls.get_default(return_type)

        if return_type == ListReply:
            @asyncio.coroutine
            def as_list(protocol, result):
                result = yield from original_post_processor(protocol, result)
                return (yield from result.aslist())
            return '_aslist', list, as_list

        elif return_type == SetReply:
            @asyncio.coroutine
            def as_set(protocol, result):
                result = yield from original_post_processor(protocol, result)
                return (yield from result.asset())
            return '_asset', set, as_set

        elif return_type in (DictReply, ZRangeReply):
            @asyncio.coroutine
            def as_dict(protocol, result):
                result = yield from original_post_processor(protocol, result)
                return (yield from result.asdict())
            return '_asdict', dict, as_dict

    # === Post processor handlers below. ===

    @asyncio.coroutine
    def multibulk_as_list(protocol, result):
        assert isinstance(result, MultiBulkReply)
        return ListReply(result)

    @asyncio.coroutine
    def multibulk_as_boolean_list(protocol, result):
        # Turn the array of integers into booleans.
        assert isinstance(result, MultiBulkReply)
        values = yield from ListReply(result).aslist()
        return [ bool(v) for v in values ]

    @asyncio.coroutine
    def multibulk_as_set(protocol, result):
        assert isinstance(result, MultiBulkReply)
        return SetReply(result)

    @asyncio.coroutine
    def multibulk_as_dict(protocol, result):
        assert isinstance(result, MultiBulkReply)
        return DictReply(result)

    @asyncio.coroutine
    def multibulk_as_zrangereply(protocol, result):
        assert isinstance(result, MultiBulkReply)
        return ZRangeReply(result)

    @asyncio.coroutine
    def multibulk_as_blocking_pop_reply(protocol, result):
        if result is None:
            raise TimeoutError('Timeout in blocking pop')
        else:
            assert isinstance(result, MultiBulkReply)
            list_name, value = yield from ListReply(result).aslist()
            return BlockingPopReply(list_name, value)

    @asyncio.coroutine
    def multibulk_as_configpair(protocol, result):
        assert isinstance(result, MultiBulkReply)
        parameter, value = yield from ListReply(result).aslist()
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
        new_cursor_pos, items_bulk = yield from ListReply(result).aslist()
        assert isinstance(items_bulk, MultiBulkReply)

        # Read all items for scan chunk in memory. This is fine, because it's
        # transmitted in chunks of about 10.
        items = yield from ListReply(items_bulk).aslist()
        return _ScanPart(int(new_cursor_pos), items)

    @asyncio.coroutine
    def bytes_to_info(protocol, result):
        assert isinstance(result, bytes)
        return InfoReply(result)

    @asyncio.coroutine
    def bytes_to_status_reply(protocol, result):
        assert isinstance(result, bytes)
        return StatusReply(result.decode('utf-8'))

    @asyncio.coroutine
    def bytes_to_status_reply_or_none(protocol, result):
        assert isinstance(result, (bytes, NoneType))
        if result:
            return StatusReply(result.decode('utf-8'))

    @asyncio.coroutine
    def bytes_to_clientlist(protocol, result):
        assert isinstance(result, bytes)
        return ClientListReply(result)

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

    @asyncio.coroutine
    def any_to_evalscript(protocol, result):
        # Result can be native, int, MultiBulkReply or even a nested structure
        assert isinstance(result, (int, bytes, MultiBulkReply, NoneType))
        return EvalScriptReply(protocol, result)


class ListOf:
    """ Annotation helper for protocol methods. """
    def __init__(self, type_):
        self.type = type_

    def __repr__(self):
        return 'ListOf(%r)' % self.type

    def __eq__(self, other):
        return isinstance(other, ListOf) and other.type == self.type

    def __hash__(self):
        return hash((ListOf, self.type))


class NativeType:
    """
    Constant which represents the native Python type that's used.
    """
    def __new__(cls):
        raise Exception('NativeType is not meant to be initialized.')


class CommandCreator:
    """
    Utility for creating a wrapper around the Redis protocol methods.
    This will also do type checking.

    This wrapper handles (optionally) post processing of the returned data and
    implements some logic where commands behave different in case of a
    transaction or pubsub.

    Warning: We use the annotations of `method` extensively for type checking
             and determining which post processor to choose.
    """
    def __init__(self, method):
        self.method = method

    @property
    def specs(self):
       """ Argspecs """
       return getfullargspec(self.method)

    @property
    def return_type(self):
        """ Return type as defined in the method's annotation. """
        return self.specs.annotations.get('return', None)

    @property
    def params(self):
        return { k:v for k, v in self.specs.annotations.items() if k != 'return' }

    @classmethod
    def get_real_type(cls, protocol, type_):
        """
        Given a protocol instance, and type annotation, return something that
        we can pass to isinstance for the typechecking.
        """
        # If NativeType was given, replace it with the type of the protocol
        # itself.
        if isinstance(type_, tuple):
            return tuple(cls.get_real_type(protocol, t) for t in type_)

        if type_ == NativeType:
            return protocol.native_type
        elif isinstance(type_, ListOf):
            return (list, types.GeneratorType) # We don't check the content of the list.
        else:
            return type_

    def _create_input_typechecker(self):
        """ Return function that does typechecking on input data. """
        params = self.params

        if params:
            def typecheck_input(protocol, *a, **kw):
                """
                Given a protocol instance and *a/**kw of this method, raise TypeError
                when the signature doesn't match.
                """
                if protocol.enable_typechecking:
                    # All @_command/@_query_command methods can take
                    # *optionally* a Transaction instance as first argument.
                    if a and isinstance(a[0], (Transaction, _NoTransactionType)):
                        a = a[1:]

                    for name, value in getcallargs(self.method, None, _NoTransaction, *a, **kw).items():
                        if name in params:
                            real_type = self.get_real_type(protocol, params[name])
                            if not isinstance(value, real_type):
                                raise TypeError('RedisProtocol.%s received %r, expected %r' %
                                                (self.method.__name__, type(value).__name__, real_type))
        else:
            def typecheck_input(protocol, *a, **kw):
                pass

        return typecheck_input

    def _create_return_typechecker(self, return_type):
        """ Return function that does typechecking on output data. """
        if return_type and not isinstance(return_type, str): # Exclude 'Transaction'/'Subscription' which are 'str'
            def typecheck_return(protocol, result):
                """
                Given protocol and result value. Raise TypeError if the result is of the wrong type.
                """
                if protocol.enable_typechecking:
                    expected_type = self.get_real_type(protocol, return_type)
                    if not isinstance(result, expected_type):
                        raise TypeError('Got unexpected return type %r in RedisProtocol.%s, expected %r' %
                                        (type(result).__name__, self.method.__name__, expected_type))
        else:
            def typecheck_return(protocol, result):
                pass

        return typecheck_return

    def _get_docstring(self, suffix, return_type):
        # Append the real signature as the first line in the docstring.
        # (This will make the sphinx docs show the real signature instead of
        # (*a, **kw) of the wrapper.)
        # (But don't put the anotations inside the copied signature, that's rather
        # ugly in the docs.)
        signature = formatargspec(* self.specs[:6])

        # Use function annotations to generate param documentation.

        def get_name(type_):
            """ Turn type annotation into doc string. """
            try:
                return {
                    BlockingPopReply: ":class:`BlockingPopReply <asyncio_redis.replies.BlockingPopReply>`",
                    ConfigPairReply: ":class:`ConfigPairReply <asyncio_redis.replies.ConfigPairReply>`",
                    DictReply: ":class:`DictReply <asyncio_redis.replies.DictReply>`",
                    InfoReply: ":class:`InfoReply <asyncio_redis.replies.InfoReply>`",
                    ClientListReply: ":class:`InfoReply <asyncio_redis.replies.ClientListReply>`",
                    ListReply: ":class:`ListReply <asyncio_redis.replies.ListReply>`",
                    MultiBulkReply: ":class:`MultiBulkReply <asyncio_redis.replies.MultiBulkReply>`",
                    NativeType: "Native Python type, as defined by :attr:`~asyncio_redis.encoders.BaseEncoder.native_type`",
                    NoneType: "None",
                    SetReply: ":class:`SetReply <asyncio_redis.replies.SetReply>`",
                    StatusReply: ":class:`StatusReply <asyncio_redis.replies.StatusReply>`",
                    ZRangeReply: ":class:`ZRangeReply <asyncio_redis.replies.ZRangeReply>`",
                    ZScoreBoundary: ":class:`ZScoreBoundary <asyncio_redis.replies.ZScoreBoundary>`",
                    EvalScriptReply: ":class:`EvalScriptReply <asyncio_redis.replies.EvalScriptReply>`",
                    Cursor: ":class:`Cursor <asyncio_redis.cursors.Cursor>`",
                    SetCursor: ":class:`SetCursor <asyncio_redis.cursors.SetCursor>`",
                    DictCursor: ":class:`DictCursor <asyncio_redis.cursors.DictCursor>`",
                    ZCursor: ":class:`ZCursor <asyncio_redis.cursors.ZCursor>`",
                    _ScanPart: ":class:`_ScanPart",
                    int: 'int',
                    bool: 'bool',
                    dict: 'dict',
                    float: 'float',
                    str: 'str',
                    bytes: 'bytes',

                    list: 'list',
                    set: 'set',
                    dict: 'dict',

                    # XXX: Because of circulare references, we cannot use the real types here.
                    'Transaction': ":class:`asyncio_redis.Transaction`",
                    'Subscription': ":class:`asyncio_redis.Subscription`",
                    'Script': ":class:`~asyncio_redis.Script`",
                }[type_]
            except KeyError:
                if isinstance(type_, ListOf):
                    return "List or iterable of %s" % get_name(type_.type)

                elif isinstance(type_, tuple):
                    return ' or '.join(get_name(t) for t in type_)
                else:
                    raise Exception('Unknown annotation %r' % type_)
                    #return "``%s``" % type_.__name__

        def get_param(k, v):
            return ':param %s: %s\n' % (k, get_name(v))

        params_str = [ get_param(k, v) for k, v in self.params.items() ]
        returns = ':returns: (Future of) %s\n' % get_name(return_type) if return_type else ''

        return '%s%s\n%s\n\n%s%s' % (
                self.method.__name__ + suffix, signature,
                self.method.__doc__,
                ''.join(params_str),
                returns
                )

    def get_methods(self):
        """
        Return all the methods to be used in the RedisProtocol class.
        """
        return [ ('', self._get_wrapped_method(None, '', self.return_type)) ]

    def _get_wrapped_method(self, post_process, suffix, return_type):
        """
        Return the wrapped method for use in the `RedisProtocol` class.
        """
        typecheck_input = self._create_input_typechecker()
        typecheck_return = self._create_return_typechecker(return_type)
        method = self.method

        # Wrap it into a check which allows this command to be run either
        # directly on the protocol, outside of transactions or from the
        # transaction object.
        @wraps(method)
        @asyncio.coroutine
        def wrapper(protocol_self, *a, **kw):
            if a and isinstance(a[0], (Transaction, _NoTransactionType)):
                transaction = a[0]
                a = a[1:]
            else:
                transaction = _NoTransaction

            # When calling from a transaction
            if transaction != _NoTransaction:
                # In case of a transaction, we receive a Future from the command.
                typecheck_input(protocol_self, *a, **kw)
                future = yield from method(protocol_self, transaction, *a, **kw)
                future2 = Future(loop=protocol_self._loop)

                # Typecheck the future when the result is available.

                @asyncio.coroutine
                def done(result):
                    if post_process:
                        result = yield from post_process(protocol_self, result)
                    typecheck_return(protocol_self, result)
                    future2.set_result(result)

                future.add_done_callback(lambda f: ensure_future(done(f.result()), loop=protocol_self._loop))

                return future2

            # When calling from a pubsub context
            elif protocol_self.in_pubsub:
                if not a or a[0] != protocol_self._subscription:
                    raise Error('Cannot run command inside pubsub subscription.')

                else:
                    typecheck_input(protocol_self, *a[1:], **kw)
                    result = yield from method(protocol_self, _NoTransaction, *a[1:], **kw)
                    if post_process:
                        result = yield from post_process(protocol_self, result)
                    typecheck_return(protocol_self, result)
                    return (result)

            else:
                typecheck_input(protocol_self, *a, **kw)
                result = yield from method(protocol_self, _NoTransaction, *a, **kw)
                if post_process:
                    result = yield from post_process(protocol_self, result)
                typecheck_return(protocol_self, result)
                return result

        wrapper.__doc__ = self._get_docstring(suffix, return_type)
        return wrapper


class QueryCommandCreator(CommandCreator):
    """
    Like `CommandCreator`, but for methods registered with `_query_command`.
    This are the methods that cause commands to be send to the server.

    Most of the commands get a reply from the server that needs to be post
    processed to get the right Python type. We inspect here the
    'returns'-annotation to determine the correct post processor.
    """
    def get_methods(self):
        # (Some commands, e.g. those that return a ListReply can generate
        # multiple protocol methods.  One that does return the ListReply, but
        # also one with the 'aslist' suffix that returns a Python list.)
        all_post_processors = PostProcessors.get_all(self.return_type)
        result = []

        for suffix, return_type, post_processor in all_post_processors:
            result.append( (suffix, self._get_wrapped_method(post_processor, suffix, return_type)) )

        return result

_SMALL_INTS = list(str(i).encode('ascii') for i in range(1000))


# List of all command methods.
_all_commands = []


class _command:
    """ Mark method as command (to be passed through CommandCreator for the
    creation of a protocol method) """
    creator = CommandCreator

    def __init__(self, method):
        self.method = method


class _query_command(_command):
    """
    Mark method as query command: This will pass through QueryCommandCreator.

    NOTE: be sure to choose the correct 'returns'-annotation. This will automatially
    determine the correct post processor function in :class:`PostProcessors`.
    """
    creator = QueryCommandCreator

    def __init__(self, method):
        super().__init__(method)


class _RedisProtocolMeta(type):
    """
    Metaclass for `RedisProtocol` which applies the _command decorator.
    """
    def __new__(cls, name, bases, attrs):
        for attr_name, value in dict(attrs).items():
            if isinstance(value, _command):
                creator = value.creator(value.method)
                for suffix, method in creator.get_methods():
                    attrs[attr_name + suffix] =  method

                    # Register command.
                    _all_commands.append(attr_name + suffix)

        return type.__new__(cls, name, bases, attrs)


class RedisProtocol(asyncio.Protocol, metaclass=_RedisProtocolMeta):
    """
    The Redis Protocol implementation.

    ::

        self.loop = asyncio.get_event_loop()
        transport, protocol = yield from loop.create_connection(RedisProtocol, 'localhost', 6379)

    :param password: Redis database password
    :type password: Native Python type as defined by the ``encoder`` parameter
    :param encoder: Encoder to use for encoding to or decoding from redis bytes to a native type.
                    (Defaults to :class:`~asyncio_redis.encoders.UTF8Encoder`)
    :type encoder: :class:`~asyncio_redis.encoders.BaseEncoder` instance.
    :param db: Redis database
    :type db: int
    :param enable_typechecking: When ``True``, check argument types for all
                                redis commands. Normally you want to have this
                                enabled.
    :type enable_typechecking: bool
    """
    def __init__(self, *, password=None, db=0, encoder=None, connection_lost_callback=None, enable_typechecking=True, loop=None):
        if encoder is None:
            encoder = UTF8Encoder()

        assert isinstance(db, int)
        assert isinstance(encoder, BaseEncoder)
        assert encoder.native_type, 'Encoder.native_type not defined'
        assert not password or isinstance(password, encoder.native_type)

        self.password = password
        self.db = db
        self._connection_lost_callback = connection_lost_callback
        self._loop = loop or asyncio.get_event_loop()

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
        self._transaction_lock = asyncio.Lock(loop=loop)
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
        self._reader = StreamReader(loop=self._loop)
        self._reader.set_transport(transport)
        self._reader_f = ensure_future(self._reader_coroutine(), loop=self._loop)

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

        ensure_future(initialize(), loop=self._loop)

    def data_received(self, data):
        """ Process data received from Redis server.  """
        self._reader.feed_data(data)

    def _encode_int(self, value:int) -> bytes:
        """ Encodes an integer to bytes. (always ascii) """
        if 0 < value < 1000: # For small values, take pre-encoded string.
            return _SMALL_INTS[value]
        else:
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
            logger.info("Connection lost with exec: %s" % exc)
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
            if not f.cancelled():
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
        return bool(self._transaction)

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
        while True:
            try:
                yield from self._handle_item(self._push_answer)
            except ConnectionLostError:
                return
            except asyncio.streams.IncompleteReadError:
                return

    @asyncio.coroutine
    def _handle_item(self, cb):
        c = yield from self._reader.readexactly(1)
        if c:
            yield from self._line_received_handlers[c](cb)
        else:
            raise ConnectionLostError(None)

    @asyncio.coroutine
    def _handle_status_reply(self, cb):
        line = (yield from self._reader.readline()).rstrip(b'\r\n')
        cb(line)

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
            data = yield from self._reader.readexactly(length)
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

        reply = MultiBulkReply(self, count, loop=self._loop)

        # Return the empty queue immediately as an answer.
        if self._in_pubsub:
            ensure_future(self._handle_pubsub_multibulk_reply(reply), loop=self._loop)
        else:
            cb(reply)

        # Wait for all multi bulk reply content.
        for i in range(count):
            yield from self._handle_item(reply._feed_received)

    @asyncio.coroutine
    def _handle_pubsub_multibulk_reply(self, multibulk_reply):
        # Read first item of the multi bulk reply raw.
        type = yield from multibulk_reply._read(decode=False, _one=True)
        assert type in (b'message', b'subscribe', b'unsubscribe', b'pmessage', b'psubscribe', b'punsubscribe')

        if type == b'message':
            channel, value = yield from multibulk_reply._read(count=2)
            yield from self._subscription._messages_queue.put(PubSubReply(channel, value))

        elif type == b'pmessage':
            pattern, channel, value = yield from multibulk_reply._read(count=3)
            yield from self._subscription._messages_queue.put(PubSubReply(channel, value, pattern=pattern))

        # We can safely ignore 'subscribe'/'unsubscribe' replies at this point,
        # they don't contain anything really useful.

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
        data += [ b'*', self._encode_int(len(args)), b'\r\n' ]

        # Write arguments.
        for arg in args:
            data += [ b'$', self._encode_int(len(arg)), b'\r\n', arg, b'\r\n' ]

        # Flush the last part
        self.transport.write(b''.join(data))

    @asyncio.coroutine
    def _get_answer(self, transaction, answer_f, _bypass=False, call=None):  # XXX: rename _bypass to not_queued
        """
        Return an answer to the pipelined query.
        (Or when we are in a transaction, return a future for the answer.)
        """
        # Wait for the answer to come in
        result = yield from answer_f

        if transaction != _NoTransaction and not _bypass:
            # When the connection is inside a transaction, the query will be queued.
            if result != b'QUEUED':
                raise Error('Expected to receive QUEUED for query in transaction, received %r.' % result)

            # Return a future which will contain the result when it arrives.
            f = Future(loop=self._loop)
            self._transaction_response_queue.append( (f, call) )
            return f
        else:
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
        elif f.cancelled():
            # Received an answer from Redis, for a query which `Future` got
            # already cancelled. Don't call set_result, that would raise an
            # `InvalidStateError` otherwise.
            pass
        else:
            f.set_result(answer)

    @asyncio.coroutine
    def _query(self, transaction, *args, _bypass=False, set_blocking=False):
        """
        Wrapper around both _send_command and _get_answer.

        Coroutine that sends the query to the server, and returns the reply.
        (Where the reply is a simple Redis type: these are `int`,
        `StatusReply`, `bytes` or `MultiBulkReply`) When we are in a transaction,
        this coroutine will return a `Future` of the actual result.
        """
        assert transaction == _NoTransaction or isinstance(transaction, Transaction)

        if not self._is_connected:
            raise NotConnectedError

        # Get lock.
        if transaction == _NoTransaction:
            yield from self._transaction_lock.acquire()
        else:
            assert transaction == self._transaction

        try:
            call = PipelinedCall(args[0], set_blocking)
            self._pipelined_calls.add(call)

            # Add a new future to our answer queue.
            answer_f = Future(loop=self._loop)
            self._queue.append(answer_f)

            # Send command
            self._send_command(args)
        finally:
            # Release lock.
            if transaction == _NoTransaction:
                self._transaction_lock.release()

        # TODO: when set_blocking=True, only release lock after reading the answer.
        #       (it doesn't make sense to free the input and pipeline commands in that case.)


        # Receive answer.
        result = yield from self._get_answer(transaction, answer_f, _bypass=_bypass, call=call)
        return result

    # Internal

    @_query_command
    def auth(self, tr, password:NativeType) -> StatusReply:
        """ Authenticate to the server """
        self.password = password
        return self._query(tr, b'auth', self.encode_from_native(password))

    @_query_command
    def select(self, tr, db:int) -> StatusReply:
        """ Change the selected database for the current connection """
        self.db = db
        return self._query(tr, b'select', self._encode_int(db))

    # Strings

    @_query_command
    def set(self, tr, key:NativeType, value:NativeType,
            expire:(int, NoneType)=None, pexpire:(int, NoneType)=None,
            only_if_not_exists:bool=False, only_if_exists:bool=False) -> (StatusReply, NoneType):
        """
        Set the string value of a key

        ::

            yield from protocol.set('key', 'value')
            result = yield from protocol.get('key')
            assert result == 'value'

        To set a value and its expiration, only if key not exists, do:

        ::

            yield from protocol.set('key', 'value', expire=1, only_if_not_exists=True)

        This will send: ``SET key value EX 1 NX`` at the network.
        To set value and its expiration in milliseconds, but only if key already exists:

        ::

            yield from protocol.set('key', 'value', pexpire=1000, only_if_exists=True)
        """
        params = [
            b'set',
            self.encode_from_native(key),
            self.encode_from_native(value)
        ]
        if expire is not None:
            params.extend((b'ex', self._encode_int(expire)))
        if pexpire is not None:
            params.extend((b'px', self._encode_int(pexpire)))
        if only_if_not_exists and only_if_exists:
            raise ValueError("only_if_not_exists and only_if_exists cannot be true simultaniously")
        if only_if_not_exists:
            params.append(b'nx')
        if only_if_exists:
            params.append(b'xx')

        return self._query(tr, *params)

    @_query_command
    def setex(self, tr, key:NativeType, seconds:int, value:NativeType) -> StatusReply:
        """ Set the string value of a key with expire """
        return self._query(tr, b'setex', self.encode_from_native(key),
                self._encode_int(seconds), self.encode_from_native(value))

    @_query_command
    def setnx(self, tr, key:NativeType, value:NativeType) -> bool:
        """ Set the string value of a key if it does not exist.
        Returns True if value is successfully set """
        return self._query(tr, b'setnx', self.encode_from_native(key), self.encode_from_native(value))

    @_query_command
    def get(self, tr, key:NativeType) -> (NativeType, NoneType):
        """ Get the value of a key """
        return self._query(tr, b'get', self.encode_from_native(key))

    @_query_command
    def mget(self, tr, keys:ListOf(NativeType)) -> ListReply:
        """ Returns the values of all specified keys. """
        return self._query(tr, b'mget', *map(self.encode_from_native, keys))

    @_query_command
    def strlen(self, tr, key:NativeType) -> int:
        """ Returns the length of the string value stored at key. An error is
        returned when key holds a non-string value.  """
        return self._query(tr, b'strlen', self.encode_from_native(key))

    @_query_command
    def append(self, tr, key:NativeType, value:NativeType) -> int:
        """ Append a value to a key """
        return self._query(tr, b'append', self.encode_from_native(key), self.encode_from_native(value))

    @_query_command
    def getset(self, tr, key:NativeType, value:NativeType) -> (NativeType, NoneType):
        """ Set the string value of a key and return its old value """
        return self._query(tr, b'getset', self.encode_from_native(key), self.encode_from_native(value))

    @_query_command
    def incr(self, tr, key:NativeType) -> int:
        """ Increment the integer value of a key by one """
        return self._query(tr, b'incr', self.encode_from_native(key))

    @_query_command
    def incrby(self, tr, key:NativeType, increment:int) -> int:
        """ Increment the integer value of a key by the given amount """
        return self._query(tr, b'incrby', self.encode_from_native(key), self._encode_int(increment))

    @_query_command
    def decr(self, tr, key:NativeType) -> int:
        """ Decrement the integer value of a key by one """
        return self._query(tr, b'decr', self.encode_from_native(key))

    @_query_command
    def decrby(self, tr, key:NativeType, increment:int) -> int:
        """ Decrement the integer value of a key by the given number """
        return self._query(tr, b'decrby', self.encode_from_native(key), self._encode_int(increment))

    @_query_command
    def randomkey(self, tr) -> NativeType:
        """ Return a random key from the keyspace """
        return self._query(tr, b'randomkey')

    @_query_command
    def exists(self, tr, key:NativeType) -> bool:
        """ Determine if a key exists """
        return self._query(tr, b'exists', self.encode_from_native(key))

    @_query_command
    def delete(self, tr, keys:ListOf(NativeType)) -> int:
        """ Delete a key """
        return self._query(tr, b'del', *map(self.encode_from_native, keys))

    @_query_command
    def move(self, tr, key:NativeType, database:int) -> int:
        """ Move a key to another database """
        return self._query(tr, b'move', self.encode_from_native(key), self._encode_int(database)) # TODO: unittest

    @_query_command
    def rename(self, tr, key:NativeType, newkey:NativeType) -> StatusReply:
        """ Rename a key """
        return self._query(tr, b'rename', self.encode_from_native(key), self.encode_from_native(newkey))

    @_query_command
    def renamenx(self, tr, key:NativeType, newkey:NativeType) -> int:
        """ Rename a key, only if the new key does not exist
        (Returns 1 if the key was successfully renamed.) """
        return self._query(tr, b'renamenx', self.encode_from_native(key), self.encode_from_native(newkey))

    @_query_command
    def bitop_and(self, tr, destkey:NativeType, srckeys:ListOf(NativeType)) -> int:
        """ Perform a bitwise AND operation between multiple keys. """
        return self._bitop(tr, b'and', destkey, srckeys)

    @_query_command
    def bitop_or(self, tr, destkey:NativeType, srckeys:ListOf(NativeType)) -> int:
        """ Perform a bitwise OR operation between multiple keys. """
        return self._bitop(tr, b'or', destkey, srckeys)

    @_query_command
    def bitop_xor(self, tr, destkey:NativeType, srckeys:ListOf(NativeType)) -> int:
        """ Perform a bitwise XOR operation between multiple keys. """
        return self._bitop(tr, b'xor', destkey, srckeys)

    def _bitop(self, tr, op, destkey, srckeys):
        return self._query(tr, b'bitop', op, self.encode_from_native(destkey), *map(self.encode_from_native, srckeys))

    @_query_command
    def bitop_not(self, tr, destkey:NativeType, key:NativeType) -> int:
        """ Perform a bitwise NOT operation between multiple keys. """
        return self._query(tr, b'bitop', b'not', self.encode_from_native(destkey), self.encode_from_native(key))

    @_query_command
    def bitcount(self, tr, key:NativeType, start:int=0, end:int=-1) -> int:
        """ Count the number of set bits (population counting) in a string. """
        return self._query(tr, b'bitcount', self.encode_from_native(key), self._encode_int(start), self._encode_int(end))

    @_query_command
    def getbit(self, tr, key:NativeType, offset:int) -> bool:
        """ Returns the bit value at offset in the string value stored at key """
        return self._query(tr, b'getbit', self.encode_from_native(key), self._encode_int(offset))

    @_query_command
    def setbit(self, tr, key:NativeType, offset:int, value:bool) -> bool:
        """ Sets or clears the bit at offset in the string value stored at key """
        return self._query(tr, b'setbit', self.encode_from_native(key), self._encode_int(offset),
                self._encode_int(int(value)))

    # Keys

    @_query_command
    def keys(self, tr, pattern:NativeType) -> ListReply:
        """
        Find all keys matching the given pattern.

        .. note:: Also take a look at :func:`~asyncio_redis.RedisProtocol.scan`.
        """
        return self._query(tr, b'keys', self.encode_from_native(pattern))

#    @_query_command
#    def dump(self, key:NativeType):
#        """ Return a serialized version of the value stored at the specified key. """
#        # Dump does not work yet. It shouldn't be decoded using utf-8'
#        raise NotImplementedError('Not supported.')

    @_query_command
    def expire(self, tr, key:NativeType, seconds:int) -> int:
        """ Set a key's time to live in seconds """
        return self._query(tr, b'expire', self.encode_from_native(key), self._encode_int(seconds))

    @_query_command
    def pexpire(self, tr, key:NativeType, milliseconds:int) -> int:
        """ Set a key's time to live in milliseconds """
        return self._query(tr, b'pexpire', self.encode_from_native(key), self._encode_int(milliseconds))

    @_query_command
    def expireat(self, tr, key:NativeType, timestamp:int) -> int:
        """ Set the expiration for a key as a UNIX timestamp """
        return self._query(tr, b'expireat', self.encode_from_native(key), self._encode_int(timestamp))

    @_query_command
    def pexpireat(self, tr, key:NativeType, milliseconds_timestamp:int) -> int:
        """ Set the expiration for a key as a UNIX timestamp specified in milliseconds """
        return self._query(tr, b'pexpireat', self.encode_from_native(key), self._encode_int(milliseconds_timestamp))

    @_query_command
    def persist(self, tr, key:NativeType) -> int:
        """ Remove the expiration from a key """
        return self._query(tr, b'persist', self.encode_from_native(key))

    @_query_command
    def ttl(self, tr, key:NativeType) -> int:
        """ Get the time to live for a key """
        return self._query(tr, b'ttl', self.encode_from_native(key))

    @_query_command
    def pttl(self, tr, key:NativeType) -> int:
        """ Get the time to live for a key in milliseconds """
        return self._query(tr, b'pttl', self.encode_from_native(key))

    # Set operations

    @_query_command
    def sadd(self, tr, key:NativeType, members:ListOf(NativeType)) -> int:
        """ Add one or more members to a set """
        return self._query(tr, b'sadd', self.encode_from_native(key), *map(self.encode_from_native, members))

    @_query_command
    def srem(self, tr, key:NativeType, members:ListOf(NativeType)) -> int:
        """ Remove one or more members from a set """
        return self._query(tr, b'srem', self.encode_from_native(key), *map(self.encode_from_native, members))

    @_query_command
    def spop(self, tr, key:NativeType) -> (NativeType, NoneType):
        """ Removes and returns a random element from the set value stored at key. """
        return self._query(tr, b'spop', self.encode_from_native(key))

    @_query_command
    def srandmember(self, tr, key:NativeType, count:int=1) -> SetReply:
        """ Get one or multiple random members from a set
        (Returns a list of members, even when count==1) """
        return self._query(tr, b'srandmember', self.encode_from_native(key), self._encode_int(count))

    @_query_command
    def sismember(self, tr, key:NativeType, value:NativeType) -> bool:
        """ Determine if a given value is a member of a set """
        return self._query(tr, b'sismember', self.encode_from_native(key), self.encode_from_native(value))

    @_query_command
    def scard(self, tr, key:NativeType) -> int:
        """ Get the number of members in a set """
        return self._query(tr, b'scard', self.encode_from_native(key))

    @_query_command
    def smembers(self, tr, key:NativeType) -> SetReply:
        """ Get all the members in a set """
        return self._query(tr, b'smembers', self.encode_from_native(key))

    @_query_command
    def sinter(self, tr, keys:ListOf(NativeType)) -> SetReply:
        """ Intersect multiple sets """
        return self._query(tr, b'sinter', *map(self.encode_from_native, keys))

    @_query_command
    def sinterstore(self, tr, destination:NativeType, keys:ListOf(NativeType)) -> int:
        """ Intersect multiple sets and store the resulting set in a key """
        return self._query(tr, b'sinterstore', self.encode_from_native(destination), *map(self.encode_from_native, keys))

    @_query_command
    def sdiff(self, tr, keys:ListOf(NativeType)) -> SetReply:
        """ Subtract multiple sets """
        return self._query(tr, b'sdiff', *map(self.encode_from_native, keys))

    @_query_command
    def sdiffstore(self, tr, destination:NativeType, keys:ListOf(NativeType)) -> int:
        """ Subtract multiple sets and store the resulting set in a key """
        return self._query(tr, b'sdiffstore', self.encode_from_native(destination),
                *map(self.encode_from_native, keys))

    @_query_command
    def sunion(self, tr, keys:ListOf(NativeType)) -> SetReply:
        """ Add multiple sets """
        return self._query(tr, b'sunion', *map(self.encode_from_native, keys))

    @_query_command
    def sunionstore(self, tr, destination:NativeType, keys:ListOf(NativeType)) -> int:
        """ Add multiple sets and store the resulting set in a key """
        return self._query(tr, b'sunionstore', self.encode_from_native(destination), *map(self.encode_from_native, keys))

    @_query_command
    def smove(self, tr, source:NativeType, destination:NativeType, value:NativeType) -> int:
        """ Move a member from one set to another """
        return self._query(tr, b'smove', self.encode_from_native(source), self.encode_from_native(destination), self.encode_from_native(value))

    # List operations

    @_query_command
    def lpush(self, tr, key:NativeType, values:ListOf(NativeType)) -> int:
        """ Prepend one or multiple values to a list """
        return self._query(tr, b'lpush', self.encode_from_native(key), *map(self.encode_from_native, values))

    @_query_command
    def lpushx(self, tr, key:NativeType, value:NativeType) -> int:
        """ Prepend a value to a list, only if the list exists """
        return self._query(tr, b'lpushx', self.encode_from_native(key), self.encode_from_native(value))

    @_query_command
    def rpush(self, tr, key:NativeType, values:ListOf(NativeType)) -> int:
        """ Append one or multiple values to a list """
        return self._query(tr, b'rpush', self.encode_from_native(key), *map(self.encode_from_native, values))

    @_query_command
    def rpushx(self, tr, key:NativeType, value:NativeType) -> int:
        """ Append a value to a list, only if the list exists """
        return self._query(tr, b'rpushx', self.encode_from_native(key), self.encode_from_native(value))

    @_query_command
    def llen(self, tr, key:NativeType) -> int:
        """ Returns the length of the list stored at key. """
        return self._query(tr, b'llen', self.encode_from_native(key))

    @_query_command
    def lrem(self, tr, key:NativeType, count:int=0, value='') -> int:
        """ Remove elements from a list """
        return self._query(tr, b'lrem', self.encode_from_native(key), self._encode_int(count), self.encode_from_native(value))

    @_query_command
    def lrange(self, tr, key, start:int=0, stop:int=-1) -> ListReply:
        """ Get a range of elements from a list. """
        return self._query(tr, b'lrange', self.encode_from_native(key), self._encode_int(start), self._encode_int(stop))

    @_query_command
    def ltrim(self, tr, key:NativeType, start:int=0, stop:int=-1) -> StatusReply:
        """ Trim a list to the specified range """
        return self._query(tr, b'ltrim', self.encode_from_native(key), self._encode_int(start), self._encode_int(stop))

    @_query_command
    def lpop(self, tr, key:NativeType) -> (NativeType, NoneType):
        """ Remove and get the first element in a list """
        return self._query(tr, b'lpop', self.encode_from_native(key))

    @_query_command
    def rpop(self, tr, key:NativeType) -> (NativeType, NoneType):
        """ Remove and get the last element in a list """
        return self._query(tr, b'rpop', self.encode_from_native(key))

    @_query_command
    def rpoplpush(self, tr, source:NativeType, destination:NativeType) -> (NativeType, NoneType):
        """ Remove the last element in a list, append it to another list and return it """
        return self._query(tr, b'rpoplpush', self.encode_from_native(source), self.encode_from_native(destination))

    @_query_command
    def lindex(self, tr, key:NativeType, index:int) -> (NativeType, NoneType):
        """ Get an element from a list by its index """
        return self._query(tr, b'lindex', self.encode_from_native(key), self._encode_int(index))

    @_query_command
    def blpop(self, tr, keys:ListOf(NativeType), timeout:int=0) -> BlockingPopReply:
        """ Remove and get the first element in a list, or block until one is available.
        This will raise :class:`~asyncio_redis.exceptions.TimeoutError` when
        the timeout was exceeded and Redis returns `None`. """
        return self._blocking_pop(tr, b'blpop', keys, timeout=timeout)

    @_query_command
    def brpop(self, tr, keys:ListOf(NativeType), timeout:int=0) -> BlockingPopReply:
        """ Remove and get the last element in a list, or block until one is available.
        This will raise :class:`~asyncio_redis.exceptions.TimeoutError` when
        the timeout was exceeded and Redis returns `None`. """
        return self._blocking_pop(tr, b'brpop', keys, timeout=timeout)

    def _blocking_pop(self, tr, command, keys, timeout:int=0):
        return self._query(tr, command, *([ self.encode_from_native(k) for k in keys ] + [self._encode_int(timeout)]), set_blocking=True)

    @_command
    @asyncio.coroutine
    def brpoplpush(self, tr, source:NativeType, destination:NativeType, timeout:int=0) -> NativeType:
        """ Pop a value from a list, push it to another list and return it; or block until one is available """
        result = yield from self._query(tr, b'brpoplpush', self.encode_from_native(source), self.encode_from_native(destination),
                    self._encode_int(timeout), set_blocking=True)

        if result is None:
            raise TimeoutError('Timeout in brpoplpush')
        else:
            assert isinstance(result, bytes)
            return self.decode_to_native(result)

    @_query_command
    def lset(self, tr, key:NativeType, index:int, value:NativeType) -> StatusReply:
        """ Set the value of an element in a list by its index. """
        return self._query(tr, b'lset', self.encode_from_native(key), self._encode_int(index), self.encode_from_native(value))

    @_query_command
    def linsert(self, tr, key:NativeType, pivot:NativeType, value:NativeType, before=False) -> int:
        """ Insert an element before or after another element in a list """
        return self._query(tr, b'linsert', self.encode_from_native(key), (b'BEFORE' if before else b'AFTER'),
                self.encode_from_native(pivot), self.encode_from_native(value))

    # Sorted Sets

    @_query_command
    def zadd(self, tr, key:NativeType, values:dict, only_if_not_exists=False, only_if_exists=False, return_num_changed=False) -> int:
        """
        Add one or more members to a sorted set, or update its score if it already exists

        ::

            yield protocol.zadd('myzset', { 'key': 4, 'key2': 5 })
        """

        options = [ ]

        assert not (only_if_not_exists and only_if_exists)
        if only_if_not_exists:
            options.append(b'NX')
        elif only_if_exists:
            options.append(b'XX')

        if return_num_changed:
            options.append(b'CH')

        data = [ ]
        for k,score in values.items():
            assert isinstance(k, self.native_type)
            assert isinstance(score, (int, float))

            data.append(self._encode_float(score))
            data.append(self.encode_from_native(k))

        return self._query(tr, b'zadd', self.encode_from_native(key), *(options + data))

    @_query_command
    def zrange(self, tr, key:NativeType, start:int=0, stop:int=-1) -> ZRangeReply:
        """
        Return a range of members in a sorted set, by index.

        You can do the following to receive the slice of the sorted set as a
        python dict (mapping the keys to their scores):

        ::

            result = yield protocol.zrange('myzset', start=10, stop=20)
            my_dict = yield result.asdict()

        or the following to retrieve it as a list of keys:

        ::

            result = yield protocol.zrange('myzset', start=10, stop=20)
            my_dict = yield result.aslist()
        """
        return self._query(tr, b'zrange', self.encode_from_native(key),
                    self._encode_int(start), self._encode_int(stop), b'withscores')

    @_query_command
    def zrevrange(self, tr, key:NativeType, start:int=0, stop:int=-1) -> ZRangeReply:
        """
        Return a range of members in a reversed sorted set, by index.

        You can do the following to receive the slice of the sorted set as a
        python dict (mapping the keys to their scores):

        ::

            my_dict = yield protocol.zrevrange_asdict('myzset', start=10, stop=20)

        or the following to retrieve it as a list of keys:

        ::

            zrange_reply = yield protocol.zrevrange('myzset', start=10, stop=20)
            my_dict = yield zrange_reply.aslist()

        """
        return self._query(tr, b'zrevrange', self.encode_from_native(key),
                    self._encode_int(start), self._encode_int(stop), b'withscores')

    @_query_command
    def zrangebyscore(self, tr, key:NativeType,
                min:ZScoreBoundary=ZScoreBoundary.MIN_VALUE,
                max:ZScoreBoundary=ZScoreBoundary.MAX_VALUE,
                offset:int=0, limit:int=-1) -> ZRangeReply:
        """ Return a range of members in a sorted set, by score """
        return self._query(tr, b'zrangebyscore', self.encode_from_native(key),
                    self._encode_zscore_boundary(min), self._encode_zscore_boundary(max),
                    b'limit', self._encode_int(offset), self._encode_int(limit),
                    b'withscores')

    @_query_command
    def zrevrangebyscore(self, tr, key:NativeType,
                max:ZScoreBoundary=ZScoreBoundary.MAX_VALUE,
                min:ZScoreBoundary=ZScoreBoundary.MIN_VALUE,
                offset:int=0, limit:int=-1) -> ZRangeReply:
        """ Return a range of members in a sorted set, by score, with scores ordered from high to low """
        return self._query(tr, b'zrevrangebyscore', self.encode_from_native(key),
                    self._encode_zscore_boundary(max), self._encode_zscore_boundary(min),
                    b'limit', self._encode_int(offset), self._encode_int(limit),
                    b'withscores')

    @_query_command
    def zremrangebyscore(self, tr, key:NativeType,
                min:ZScoreBoundary=ZScoreBoundary.MIN_VALUE,
                max:ZScoreBoundary=ZScoreBoundary.MAX_VALUE) -> int:
        """ Remove all members in a sorted set within the given scores """
        return self._query(tr, b'zremrangebyscore', self.encode_from_native(key),
                    self._encode_zscore_boundary(min), self._encode_zscore_boundary(max))

    @_query_command
    def zremrangebyrank(self, tr, key:NativeType, min:int=0, max:int=-1) -> int:
        """ Remove all members in a sorted set within the given indexes """
        return self._query(tr, b'zremrangebyrank', self.encode_from_native(key),
                    self._encode_int(min), self._encode_int(max))

    @_query_command
    def zcount(self, tr, key:NativeType, min:ZScoreBoundary, max:ZScoreBoundary) -> int:
        """ Count the members in a sorted set with scores within the given values """
        return self._query(tr, b'zcount', self.encode_from_native(key),
                    self._encode_zscore_boundary(min), self._encode_zscore_boundary(max))

    @_query_command
    def zscore(self, tr, key:NativeType, member:NativeType) -> (float, NoneType):
        """ Get the score associated with the given member in a sorted set """
        return self._query(tr, b'zscore', self.encode_from_native(key), self.encode_from_native(member))

    @_query_command
    def zunionstore(self, tr, destination:NativeType, keys:ListOf(NativeType), weights:(NoneType,ListOf(float))=None,
                                    aggregate=ZAggregate.SUM) -> int:
        """ Add multiple sorted sets and store the resulting sorted set in a new key """
        return self._zstore(tr, b'zunionstore', destination, keys, weights, aggregate)

    @_query_command
    def zinterstore(self, tr, destination:NativeType, keys:ListOf(NativeType), weights:(NoneType,ListOf(float))=None,
                                    aggregate=ZAggregate.SUM) -> int:
        """ Intersect multiple sorted sets and store the resulting sorted set in a new key """
        return self._zstore(tr, b'zinterstore', destination, keys, weights, aggregate)

    def _zstore(self, tr, command, destination, keys, weights, aggregate):
        """ Common part for zunionstore and zinterstore. """
        numkeys = len(keys)
        if weights is None:
            weights = [1] * numkeys

        return self._query(tr, *
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

    @_query_command
    def zcard(self, tr, key:NativeType) -> int:
        """ Get the number of members in a sorted set """
        return self._query(tr, b'zcard', self.encode_from_native(key))

    @_query_command
    def zrank(self, tr, key:NativeType, member:NativeType) -> (int, NoneType):
        """ Determine the index of a member in a sorted set """
        return self._query(tr, b'zrank', self.encode_from_native(key), self.encode_from_native(member))

    @_query_command
    def zrevrank(self, tr, key:NativeType, member:NativeType) -> (int, NoneType):
        """ Determine the index of a member in a sorted set, with scores ordered from high to low """
        return self._query(tr, b'zrevrank', self.encode_from_native(key), self.encode_from_native(member))

    @_query_command
    def zincrby(self, tr, key:NativeType, increment:float, member:NativeType, only_if_exists=False) -> (float, NoneType):
        """ Increment the score of a member in a sorted set """

        if only_if_exists:
            return self._query(tr, b'zadd', self.encode_from_native(key), b'xx', b'incr',
                    self._encode_float(increment), self.encode_from_native(member))
        else:
            return self._query(tr, b'zincrby', self.encode_from_native(key),
                    self._encode_float(increment), self.encode_from_native(member))

    @_query_command
    def zrem(self, tr, key:NativeType, members:ListOf(NativeType)) -> int:
        """ Remove one or more members from a sorted set """
        return self._query(tr, b'zrem', self.encode_from_native(key), *map(self.encode_from_native, members))

    # Hashes

    @_query_command
    def hset(self, tr, key:NativeType, field:NativeType, value:NativeType) -> int:
        """ Set the string value of a hash field """
        return self._query(tr, b'hset', self.encode_from_native(key), self.encode_from_native(field), self.encode_from_native(value))

    @_query_command
    def hmset(self, tr, key:NativeType, values:dict) -> StatusReply:
        """ Set multiple hash fields to multiple values """
        data = [ ]
        for k,v in values.items():
            assert isinstance(k, self.native_type)
            assert isinstance(v, self.native_type)

            data.append(self.encode_from_native(k))
            data.append(self.encode_from_native(v))

        return self._query(tr, b'hmset', self.encode_from_native(key), *data)

    @_query_command
    def hsetnx(self, tr, key:NativeType, field:NativeType, value:NativeType) -> int:
        """ Set the value of a hash field, only if the field does not exist """
        return self._query(tr, b'hsetnx', self.encode_from_native(key), self.encode_from_native(field), self.encode_from_native(value))

    @_query_command
    def hdel(self, tr, key:NativeType, fields:ListOf(NativeType)) -> int:
        """ Delete one or more hash fields """
        return self._query(tr, b'hdel', self.encode_from_native(key), *map(self.encode_from_native, fields))

    @_query_command
    def hget(self, tr, key:NativeType, field:NativeType) -> (NativeType, NoneType):
        """ Get the value of a hash field """
        return self._query(tr, b'hget', self.encode_from_native(key), self.encode_from_native(field))

    @_query_command
    def hexists(self, tr, key:NativeType, field:NativeType) -> bool:
        """ Returns if field is an existing field in the hash stored at key. """
        return self._query(tr, b'hexists', self.encode_from_native(key), self.encode_from_native(field))

    @_query_command
    def hkeys(self, tr, key:NativeType) -> SetReply:
        """ Get all the keys in a hash. (Returns a set) """
        return self._query(tr, b'hkeys', self.encode_from_native(key))

    @_query_command
    def hvals(self, tr, key:NativeType) -> ListReply:
        """ Get all the values in a hash. (Returns a list) """
        return self._query(tr, b'hvals', self.encode_from_native(key))

    @_query_command
    def hlen(self, tr, key:NativeType) -> int:
        """ Returns the number of fields contained in the hash stored at key. """
        return self._query(tr, b'hlen', self.encode_from_native(key))

    @_query_command
    def hgetall(self, tr, key:NativeType) -> DictReply:
        """ Get the value of a hash field """
        return self._query(tr, b'hgetall', self.encode_from_native(key))

    @_query_command
    def hmget(self, tr, key:NativeType, fields:ListOf(NativeType)) -> ListReply:
        """ Get the values of all the given hash fields """
        return self._query(tr, b'hmget', self.encode_from_native(key), *map(self.encode_from_native, fields))

    @_query_command
    def hincrby(self, tr, key:NativeType, field:NativeType, increment) -> int:
        """ Increment the integer value of a hash field by the given number
        Returns: the value at field after the increment operation. """
        assert isinstance(increment, int)
        return self._query(tr, b'hincrby', self.encode_from_native(key), self.encode_from_native(field), self._encode_int(increment))

    @_query_command
    def hincrbyfloat(self, tr, key:NativeType, field:NativeType, increment:(int,float)) -> float:
        """ Increment the float value of a hash field by the given amount
        Returns: the value at field after the increment operation. """
        return self._query(tr, b'hincrbyfloat', self.encode_from_native(key), self.encode_from_native(field), self._encode_float(increment))

    # Pubsub
    # (subscribe, unsubscribe, etc... should be called through the Subscription class.)

    @_command
    def start_subscribe(self, tr, *a) -> 'Subscription':
        """
        Start a pubsub listener.

        ::

            # Create subscription
            subscription = yield from protocol.start_subscribe()
            yield from subscription.subscribe(['key'])
            yield from subscription.psubscribe(['pattern*'])

            while True:
                result = yield from subscription.next_published()
                print(result)

        :returns: :class:`~asyncio_redis.Subscription`
        """
        # (Make coroutine. @asyncio.coroutine breaks documentation. It uses
        # @functools.wraps to make a generator for this function. But _command
        # will no longer be able to read the signature.)
        if False: yield

        if self.in_use:
            raise Error('Cannot start pubsub listener when a protocol is in use.')

        subscription = Subscription(self)

        self._in_pubsub = True
        self._subscription = subscription
        return subscription

    @_command
    def _subscribe(self, tr, channels:ListOf(NativeType)) -> NoneType:
        """ Listen for messages published to the given channels """
        self._pubsub_channels |= set(channels)
        return self._pubsub_method('subscribe', channels)

    @_command
    def _unsubscribe(self, tr, channels:ListOf(NativeType)) -> NoneType:
        """ Stop listening for messages posted to the given channels """
        self._pubsub_channels -= set(channels)
        return self._pubsub_method('unsubscribe', channels)

    @_command
    def _psubscribe(self, tr, patterns:ListOf(NativeType)) -> NoneType:
        """ Listen for messages published to channels matching the given patterns """
        self._pubsub_patterns |= set(patterns)
        return self._pubsub_method('psubscribe', patterns)

    @_command
    def _punsubscribe(self, tr, patterns:ListOf(NativeType)) -> NoneType: # XXX: unittest
        """ Stop listening for messages posted to channels matching the given patterns """
        self._pubsub_patterns -= set(patterns)
        return self._pubsub_method('punsubscribe', patterns)

    @asyncio.coroutine
    def _pubsub_method(self, method, params):
        if not self._in_pubsub:
            raise Error('Cannot call pubsub methods without calling start_subscribe')

        # Send
        self._send_command([method.encode('ascii')] + list(map(self.encode_from_native, params)))

        # Note that we can't use `self._query` here. The reason is that one
        # subscribe/unsubscribe command returns a separate answer for every
        # parameter. It doesn't fit in the same model of all the other queries
        # where one query puts a Future on the queue that is replied with the
        # incoming answer.
        # Redis returns something like [ 'subscribe', 'channel_name', 1] for
        # each parameter, but we can safely ignore those replies that.

    @_query_command
    def publish(self, tr, channel:NativeType, message:NativeType) -> int:
        """ Post a message to a channel
        (Returns the number of clients that received this message.) """
        return self._query(tr, b'publish', self.encode_from_native(channel), self.encode_from_native(message))

    @_query_command
    def pubsub_channels(self, tr, pattern:(NativeType, NoneType)=None) -> ListReply:
        """
        Lists the currently active channels. An active channel is a Pub/Sub
        channel with one ore more subscribers (not including clients subscribed
        to patterns).
        """
        return self._query(tr, b'pubsub', b'channels',
                    (self.encode_from_native(pattern) if pattern else b'*'))

    @_query_command
    def pubsub_numsub(self, tr, channels:ListOf(NativeType)) -> DictReply:
        """Returns the number of subscribers (not counting clients subscribed
        to patterns) for the specified channels.  """
        return self._query(tr, b'pubsub', b'numsub', *[ self.encode_from_native(c) for c in channels ])

    @_query_command
    def pubsub_numpat(self, tr) -> int:
        """ Returns the number of subscriptions to patterns (that are performed
        using the PSUBSCRIBE command). Note that this is not just the count of
        clients subscribed to patterns but the total number of patterns all the
        clients are subscribed to. """
        return self._query(tr, b'pubsub', b'numpat')

    # Server

    @_query_command
    def ping(self, tr) -> StatusReply:
        """ Ping the server (Returns PONG) """
        return self._query(tr, b'ping')

    @_query_command
    def echo(self, tr, string:NativeType) -> NativeType:
        """ Echo the given string """
        return self._query(tr, b'echo', self.encode_from_native(string))

    @_query_command
    def save(self, tr) -> StatusReply:
        """ Synchronously save the dataset to disk """
        return self._query(tr, b'save')

    @_query_command
    def bgsave(self, tr) -> StatusReply:
        """ Asynchronously save the dataset to disk """
        return self._query(tr, b'bgsave')

    @_query_command
    def bgrewriteaof(self, tr) -> StatusReply:
        """ Asynchronously rewrite the append-only file """
        return self._query(tr, b'bgrewriteaof')

    @_query_command
    def lastsave(self, tr) -> int:
        """ Get the UNIX time stamp of the last successful save to disk """
        return self._query(tr, b'lastsave')

    @_query_command
    def dbsize(self, tr) -> int:
        """ Return the number of keys in the currently-selected database. """
        return self._query(tr, b'dbsize')

    @_query_command
    def flushall(self, tr) -> StatusReply:
        """ Remove all keys from all databases """
        return self._query(tr, b'flushall')

    @_query_command
    def flushdb(self, tr) -> StatusReply:
        """ Delete all the keys of the currently selected DB. This command never fails. """
        return self._query(tr, b'flushdb')

#    @_query_command
#    def object(self, subcommand, args):
#        """ Inspect the internals of Redis objects """
#        raise NotImplementedError

    @_query_command
    def type(self, tr, key:NativeType) -> StatusReply:
        """ Determine the type stored at key """
        return self._query(tr, b'type', self.encode_from_native(key))

    @_query_command
    def config_set(self, tr, parameter:str, value:str) -> StatusReply:
        """ Set a configuration parameter to the given value """
        return self._query(tr, b'config', b'set', self.encode_from_native(parameter),
                        self.encode_from_native(value))

    @_query_command
    def config_get(self, tr, parameter:str) -> ConfigPairReply:
        """ Get the value of a configuration parameter """
        return self._query(tr, b'config', b'get', self.encode_from_native(parameter))

    @_query_command
    def config_rewrite(self, tr) -> StatusReply:
        """ Rewrite the configuration file with the in memory configuration """
        return self._query(tr, b'config', b'rewrite')

    @_query_command
    def config_resetstat(self, tr) -> StatusReply:
        """ Reset the stats returned by INFO """
        return self._query(tr, b'config', b'resetstat')

    @_query_command
    def info(self, tr, section:(NativeType, NoneType)=None) -> InfoReply:
        """ Get information and statistics about the server """
        if section is None:
            return self._query(tr, b'info')
        else:
            return self._query(tr, b'info', self.encode_from_native(section))

    @_query_command
    def shutdown(self, tr, save=False) -> StatusReply:
        """ Synchronously save the dataset to disk and then shut down the server """
        return self._query(tr, b'shutdown', (b'save' if save else b'nosave'))

    @_query_command
    def client_getname(self, tr) -> NativeType:
        """ Get the current connection name """
        return self._query(tr, b'client', b'getname')

    @_query_command
    def client_setname(self, tr, name) -> StatusReply:
        """ Set the current connection name """
        return self._query(tr, b'client', b'setname', self.encode_from_native(name))

    @_query_command
    def client_list(self, tr) -> ClientListReply:
        """ Get the list of client connections """
        return self._query(tr, b'client', b'list')

    @_query_command
    def client_kill(self, tr, address:str) -> StatusReply:
        """
        Kill the connection of a client
        `address` should be an "ip:port" string.
        """
        return self._query(tr, b'client', b'kill', address.encode('utf-8'))

    # LUA scripting

    @_command
    @asyncio.coroutine
    def register_script(self, tr, script:str) -> 'Script':
        """
        Register a LUA script.

        ::

            script = yield from protocol.register_script(lua_code)
            result = yield from script.run(keys=[...], args=[...])
        """
        # The register_script APi was made compatible with the redis.py library:
        # https://github.com/andymccurdy/redis-py
        sha = yield from self.script_load(tr, script)
        return Script(sha, script, lambda:self.evalsha)

    @_query_command
    def script_exists(self, tr, shas:ListOf(str)) -> ListOf(bool):
        """ Check existence of scripts in the script cache. """
        return self._query(tr, b'script', b'exists', *[ sha.encode('ascii') for sha in shas ])

    @_query_command
    def script_flush(self, tr) -> StatusReply:
        """ Remove all the scripts from the script cache. """
        return self._query(tr, b'script', b'flush')

    @_query_command
    @asyncio.coroutine
    def script_kill(self, tr) -> StatusReply:
        """
        Kill the script currently in execution.  This raises
        :class:`~asyncio_redis.exceptions.NoRunningScriptError` when there are no
        scrips running.
        """
        try:
            return (yield from self._query(tr, b'script', b'kill'))
        except ErrorReply as e:
            if 'NOTBUSY' in e.args[0]:
                raise NoRunningScriptError
            else:
                raise

    @_query_command
    @asyncio.coroutine
    def evalsha(self, tr, sha:str,
                        keys:(ListOf(NativeType), NoneType)=None,
                        args:(ListOf(NativeType), NoneType)=None) -> EvalScriptReply:
        """
        Evaluates a script cached on the server side by its SHA1 digest.
        Scripts are cached on the server side using the SCRIPT LOAD command.

        The return type/value depends on the script.

        This will raise a :class:`~asyncio_redis.exceptions.ScriptKilledError`
        exception if the script was killed.
        """
        if not keys: keys = []
        if not args: args = []

        try:
            result = yield from self._query(tr, b'evalsha', sha.encode('ascii'),
                        self._encode_int(len(keys)),
                        *map(self.encode_from_native, keys + args))

            return result
        except ErrorReply:
            raise ScriptKilledError

    @_query_command
    def script_load(self, tr, script:str) -> str:
        """ Load script, returns sha1 """
        return self._query(tr, b'script', b'load', script.encode('utf-8'))

    # Scanning

    @_command
    def scan(self, tr, match:(NativeType, NoneType)=None) -> Cursor:
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

        It's possible to alter the COUNT-parameter, by assigning a value to
        ``cursor.count``, before calling ``fetchone`` or ``fetchall``. For
        instance:

        ::

            cursor.count = 100

        Also see: :func:`~asyncio_redis.RedisProtocol.sscan`,
        :func:`~asyncio_redis.RedisProtocol.hscan` and
        :func:`~asyncio_redis.RedisProtocol.zscan`

        Redis reference: http://redis.io/commands/scan
        """
        if False: yield

        def scanfunc(cursor, count):
            return self._scan(tr, cursor, match, count)

        return Cursor(name='scan(match=%r)' % match, scanfunc=scanfunc)

    @_query_command
    def _scan(self, tr, cursor:int, match:(NativeType,NoneType), count:int) -> _ScanPart:
        match = b'*' if match is None else self.encode_from_native(match)

        return self._query(tr, b'scan', self._encode_int(cursor),
                    b'match', match,
                    b'count', self._encode_int(count))

    @_command
    def sscan(self, tr, key:NativeType, match:(NativeType,NoneType)=None) -> SetCursor:
        """
        Incrementally iterate set elements

        Also see: :func:`~asyncio_redis.RedisProtocol.scan`
        """
        if False: yield
        name = 'sscan(key=%r match=%r)' % (key, match)

        def scan(cursor, count):
            return self._do_scan(tr, b'sscan', key, cursor, match, count)

        return SetCursor(name=name, scanfunc=scan)

    @_command
    def hscan(self, tr, key:NativeType, match:(NativeType,NoneType)=None) -> DictCursor:
        """
        Incrementally iterate hash fields and associated values
        Also see: :func:`~asyncio_redis.RedisProtocol.scan`
        """
        if False: yield
        name = 'hscan(key=%r match=%r)' % (key, match)

        def scan(cursor, count):
            return self._do_scan(tr, b'hscan', key, cursor, match, count)

        return DictCursor(name=name, scanfunc=scan)

    @_command
    def zscan(self, tr, key:NativeType, match:(NativeType,NoneType)=None) -> DictCursor:
        """
        Incrementally iterate sorted sets elements and associated scores
        Also see: :func:`~asyncio_redis.RedisProtocol.scan`
        """
        if False: yield
        name = 'zscan(key=%r match=%r)' % (key, match)

        def scan(cursor, count):
            return self._do_scan(b'zscan', key, cursor, match, count)

        return ZCursor(name=name, scanfunc=scan)

    @_query_command
    def _do_scan(self, tr, verb:bytes, key:NativeType, cursor:int, match:(NativeType,NoneType), count:int) -> _ScanPart:
        match = b'*' if match is None else self.encode_from_native(match)

        return self._query(tr, verb, self.encode_from_native(key),
                self._encode_int(cursor),
                b'match', match,
                b'count', self._encode_int(count))

    # Transaction
    @_command
    @asyncio.coroutine
    def watch(self, tr, keys:ListOf(NativeType)) -> NoneType:
        """
        Watch keys.

        ::

            # Watch keys for concurrent updates
            yield from protocol.watch(['key', 'other_key'])

            value = yield from protocol.get('key')
            another_value = yield from protocol.get('another_key')

            transaction = yield from protocol.multi()

            f1 = yield from transaction.set('key', another_value)
            f2 = yield from transaction.set('another_key', value)

            # Commit transaction
            yield from transaction.exec()

            # Retrieve results
            yield from f1
            yield from f2

        """
        return self._watch(tr, keys)

    @asyncio.coroutine
    def _watch(self, tr, keys:ListOf(NativeType)) -> NoneType:
        result = yield from self._query(tr, b'watch', *map(self.encode_from_native, keys), _bypass=True)
        assert result == b'OK'

    @_command
    @asyncio.coroutine
    def multi(self, tr, watch:(ListOf(NativeType),NoneType)=None) -> 'Transaction':
        """
        Start of transaction.

        ::

            transaction = yield from protocol.multi()

            # Run commands in transaction
            f1 = yield from transaction.set('key', 'value')
            f2 = yield from transaction.set('another_key', 'another_value')

            # Commit transaction
            yield from transaction.exec()

            # Retrieve results (you can also use asyncio.tasks.gather)
            result1 = yield from f1
            result2 = yield from f2

        :returns: A :class:`asyncio_redis.Transaction` instance.
        """
        # Create transaction object.
        if tr != _NoTransaction:
            raise Error('Multi calls can not be nested.')
        else:
            yield from self._transaction_lock.acquire()
            tr = Transaction(self)
            self._transaction = tr

        # Call watch
        if watch is not None:
            yield from self._watch(tr, watch)
#        yield from asyncio.sleep(.015)

        # Call multi
        result = yield from self._query(tr, b'multi', _bypass=True)
        assert result == b'OK'

        self._transaction_response_queue = deque()

        return tr

    @asyncio.coroutine
    def _exec(self, tr):
        """
        Execute all commands issued after MULTI
        """
        if not self._transaction or self._transaction != tr:
            raise Error('Not in transaction')
        try:
            futures_and_postprocessors = self._transaction_response_queue
            self._transaction_response_queue = None

            # Get transaction answers.
            multi_bulk_reply = yield from self._query(tr, b'exec', _bypass=True)

            if multi_bulk_reply is None:
                # We get None when a transaction failed.
                raise TransactionError('Transaction failed.')
            else:
                assert isinstance(multi_bulk_reply, MultiBulkReply)

            for f in multi_bulk_reply.iter_raw():
                answer = yield from f
                f2, call = futures_and_postprocessors.popleft()

                if isinstance(answer, Exception):
                    f2.set_exception(answer)
                else:
                    if call:
                        self._pipelined_calls.remove(call)

                    f2.set_result(answer)
        finally:
            self._transaction_response_queue = deque()
            self._transaction = None
            self._transaction_lock.release()

    @asyncio.coroutine
    def _discard(self, tr):
        """
        Discard all commands issued after MULTI
        """
        if not self._transaction or self._transaction != tr:
            raise Error('Not in transaction')

        try:
            result = yield from self._query(tr, b'discard', _bypass=True)
            assert result == b'OK'
        finally:
            self._transaction_response_queue = deque()
            self._transaction = None
            self._transaction_lock.release()

    @asyncio.coroutine
    def _unwatch(self, tr):
        """
        Forget about all watched keys
        """
        if not self._transaction or self._transaction != tr:
            raise Error('Not in transaction')

        result = yield from self._query(tr, b'unwatch')   # XXX: should be _bypass???
        assert result == b'OK'


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

            script_reply = yield from script.run(keys=[], args=[])

            # If the LUA script returns something, retrieve the return value
            result = yield from script_reply.return_value()

        This will raise a :class:`~asyncio_redis.exceptions.ScriptKilledError`
        exception if the script was killed.
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
            raise AttributeError(name)

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
        return self._protocol._discard(self)

    def exec(self):
        """
        Execute transaction.

        This can raise a :class:`~asyncio_redis.exceptions.TransactionError`
        when the transaction fails.
        """
        return self._protocol._exec(self)

    def unwatch(self): # XXX: test
        """
        Forget about all watched keys
        """
        return self._protocol._unwatch(self)


class Subscription:
    """
    Pubsub subscription
    """
    def __init__(self, protocol):
        self.protocol = protocol
        self._messages_queue = Queue(loop=protocol._loop) # Pubsub queue

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

    @asyncio.coroutine
    def next_published(self):
        """
        Coroutine which waits for next pubsub message to be received and
        returns it.

        :returns: instance of :class:`PubSubReply <asyncio_redis.replies.PubSubReply>`
        """
        return (yield from self._messages_queue.get())


class HiRedisProtocol(RedisProtocol, metaclass=_RedisProtocolMeta):
    """
    Protocol implementation that uses the `hiredis` library for parsing the
    incoming data. This will be faster in many cases, but not necessarily
    always.

    It does not (yet) support streaming of multibulk replies, which means that
    you won't see the first item of a multi bulk reply, before the whole
    response has been parsed.
    """
    def __init__(self, *, password=None, db=0, encoder=None,
                 connection_lost_callback=None, enable_typechecking=True,
                 loop=None):
        super().__init__(password=password,
                         db=db,
                         encoder=encoder,
                         connection_lost_callback=connection_lost_callback,
                         enable_typechecking=enable_typechecking,
                         loop=loop)
        self._hiredis = None
        assert hiredis, "`hiredis` libary not available. Please don't use HiRedisProtocol."

    def connection_made(self, transport):
        super().connection_made(transport)
        self._hiredis = hiredis.Reader()

    def data_received(self, data):
        # Move received data to hiredis parser
        self._hiredis.feed(data)

        while True:
            item = self._hiredis.gets()

            if item is not False:
                self._process_hiredis_item(item, self._push_answer)
            else:
                break

    def _process_hiredis_item(self, item, cb):
        if isinstance(item, (bytes, int)):
            cb(item)
        elif isinstance(item, list):
            reply = MultiBulkReply(self, len(item), loop=self._loop)

            for i in item:
                self._process_hiredis_item(i, reply._feed_received)

            cb(reply)
        elif isinstance(item, hiredis.ReplyError):
            cb(ErrorReply(item.args[0]))
        elif isinstance(item, NoneType):
            cb(item)

    @asyncio.coroutine
    def _reader_coroutine(self):
        # We don't need this one.
        return
