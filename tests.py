#!/usr/bin/env python

from asyncio.futures import Future
from asyncio.tasks import gather

from asyncio_redis import (
        Connection,
        Error,
        ErrorReply,
        NoAvailableConnectionsInPoolError,
        NoRunningScriptError,
        NotConnectedError,
        Pool,
        RedisProtocol,
        Script,
        ScriptKilledError,
        Subscription,
        Transaction,
        TransactionError,
        ZScoreBoundary,
)
from asyncio_redis.replies import (
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
from asyncio_redis.exceptions import TimeoutError
from asyncio_redis.cursors import Cursor
from asyncio_redis.encoders import BytesEncoder

import asyncio
import unittest
import os

PORT = int(os.environ.get('REDIS_PORT', 6379))
HOST = os.environ.get('REDIS_HOST', 'localhost')


@asyncio.coroutine
def connect(loop, protocol=RedisProtocol):
    transport, protocol = yield from loop.create_connection(lambda: protocol(loop=loop), HOST, PORT)
    return transport, protocol


def redis_test(function):
    """
    Decorator for methods (which are coroutines) in RedisProtocolTest

    Wraps the coroutine inside `run_until_complete`.
    """
    function = asyncio.coroutine(function)

    def wrapper(self):
        @asyncio.coroutine
        def c():
            # Create connection
            transport, protocol = yield from connect(self.loop, self.protocol_class)

            # Run test
            yield from function(self, transport, protocol)

            # Close connection
            transport.close()

        self.loop.run_until_complete(c())
    return wrapper


class RedisProtocolTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.protocol_class = RedisProtocol

    def tearDown(self):
        pass

    @redis_test
    def test_ping(self, transport, protocol):
        result = yield from protocol.ping()
        self.assertEqual(result, StatusReply('PONG'))
        self.assertEqual(repr(result), u"StatusReply(status='PONG')")

    @redis_test
    def test_echo(self, transport, protocol):
        result = yield from protocol.echo(u'my string')
        self.assertEqual(result, u'my string')

    @redis_test
    def test_set_and_get(self, transport, protocol):
        # Set
        value = yield from protocol.set(u'my_key', u'my_value')
        self.assertEqual(value, StatusReply('OK'))

        # Get
        value = yield from protocol.get(u'my_key')
        self.assertEqual(value, u'my_value')

        # Getset
        value = yield from protocol.getset(u'my_key', u'new_value')
        self.assertEqual(value, u'my_value')

        value = yield from protocol.get(u'my_key')
        self.assertEqual(value, u'new_value')

    @redis_test
    def test_extended_set(self, transport, protocol):
        yield from protocol.delete([u'my_key', u'other_key'])
        # set with expire only if not exists
        value = yield from protocol.set(u'my_key', u'my_value',
                                        expire=10, only_if_not_exists=True)
        self.assertEqual(value, StatusReply('OK'))
        value = yield from protocol.ttl(u'my_key')
        self.assertIn(value, (10, 9))

        # check NX flag for SET command
        value = yield from protocol.set(u'my_key', u'my_value',
                                        expire=10, only_if_not_exists=True)
        self.assertIsNone(value)

        # check XX flag for SET command
        value = yield from protocol.set(u'other_key', 'some_value', only_if_exists=True)

        self.assertIsNone(value)

        # set with pexpire only if key exists
        value = yield from protocol.set(u'my_key', u'other_value',
                                        pexpire=20000, only_if_exists=True)
        self.assertEqual(value, StatusReply('OK'))

        value = yield from protocol.get(u'my_key')

        self.assertEqual(value, u'other_value')

        value = yield from protocol.ttl(u'my_key')
        self.assertIn(value, (20, 19))

    @redis_test
    def test_setex(self, transport, protocol):
        # Set
        value = yield from protocol.setex(u'my_key', 10, u'my_value')
        self.assertEqual(value, StatusReply('OK'))

        # TTL
        value = yield from protocol.ttl(u'my_key')
        self.assertIn(value, (10, 9)) # may be some delay

        # Get
        value = yield from protocol.get(u'my_key')
        self.assertEqual(value, u'my_value')

    @redis_test
    def test_setnx(self, transport, protocol):
        yield from protocol.delete([u'my_key'])

        # Setnx while key does not exists
        value = yield from protocol.setnx(u'my_key', u'my_value')
        self.assertEqual(value, True)

        # Get
        value = yield from protocol.get(u'my_key')
        self.assertEqual(value, u'my_value')

        # Setnx if key exists
        value = yield from protocol.setnx(u'my_key', u'other_value')
        self.assertEqual(value, False)

        # Get old value
        value = yield from protocol.get(u'my_key')
        self.assertEqual(value, u'my_value')

    @redis_test
    def test_special_characters(self, transport, protocol):
        # Test some special unicode values and spaces.
        value = u'my value with special chars " # éçåø´¨åø´h '

        result = yield from protocol.set(u'my key with spaces', value)
        result = yield from protocol.get(u'my key with spaces')
        self.assertEqual(result, value)

        # Test newlines
        value = u'ab\ncd\ref\r\ngh'
        result = yield from protocol.set(u'my-key', value)
        result = yield from protocol.get(u'my-key')
        self.assertEqual(result, value)

    @redis_test
    def test_mget(self, transport, protocol):
        # mget
        yield from protocol.set(u'my_key', u'a')
        yield from protocol.set(u'my_key2', u'b')
        result = yield from protocol.mget([ u'my_key', u'my_key2', u'not_exists'])
        self.assertIsInstance(result, ListReply)
        result = yield from result.aslist()
        self.assertEqual(result, [u'a', u'b', None])

    @redis_test
    def test_strlen(self, transport, protocol):
        yield from protocol.delete([ u'my_key' ])
        yield from protocol.delete([ u'my_key2' ])
        yield from protocol.delete([ u'my_key3' ])
        yield from protocol.set(u'my_key', u'my_value')
        yield from protocol.hset(u'my_key3', u'a', u'b')

        # strlen
        value = yield from protocol.strlen(u'my_key')
        self.assertEqual(value, len(u'my_value'))

        value = yield from protocol.strlen(u'my_key2')
        self.assertEqual(value, 0)

        with self.assertRaises(ErrorReply):
            yield from protocol.strlen(u'my_key3')
        # Redis exception: b'ERR Operation against a key holding the wrong kind of value')

    @redis_test
    def test_exists_and_delete(self, transport, protocol):
        # Set
        yield from protocol.set(u'my_key', u'aaa')
        value = yield from protocol.append(u'my_key', u'bbb')
        self.assertEqual(value, 6) # Total length
        value = yield from protocol.get(u'my_key')
        self.assertEqual(value, u'aaabbb')

    @redis_test
    def test_exists_and_delete2(self, transport, protocol):
        # Exists
        value = yield from protocol.exists(u'unknown_key')
        self.assertEqual(value, False)

        # Set
        value = yield from protocol.set(u'known_key', u'value')
        value = yield from protocol.exists(u'known_key')
        self.assertEqual(value, True)

        # Delete
        value = yield from protocol.set(u'known_key2', u'value')
        value = yield from protocol.delete([ u'known_key', u'known_key2' ])
        self.assertEqual(value, 2)

        value = yield from protocol.delete([ u'known_key' ])
        self.assertEqual(value, 0)

        value = yield from protocol.exists(u'known_key')
        self.assertEqual(value, False)

    @redis_test
    def test_rename(self, transport, protocol):
        # Set
        value = yield from protocol.set(u'old_key', u'value')
        value = yield from protocol.exists(u'old_key')
        self.assertEqual(value, True)

        # Rename
        value = yield from protocol.rename(u'old_key', u'new_key')
        self.assertEqual(value, StatusReply('OK'))

        value = yield from protocol.exists(u'old_key')
        self.assertEqual(value, False)
        value = yield from protocol.exists(u'new_key')
        self.assertEqual(value, True)

        value = yield from protocol.get(u'old_key')
        self.assertEqual(value, None)
        value = yield from protocol.get(u'new_key')
        self.assertEqual(value, 'value')

        # RenameNX
        yield from protocol.delete([ u'key3' ])
        value = yield from protocol.renamenx(u'new_key', u'key3')
        self.assertEqual(value, 1)

        yield from protocol.set(u'key4', u'existing-value')
        value = yield from protocol.renamenx(u'key3', u'key4')
        self.assertEqual(value, 0)

    @redis_test
    def test_expire(self, transport, protocol):
        # Set
        value = yield from protocol.set(u'key', u'value')

        # Expire (10s)
        value = yield from protocol.expire(u'key', 10)
        self.assertEqual(value, 1)

        value = yield from protocol.exists(u'key')
        self.assertEqual(value, True)

        # TTL
        value = yield from protocol.ttl(u'key')
        self.assertIsInstance(value, int)
        self.assertLessEqual(value, 10)

        # PTTL
        value = yield from protocol.pttl(u'key')
        self.assertIsInstance(value, int)
        self.assertLessEqual(value, 10 * 1000)

        # Pexpire
        value = yield from protocol.pexpire(u'key', 10*1000)
        self.assertEqual(value, 1) # XXX: check this
        value = yield from protocol.pttl(u'key')
        self.assertLessEqual(value, 10 * 1000)

        # Expire (1s) and wait
        value = yield from protocol.expire(u'key', 1)
        value = yield from protocol.exists(u'key')
        self.assertEqual(value, True)

        yield from asyncio.sleep(2, loop=self.loop)

        value = yield from protocol.exists(u'key')
        self.assertEqual(value, False)

        # Test persist
        yield from protocol.set(u'key', u'value')
        yield from protocol.expire(u'key', 1)
        value = yield from protocol.persist(u'key')
        self.assertEqual(value, 1)
        value = yield from protocol.persist(u'key')
        self.assertEqual(value, 0)

        yield from asyncio.sleep(2, loop=self.loop)

        value = yield from protocol.exists(u'key')
        self.assertEqual(value, True)

        # Test expireat
        value = yield from protocol.expireat(u'key', 1293840000)
        self.assertIsInstance(value, int)

        # Test pexpireat
        value = yield from protocol.pexpireat(u'key', 1555555555005)
        self.assertIsInstance(value, int)

    @redis_test
    def test_set(self, transport, protocol):
        # Create set
        value = yield from protocol.delete([ u'our_set' ])
        value = yield from protocol.sadd(u'our_set', [u'a', u'b'])
        value = yield from protocol.sadd(u'our_set', [u'c'])
        self.assertEqual(value, 1)

        # scard
        value = yield from protocol.scard(u'our_set')
        self.assertEqual(value, 3)

        # Smembers
        value = yield from protocol.smembers(u'our_set')
        self.assertIsInstance(value, SetReply)
        self.assertEqual(repr(value), u"SetReply(length=3)")
        value = yield from value.asset()
        self.assertEqual(value, { u'a', u'b', u'c' })

        # sismember
        value = yield from protocol.sismember(u'our_set', 'a')
        self.assertEqual(value, True)
        value = yield from protocol.sismember(u'our_set', 'd')
        self.assertEqual(value, False)

        # Intersection, union and diff
        yield from protocol.delete([ u'set2' ])
        yield from protocol.sadd(u'set2', [u'b', u'c', u'd', u'e'])

        value = yield from protocol.sunion([ u'our_set', 'set2' ])
        self.assertIsInstance(value, SetReply)
        value = yield from value.asset()
        self.assertEqual(value, set([u'a', u'b', u'c', u'd', u'e']))

        value = yield from protocol.sinter([ u'our_set', 'set2' ])
        value = yield from value.asset()
        self.assertEqual(value, set([u'b', u'c']))

        value = yield from protocol.sdiff([ u'our_set', 'set2' ])
        self.assertIsInstance(value, SetReply)
        value = yield from value.asset()
        self.assertEqual(value, set([u'a']))
        value = yield from protocol.sdiff([ u'set2', u'our_set' ])
        value = yield from value.asset()
        self.assertEqual(value, set([u'd', u'e']))

        # Interstore
        value = yield from protocol.sinterstore(u'result', [u'our_set', 'set2'])
        self.assertEqual(value, 2)
        value = yield from protocol.smembers(u'result')
        self.assertIsInstance(value, SetReply)
        value = yield from value.asset()
        self.assertEqual(value, set([u'b', u'c']))

        # Unionstore
        value = yield from protocol.sunionstore(u'result', [u'our_set', 'set2'])
        self.assertEqual(value, 5)
        value = yield from protocol.smembers(u'result')
        self.assertIsInstance(value, SetReply)
        value = yield from value.asset()
        self.assertEqual(value, set([u'a', u'b', u'c', u'd', u'e']))

        # Sdiffstore
        value = yield from protocol.sdiffstore(u'result', [u'set2', 'our_set'])
        self.assertEqual(value, 2)
        value = yield from protocol.smembers(u'result')
        self.assertIsInstance(value, SetReply)
        value = yield from value.asset()
        self.assertEqual(value, set([u'd', u'e']))

    @redis_test
    def test_srem(self, transport, protocol):
        yield from protocol.delete([ u'our_set' ])
        yield from protocol.sadd(u'our_set', [u'a', u'b', u'c', u'd'])

        # Call srem
        result = yield from protocol.srem(u'our_set', [u'b', u'c'])
        self.assertEqual(result, 2)

        result = yield from protocol.smembers(u'our_set')
        self.assertIsInstance(result, SetReply)
        result = yield from result.asset()
        self.assertEqual(result, set([u'a', u'd']))

    @redis_test
    def test_spop(self, transport, protocol):
        @asyncio.coroutine
        def setup():
            yield from protocol.delete([ u'my_set' ])
            yield from protocol.sadd(u'my_set', [u'value1'])
            yield from protocol.sadd(u'my_set', [u'value2'])

        # Test spop
        yield from setup()
        result = yield from protocol.spop(u'my_set')
        self.assertIn(result, [u'value1', u'value2'])
        result = yield from protocol.smembers(u'my_set')
        self.assertIsInstance(result, SetReply)
        result = yield from result.asset()
        self.assertEqual(len(result), 1)

        # Test srandmember
        yield from setup()
        result = yield from protocol.srandmember(u'my_set')
        self.assertIsInstance(result, SetReply)
        result = yield from result.asset()
        self.assertIn(list(result)[0], [u'value1', u'value2'])
        result = yield from protocol.smembers(u'my_set')
        self.assertIsInstance(result, SetReply)
        result = yield from result.asset()
        self.assertEqual(len(result), 2)

    @redis_test
    def test_type(self, transport, protocol):
        # Setup
        yield from protocol.delete([ u'key1' ])
        yield from protocol.delete([ u'key2' ])
        yield from protocol.delete([ u'key3' ])

        yield from protocol.set(u'key1', u'value')
        yield from protocol.lpush(u'key2', [u'value'])
        yield from protocol.sadd(u'key3', [u'value'])

        # Test types
        value = yield from protocol.type(u'key1')
        self.assertEqual(value, StatusReply('string'))

        value = yield from protocol.type(u'key2')
        self.assertEqual(value, StatusReply('list'))

        value = yield from protocol.type(u'key3')
        self.assertEqual(value, StatusReply('set'))

    @redis_test
    def test_list(self, transport, protocol):
        # Create list
        yield from protocol.delete([ u'my_list' ])
        value = yield from protocol.lpush(u'my_list', [u'v1', u'v2'])
        value = yield from protocol.rpush(u'my_list', [u'v3', u'v4'])
        self.assertEqual(value, 4)

        # lrange
        value = yield from protocol.lrange(u'my_list')
        self.assertIsInstance(value, ListReply)
        self.assertEqual(repr(value), u"ListReply(length=4)")
        value = yield from value.aslist()
        self.assertEqual(value, [ u'v2', 'v1', 'v3', 'v4'])

        # lset
        value = yield from protocol.lset(u'my_list', 3, 'new-value')
        self.assertEqual(value, StatusReply('OK'))

        value = yield from protocol.lrange(u'my_list')
        self.assertIsInstance(value, ListReply)
        value = yield from value.aslist()
        self.assertEqual(value, [ u'v2', 'v1', 'v3', 'new-value'])

        # lindex
        value = yield from protocol.lindex(u'my_list', 1)
        self.assertEqual(value, 'v1')
        value = yield from protocol.lindex(u'my_list', 10) # Unknown index
        self.assertEqual(value, None)

        # Length
        value = yield from protocol.llen(u'my_list')
        self.assertEqual(value, 4)

        # Remove element from list.
        value = yield from protocol.lrem(u'my_list', value=u'new-value')
        self.assertEqual(value, 1)

        # Pop
        value = yield from protocol.rpop(u'my_list')
        self.assertEqual(value, u'v3')
        value = yield from protocol.lpop(u'my_list')
        self.assertEqual(value, u'v2')
        value = yield from protocol.lpop(u'my_list')
        self.assertEqual(value, u'v1')
        value = yield from protocol.lpop(u'my_list')
        self.assertEqual(value, None)

        # Blocking lpop
        test_order = []

        @asyncio.coroutine
        def blpop():
            test_order.append('#1')
            value = yield from protocol.blpop([u'my_list'])
            self.assertIsInstance(value, BlockingPopReply)
            self.assertEqual(value.list_name, u'my_list')
            self.assertEqual(value.value, u'value')
            test_order.append('#3')
        f = asyncio.async(blpop(), loop=self.loop)

        transport2, protocol2 = yield from connect(self.loop)

        test_order.append('#2')
        yield from protocol2.rpush(u'my_list', [u'value'])
        yield from f
        self.assertEqual(test_order, ['#1', '#2', '#3'])

        # Blocking rpop
        @asyncio.coroutine
        def blpop():
            value = yield from protocol.brpop([u'my_list'])
            self.assertIsInstance(value, BlockingPopReply)
            self.assertEqual(value.list_name, u'my_list')
            self.assertEqual(value.value, u'value2')
        f = asyncio.async(blpop(), loop=self.loop)

        yield from protocol2.rpush(u'my_list', [u'value2'])
        yield from f

    @redis_test
    def test_brpoplpush(self, transport, protocol):
        yield from protocol.delete([ u'from' ])
        yield from protocol.delete([ u'to' ])
        yield from protocol.lpush(u'to', [u'1'])

        @asyncio.coroutine
        def brpoplpush():
            result = yield from protocol.brpoplpush(u'from', u'to')
            self.assertEqual(result, u'my_value')
        f = asyncio.async(brpoplpush(), loop=self.loop)

        transport2, protocol2 = yield from connect(self.loop)
        yield from protocol2.rpush(u'from', [u'my_value'])
        yield from f

    @redis_test
    def test_blocking_timeout(self, transport, protocol):
        yield from protocol.delete([u'from'])
        yield from protocol.delete([u'to'])

        # brpoplpush
        with self.assertRaises(TimeoutError) as e:
            result = yield from protocol.brpoplpush(u'from', u'to', 1)
        self.assertIn('Timeout in brpoplpush', e.exception.args[0])

        # brpop
        with self.assertRaises(TimeoutError) as e:
            result = yield from protocol.brpop([u'from'], 1)
        self.assertIn('Timeout in blocking pop', e.exception.args[0])

        # blpop
        with self.assertRaises(TimeoutError) as e:
            result = yield from protocol.blpop([u'from'], 1)
        self.assertIn('Timeout in blocking pop', e.exception.args[0])

    @redis_test
    def test_linsert(self, transport, protocol):
        # Prepare
        yield from protocol.delete([ u'my_list' ])
        yield from protocol.rpush(u'my_list', [u'1'])
        yield from protocol.rpush(u'my_list', [u'2'])
        yield from protocol.rpush(u'my_list', [u'3'])

        # Insert after
        result = yield from protocol.linsert(u'my_list', u'1', u'A')
        self.assertEqual(result, 4)
        result = yield from protocol.lrange(u'my_list')
        self.assertIsInstance(result, ListReply)
        result = yield from result.aslist()
        self.assertEqual(result, [u'1', u'A', u'2', u'3'])

        # Insert before
        result = yield from protocol.linsert(u'my_list', u'3', u'B', before=True)
        self.assertEqual(result, 5)
        result = yield from protocol.lrange(u'my_list')
        self.assertIsInstance(result, ListReply)
        result = yield from result.aslist()
        self.assertEqual(result, [u'1', u'A', u'2', u'B', u'3'])

    @redis_test
    def test_rpoplpush(self, transport, protocol):
        # Prepare
        yield from protocol.delete([ u'my_list' ])
        yield from protocol.delete([ u'my_list2' ])
        yield from protocol.lpush(u'my_list', [u'value'])
        yield from protocol.lpush(u'my_list2', [u'value2'])

        value = yield from protocol.llen(u'my_list')
        value2 = yield from protocol.llen(u'my_list2')
        self.assertEqual(value, 1)
        self.assertEqual(value2, 1)

        # rpoplpush
        result = yield from protocol.rpoplpush(u'my_list', u'my_list2')
        self.assertEqual(result, u'value')

    @redis_test
    def test_pushx(self, transport, protocol):
        yield from protocol.delete([ u'my_list' ])

        # rpushx
        result = yield from protocol.rpushx(u'my_list', u'a')
        self.assertEqual(result, 0)

        yield from protocol.rpush(u'my_list', [u'a'])
        result = yield from protocol.rpushx(u'my_list', u'a')
        self.assertEqual(result, 2)

        # lpushx
        yield from protocol.delete([ u'my_list' ])
        result = yield from protocol.lpushx(u'my_list', u'a')
        self.assertEqual(result, 0)

        yield from protocol.rpush(u'my_list', [u'a'])
        result = yield from protocol.lpushx(u'my_list', u'a')
        self.assertEqual(result, 2)

    @redis_test
    def test_ltrim(self, transport, protocol):
        yield from protocol.delete([ u'my_list' ])
        yield from protocol.lpush(u'my_list', [u'a'])
        yield from protocol.lpush(u'my_list', [u'b'])
        result = yield from protocol.ltrim(u'my_list')
        self.assertEqual(result, StatusReply('OK'))

    @redis_test
    def test_hashes(self, transport, protocol):
        yield from protocol.delete([ u'my_hash' ])

        # Set in hash
        result = yield from protocol.hset(u'my_hash', u'key', u'value')
        self.assertEqual(result, 1)
        result = yield from protocol.hset(u'my_hash', u'key2', u'value2')
        self.assertEqual(result, 1)

        # hlen
        result = yield from protocol.hlen(u'my_hash')
        self.assertEqual(result, 2)

        # hexists
        result = yield from protocol.hexists(u'my_hash', u'key')
        self.assertEqual(result, True)
        result = yield from protocol.hexists(u'my_hash', u'unknown_key')
        self.assertEqual(result, False)

        # Get from hash
        result = yield from protocol.hget(u'my_hash', u'key2')
        self.assertEqual(result, u'value2')
        result = yield from protocol.hget(u'my_hash', u'unknown-key')
        self.assertEqual(result, None)

        result = yield from protocol.hgetall(u'my_hash')
        self.assertIsInstance(result, DictReply)
        self.assertEqual(repr(result), u"DictReply(length=2)")
        result = yield from result.asdict()
        self.assertEqual(result, {u'key': u'value', u'key2': u'value2' })

        result = yield from protocol.hkeys(u'my_hash')
        self.assertIsInstance(result, SetReply)
        result = yield from result.asset()
        self.assertIsInstance(result, set)
        self.assertEqual(result, {u'key', u'key2' })

        result = yield from protocol.hvals(u'my_hash')
        self.assertIsInstance(result, ListReply)
        result = yield from result.aslist()
        self.assertIsInstance(result, list)
        self.assertEqual(set(result), {u'value', u'value2' })

        # HDel
        result = yield from protocol.hdel(u'my_hash', [u'key2'])
        self.assertEqual(result, 1)
        result = yield from protocol.hdel(u'my_hash', [u'key2'])
        self.assertEqual(result, 0)

        result = yield from protocol.hkeys(u'my_hash')
        self.assertIsInstance(result, SetReply)
        result = yield from result.asset()
        self.assertEqual(result, { u'key' })

    @redis_test
    def test_keys(self, transport, protocol):
        # Create some keys in this 'namespace'
        yield from protocol.set('our-keytest-key1', 'a')
        yield from protocol.set('our-keytest-key2', 'a')
        yield from protocol.set('our-keytest-key3', 'a')

        # Test 'keys'
        multibulk = yield from protocol.keys(u'our-keytest-key*')
        generator = [ (yield from f) for f in multibulk ]
        all_keys = yield from generator
        self.assertEqual(set(all_keys), {
                            'our-keytest-key1',
                            'our-keytest-key2',
                            'our-keytest-key3' })

    @redis_test
    def test_hmset_get(self, transport, protocol):
        yield from protocol.delete([ u'my_hash' ])
        yield from protocol.hset(u'my_hash', u'a', u'1')

        # HMSet
        result = yield from protocol.hmset(u'my_hash', { 'b':'2', 'c': '3'})
        self.assertEqual(result, StatusReply('OK'))

        # HMGet
        result = yield from protocol.hmget(u'my_hash', [u'a', u'b', u'c'])
        self.assertIsInstance(result, ListReply)
        result = yield from result.aslist()
        self.assertEqual(result, [ u'1', u'2', u'3'])

        result = yield from protocol.hmget(u'my_hash', [u'c', u'b'])
        self.assertIsInstance(result, ListReply)
        result = yield from result.aslist()
        self.assertEqual(result, [ u'3', u'2' ])

        # Hsetnx
        result = yield from protocol.hsetnx(u'my_hash', u'b', '4')
        self.assertEqual(result, 0) # Existing key. Not set
        result = yield from protocol.hget(u'my_hash', u'b')
        self.assertEqual(result, u'2')

        result = yield from protocol.hsetnx(u'my_hash', u'd', '5')
        self.assertEqual(result, 1) # New key, set
        result = yield from protocol.hget(u'my_hash', u'd')
        self.assertEqual(result, u'5')

    @redis_test
    def test_hincr(self, transport, protocol):
        yield from protocol.delete([ u'my_hash' ])
        yield from protocol.hset(u'my_hash', u'a', u'10')

        # hincrby
        result = yield from protocol.hincrby(u'my_hash', u'a', 2)
        self.assertEqual(result, 12)

        # hincrbyfloat
        result = yield from protocol.hincrbyfloat(u'my_hash', u'a', 3.7)
        self.assertEqual(result, 15.7)

    @redis_test
    def test_pubsub(self, transport, protocol):
        @asyncio.coroutine
        def listener():
            # Subscribe
            transport2, protocol2 = yield from connect(self.loop)

            self.assertEqual(protocol2.in_pubsub, False)
            subscription = yield from protocol2.start_subscribe()
            self.assertIsInstance(subscription, Subscription)
            self.assertEqual(protocol2.in_pubsub, True)
            yield from subscription.subscribe([u'our_channel'])

            value = yield from subscription.next_published()
            self.assertIsInstance(value, PubSubReply)
            self.assertEqual(value.channel, u'our_channel')
            self.assertEqual(value.value, u'message1')

            value = yield from subscription.next_published()
            self.assertIsInstance(value, PubSubReply)
            self.assertEqual(value.channel, u'our_channel')
            self.assertEqual(value.value, u'message2')
            self.assertEqual(repr(value), u"PubSubReply(channel='our_channel', value='message2')")

        f = asyncio.async(listener(), loop=self.loop)

        @asyncio.coroutine
        def sender():
            value = yield from protocol.publish(u'our_channel', 'message1')
            self.assertGreaterEqual(value, 1) # Nr of clients that received the message
            value = yield from protocol.publish(u'our_channel', 'message2')
            self.assertGreaterEqual(value, 1)

            # Test pubsub_channels
            result = yield from protocol.pubsub_channels()
            self.assertIsInstance(result, ListReply)
            result = yield from result.aslist()
            self.assertIn(u'our_channel', result)

            result = yield from protocol.pubsub_channels_aslist(u'our_c*')
            self.assertIn(u'our_channel', result)

            result = yield from protocol.pubsub_channels_aslist(u'unknown-channel-prefix*')
            self.assertEqual(result, [])

            # Test pubsub numsub.
            result = yield from protocol.pubsub_numsub([ u'our_channel', u'some_unknown_channel' ])
            self.assertIsInstance(result, DictReply)
            result = yield from result.asdict()
            self.assertEqual(len(result), 2)
            self.assertGreater(int(result['our_channel']), 0)
                    # XXX: the cast to int is required, because the redis
                    #      protocol currently returns strings instead of
                    #      integers for the count. See:
                    #      https://github.com/antirez/redis/issues/1561
            self.assertEqual(int(result['some_unknown_channel']), 0)

            # Test pubsub numpat
            result = yield from protocol.pubsub_numpat()
            self.assertIsInstance(result, int)

        yield from asyncio.sleep(.5, loop=self.loop)
        yield from sender()
        yield from f

    @redis_test
    def test_pubsub_many(self, transport, protocol):
        """ Create a listener that listens to several channels. """
        @asyncio.coroutine
        def listener():
            # Subscribe
            transport2, protocol2 = yield from connect(self.loop)

            self.assertEqual(protocol2.in_pubsub, False)
            subscription = yield from protocol2.start_subscribe()
            yield from subscription.subscribe(['channel1', 'channel2'])
            yield from subscription.subscribe(['channel3', 'channel4'])

            results = []
            for i in range(4):
                results.append((yield from subscription.next_published()))

            self.assertEqual(results, [
                    PubSubReply('channel1', 'message1'),
                    PubSubReply('channel2', 'message2'),
                    PubSubReply('channel3', 'message3'),
                    PubSubReply('channel4', 'message4'),
                ])

        f = asyncio.async(listener(), loop=self.loop)

        @asyncio.coroutine
        def sender():
            # Should not be received
            yield from protocol.publish('channel5', 'message5')

            # These for should be received.
            yield from protocol.publish('channel1', 'message1')
            yield from protocol.publish('channel2', 'message2')
            yield from protocol.publish('channel3', 'message3')
            yield from protocol.publish('channel4', 'message4')

        yield from asyncio.sleep(.5, loop=self.loop)
        yield from sender()
        yield from f

    @redis_test
    def test_incr(self, transport, protocol):
        yield from protocol.set(u'key1', u'3')

        # Incr
        result = yield from protocol.incr(u'key1')
        self.assertEqual(result, 4)
        result = yield from protocol.incr(u'key1')
        self.assertEqual(result, 5)

        # Incrby
        result = yield from protocol.incrby(u'key1', 10)
        self.assertEqual(result, 15)

        # Decr
        result = yield from protocol.decr(u'key1')
        self.assertEqual(result, 14)

        # Decrby
        result = yield from protocol.decrby(u'key1', 4)
        self.assertEqual(result, 10)

    @redis_test
    def test_bitops(self, transport, protocol):
        yield from protocol.set('a', 'fff')
        yield from protocol.set('b', '555')

        a = b'f'[0]
        b = b'5'[0]

        # Calculate set bits in the character 'f'
        set_bits = len([ c for c in bin(a) if c == '1' ])

        # Bitcount
        result = yield from protocol.bitcount('a')
        self.assertEqual(result, set_bits * 3)

        # And
        result = yield from protocol.bitop_and('result', ['a', 'b'])
        self.assertEqual(result, 3)
        result = yield from protocol.get('result')
        self.assertEqual(result, chr(a & b) * 3)

        # Or
        result = yield from protocol.bitop_or('result', ['a', 'b'])
        self.assertEqual(result, 3)
        result = yield from protocol.get('result')
        self.assertEqual(result, chr(a | b) * 3)

        # Xor
        result = yield from protocol.bitop_xor('result', ['a', 'b'])
        self.assertEqual(result, 3)
        result = yield from protocol.get('result')
        self.assertEqual(result, chr(a ^ b) * 3)

        # Not
        result = yield from protocol.bitop_not('result', 'a')
        self.assertEqual(result, 3)

            # Check result using bytes protocol
        bytes_transport, bytes_protocol = yield from connect(self.loop, lambda **kw: RedisProtocol(encoder=BytesEncoder(), **kw))
        result = yield from bytes_protocol.get(b'result')
        self.assertIsInstance(result, bytes)
        self.assertEqual(result, bytes((~a % 256, ~a % 256, ~a % 256)))

    @redis_test
    def test_setbit(self, transport, protocol):
        yield from protocol.set('a', 'fff')

        value = yield from protocol.getbit('a', 3)
        self.assertIsInstance(value, bool)
        self.assertEqual(value, False)

        value = yield from protocol.setbit('a', 3, True)
        self.assertIsInstance(value, bool)
        self.assertEqual(value, False) # Set returns the old value.

        value = yield from protocol.getbit('a', 3)
        self.assertIsInstance(value, bool)
        self.assertEqual(value, True)

    @redis_test
    def test_zscore(self, transport, protocol):
        yield from protocol.delete([ 'myzset' ])

        # Test zscore return value for NIL server response
        value = yield from protocol.zscore('myzset', 'key')
        self.assertIsNone(value)

        # zadd key 4.0
        result = yield from protocol.zadd('myzset', { 'key': 4})
        self.assertEqual(result, 1)

        # Test zscore value for existing zset members
        value = yield from protocol.zscore('myzset', 'key')
        self.assertEqual(value, 4.0)

    @redis_test
    def test_zset(self, transport, protocol):
        yield from protocol.delete([ 'myzset' ])

        # Test zadd
        result = yield from protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })
        self.assertEqual(result, 3)

        # Test zcard
        result = yield from protocol.zcard('myzset')
        self.assertEqual(result, 3)

        # Test zrank
        result = yield from protocol.zrank('myzset', 'key')
        self.assertEqual(result, 0)
        result = yield from protocol.zrank('myzset', 'key3')
        self.assertEqual(result, 2)

        result = yield from protocol.zrank('myzset', 'unknown-key')
        self.assertEqual(result, None)

        # Test revrank
        result = yield from protocol.zrevrank('myzset', 'key')
        self.assertEqual(result, 2)
        result = yield from protocol.zrevrank('myzset', 'key3')
        self.assertEqual(result, 0)

        result = yield from protocol.zrevrank('myzset', 'unknown-key')
        self.assertEqual(result, None)

        # Test zrange
        result = yield from protocol.zrange('myzset')
        self.assertIsInstance(result, ZRangeReply)
        self.assertEqual(repr(result), u"ZRangeReply(length=3)")
        self.assertEqual((yield from result.asdict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        result = yield from protocol.zrange('myzset')
        self.assertIsInstance(result, ZRangeReply)

        etalon = [ ('key', 4.0), ('key2', 5.0), ('key3', 5.5) ]
        for i, f in enumerate(result): # Ordering matter
            d = yield from f
            self.assertEqual(d, etalon[i])

        # Test zrange_asdict
        result = yield from protocol.zrange_asdict('myzset')
        self.assertEqual(result, { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        # Test zrange with negative indexes
        result = yield from protocol.zrange('myzset', -2, -1)
        self.assertEqual((yield from result.asdict()),
                {'key2': 5.0, 'key3': 5.5 })
        result = yield from protocol.zrange('myzset', -2, -1)
        self.assertIsInstance(result, ZRangeReply)

        for f in result:
            d = yield from f
            self.assertIn(d, [ ('key2', 5.0), ('key3', 5.5) ])

        # Test zrangebyscore
        result = yield from protocol.zrangebyscore('myzset')
        self.assertEqual((yield from result.asdict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        result = yield from protocol.zrangebyscore('myzset', min=ZScoreBoundary(4.5))
        self.assertEqual((yield from result.asdict()),
                { 'key2': 5.0, 'key3': 5.5 })

        result = yield from protocol.zrangebyscore('myzset', max=ZScoreBoundary(5.5))
        self.assertEqual((yield from result.asdict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })
        result = yield from protocol.zrangebyscore('myzset',
                        max=ZScoreBoundary(5.5, exclude_boundary=True))
        self.assertEqual((yield from result.asdict()),
                { 'key': 4.0, 'key2': 5.0 })

        # Test zrevrangebyscore (identical to zrangebyscore, unless we call aslist)
        result = yield from protocol.zrevrangebyscore('myzset')
        self.assertIsInstance(result, DictReply)
        self.assertEqual((yield from result.asdict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        self.assertEqual((yield from protocol.zrevrangebyscore_asdict('myzset')),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        result = yield from protocol.zrevrangebyscore('myzset', min=ZScoreBoundary(4.5))
        self.assertEqual((yield from result.asdict()),
                { 'key2': 5.0, 'key3': 5.5 })

        result = yield from protocol.zrevrangebyscore('myzset', max=ZScoreBoundary(5.5))
        self.assertIsInstance(result, DictReply)
        self.assertEqual((yield from result.asdict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })
        result = yield from protocol.zrevrangebyscore('myzset',
                        max=ZScoreBoundary(5.5, exclude_boundary=True))
        self.assertEqual((yield from result.asdict()),
                { 'key': 4.0, 'key2': 5.0 })


    @redis_test
    def test_zrevrange(self, transport, protocol):
        yield from protocol.delete([ 'myzset' ])

        # Test zadd
        result = yield from protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })
        self.assertEqual(result, 3)

        # Test zrevrange
        result = yield from protocol.zrevrange('myzset')
        self.assertIsInstance(result, ZRangeReply)
        self.assertEqual(repr(result), u"ZRangeReply(length=3)")
        self.assertEqual((yield from result.asdict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        self.assertEqual((yield from protocol.zrevrange_asdict('myzset')),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        result = yield from protocol.zrevrange('myzset')
        self.assertIsInstance(result, ZRangeReply)

        etalon = [ ('key3', 5.5), ('key2', 5.0), ('key', 4.0) ]
        for i, f in enumerate(result): # Ordering matter
            d = yield from f
            self.assertEqual(d, etalon[i])

    @redis_test
    def test_zset_zincrby(self, transport, protocol):
        yield from protocol.delete([ 'myzset' ])
        yield from protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })

        # Test zincrby
        result = yield from protocol.zincrby('myzset', 1.1, 'key')
        self.assertEqual(result, 5.1)

        result = yield from protocol.zrange('myzset')
        self.assertEqual((yield from result.asdict()),
                { 'key': 5.1, 'key2': 5.0, 'key3': 5.5 })

    @redis_test
    def test_zset_zrem(self, transport, protocol):
        yield from protocol.delete([ 'myzset' ])
        yield from protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })

        # Test zrem
        result = yield from protocol.zrem('myzset', ['key'])
        self.assertEqual(result, 1)

        result = yield from protocol.zrem('myzset', ['key'])
        self.assertEqual(result, 0)

        result = yield from protocol.zrange('myzset')
        self.assertEqual((yield from result.asdict()),
                { 'key2': 5.0, 'key3': 5.5 })

    @redis_test
    def test_zset_zrembyscore(self, transport, protocol):
        # Test zremrangebyscore (1)
        yield from protocol.delete([ 'myzset' ])
        yield from protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })

        result = yield from protocol.zremrangebyscore('myzset', min=ZScoreBoundary(5.0))
        self.assertEqual(result, 2)
        result = yield from protocol.zrange('myzset')
        self.assertEqual((yield from result.asdict()), { 'key': 4.0 })

        # Test zremrangebyscore (2)
        yield from protocol.delete([ 'myzset' ])
        yield from protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })

        result = yield from protocol.zremrangebyscore('myzset', max=ZScoreBoundary(5.0))
        self.assertEqual(result, 2)
        result = yield from protocol.zrange('myzset')
        self.assertEqual((yield from result.asdict()), { 'key3': 5.5 })

    @redis_test
    def test_zset_zremrangebyrank(self, transport, protocol):
        @asyncio.coroutine
        def setup():
            yield from protocol.delete([ 'myzset' ])
            yield from protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })

        # Test zremrangebyrank (1)
        yield from setup()
        result = yield from protocol.zremrangebyrank('myzset')
        self.assertEqual(result, 3)
        result = yield from protocol.zrange('myzset')
        self.assertEqual((yield from result.asdict()), { })

        # Test zremrangebyrank (2)
        yield from setup()
        result = yield from protocol.zremrangebyrank('myzset', min=2)
        self.assertEqual(result, 1)
        result = yield from protocol.zrange('myzset')
        self.assertEqual((yield from result.asdict()), { 'key': 4.0, 'key2': 5.0 })

        # Test zremrangebyrank (3)
        yield from setup()
        result = yield from protocol.zremrangebyrank('myzset', max=1)
        self.assertEqual(result, 2)
        result = yield from protocol.zrange('myzset')
        self.assertEqual((yield from result.asdict()), { 'key3': 5.5 })

    @redis_test
    def test_zunionstore(self, transport, protocol):
        yield from protocol.delete([ 'set_a', 'set_b' ])
        yield from protocol.zadd('set_a', { 'key': 4, 'key2': 5, 'key3': 5.5 })
        yield from protocol.zadd('set_b', { 'key': -1, 'key2': 1.1, 'key4': 9 })

        # Call zunionstore
        result = yield from protocol.zunionstore('union_key', [ 'set_a', 'set_b' ])
        self.assertEqual(result, 4)
        result = yield from protocol.zrange('union_key')
        result = yield from result.asdict()
        self.assertEqual(result, { 'key': 3.0, 'key2': 6.1, 'key3': 5.5, 'key4': 9.0 })

        # Call zunionstore with weights.
        result = yield from protocol.zunionstore('union_key', [ 'set_a', 'set_b' ], [1, 1.5])
        self.assertEqual(result, 4)
        result = yield from protocol.zrange('union_key')
        result = yield from result.asdict()
        self.assertEqual(result, { 'key': 2.5, 'key2': 6.65, 'key3': 5.5, 'key4': 13.5 })

    @redis_test
    def test_zinterstore(self, transport, protocol):
        yield from protocol.delete([ 'set_a', 'set_b' ])
        yield from protocol.zadd('set_a', { 'key': 4, 'key2': 5, 'key3': 5.5 })
        yield from protocol.zadd('set_b', { 'key': -1, 'key2': 1.5, 'key4': 9 })

        # Call zinterstore
        result = yield from protocol.zinterstore('inter_key', [ 'set_a', 'set_b' ])
        self.assertEqual(result, 2)
        result = yield from protocol.zrange('inter_key')
        result = yield from result.asdict()
        self.assertEqual(result, { 'key': 3.0, 'key2': 6.5 })

        # Call zinterstore with weights.
        result = yield from protocol.zinterstore('inter_key', [ 'set_a', 'set_b' ], [1, 1.5])
        self.assertEqual(result, 2)
        result = yield from protocol.zrange('inter_key')
        result = yield from result.asdict()
        self.assertEqual(result, { 'key': 2.5, 'key2': 7.25, })

    @redis_test
    def test_randomkey(self, transport, protocol):
        yield from protocol.set(u'key1', u'value')
        result = yield from protocol.randomkey()
        self.assertIsInstance(result, str)

    @redis_test
    def test_dbsize(self, transport, protocol):
        result = yield from protocol.dbsize()
        self.assertIsInstance(result, int)

    @redis_test
    def test_client_names(self, transport, protocol):
        # client_setname
        result = yield from protocol.client_setname(u'my-connection-name')
        self.assertEqual(result, StatusReply('OK'))

        # client_getname
        result = yield from protocol.client_getname()
        self.assertEqual(result, u'my-connection-name')

        # client list
        result = yield from protocol.client_list()
        self.assertIsInstance(result, ClientListReply)

    @redis_test
    def test_lua_script(self, transport, protocol):
        code = """
        local value = redis.call('GET', KEYS[1])
        value = tonumber(value)
        return value * ARGV[1]
        """
        yield from protocol.set('foo', '2')

        # Register script
        script = yield from protocol.register_script(code)
        self.assertIsInstance(script, Script)

        # Call script.
        result = yield from script.run(keys=['foo'], args=['5'])
        self.assertIsInstance(result, EvalScriptReply)
        result = yield from result.return_value()
        self.assertEqual(result, 10)

        # Test evalsha directly
        result = yield from protocol.evalsha(script.sha, keys=['foo'], args=['5'])
        self.assertIsInstance(result, EvalScriptReply)
        result = yield from result.return_value()
        self.assertEqual(result, 10)

        # Test script exists
        result = yield from protocol.script_exists([ script.sha, script.sha, 'unknown-script' ])
        self.assertEqual(result, [ True, True, False ])

        # Test script flush
        result = yield from protocol.script_flush()
        self.assertEqual(result, StatusReply('OK'))

        result = yield from protocol.script_exists([ script.sha, script.sha, 'unknown-script' ])
        self.assertEqual(result, [ False, False, False ])

        # Test another script where evalsha returns a string.
        code2 = """
        return "text"
        """
        script2 = yield from protocol.register_script(code2)
        result = yield from protocol.evalsha(script2.sha)
        self.assertIsInstance(result, EvalScriptReply)
        result = yield from result.return_value()
        self.assertIsInstance(result, str)
        self.assertEqual(result, u'text')

    @redis_test
    def test_script_return_types(self, transport, protocol):
        #  Test whether LUA scripts are returning correct return values.
        script_and_return_values = {
            'return "string" ': "string", # str
            'return 5 ': 5, # int
            'return ': None, # NoneType
            'return {1, 2, 3}': [1, 2, 3], # list

            # Complex nested data structure.
            'return {1, 2, "text", {3, { 4, 5 }, 6, { 7, 8 } } }': [1, 2, "text", [3, [ 4, 5 ], 6, [ 7, 8 ] ] ],
        }
        for code, return_value in script_and_return_values.items():
            # Register script
            script = yield from protocol.register_script(code)

            # Call script.
            scriptreply = yield from script.run()
            result = yield from scriptreply.return_value()
            self.assertEqual(result, return_value)

    @redis_test
    def test_script_kill(self, transport, protocol):
        # Test script kill (when nothing is running.)
        with self.assertRaises(NoRunningScriptError):
            result = yield from protocol.script_kill()

        # Test script kill (when a while/true is running.)

        @asyncio.coroutine
        def run_while_true():
            code = """
            local i = 0
            while true do
                i = i + 1
            end
            """
            transport, protocol = yield from connect(self.loop, RedisProtocol)

            script = yield from protocol.register_script(code)
            with self.assertRaises(ScriptKilledError):
                yield from script.run()

        # (start script)
        asyncio.async(run_while_true(), loop=self.loop)
        yield from asyncio.sleep(.5, loop=self.loop)

        result = yield from protocol.script_kill()
        self.assertEqual(result, StatusReply('OK'))

    @redis_test
    def test_transaction(self, transport, protocol):
        # Prepare
        yield from protocol.set(u'my_key', u'a')
        yield from protocol.set(u'my_key2', u'b')
        yield from protocol.set(u'my_key3', u'c')
        yield from protocol.delete([ u'my_hash' ])
        yield from protocol.hmset(u'my_hash', {'a':'1', 'b':'2', 'c':'3'})

        # Start transaction
        self.assertEqual(protocol.in_transaction, False)
        transaction = yield from protocol.multi()
        self.assertIsInstance(transaction, Transaction)
        self.assertEqual(protocol.in_transaction, True)

        # Run commands
        f1 = yield from transaction.get('my_key')
        f2 = yield from transaction.mget(['my_key', 'my_key2'])
        f3 = yield from transaction.get('my_key3')
        f4 = yield from transaction.mget(['my_key2', 'my_key3'])
        f5 = yield from transaction.hgetall('my_hash')

        for f in [ f1, f2, f3, f4, f5]:
            self.assertIsInstance(f, Future)

        # Running commands directly on protocol should fail.
        with self.assertRaises(Error) as e:
            yield from protocol.set('a', 'b')
        self.assertEqual(e.exception.args[0], 'Cannot run command inside transaction (use the Transaction object instead)')

        # Calling subscribe inside transaction should fail.
        with self.assertRaises(Error) as e:
            yield from transaction.start_subscribe()
        self.assertEqual(e.exception.args[0], 'Cannot start pubsub listener when a protocol is in use.')

        # Complete transaction
        result = yield from transaction.exec()
        self.assertEqual(result, None)

        # Read futures
        r1 = yield from f1
        r3 = yield from f3 # 2 & 3 switched by purpose. (order shouldn't matter.)
        r2 = yield from f2
        r4 = yield from f4
        r5 = yield from f5

        r2 = yield from r2.aslist()
        r4 = yield from r4.aslist()
        r5 = yield from r5.asdict()

        self.assertEqual(r1, u'a')
        self.assertEqual(r2, [u'a', u'b'])
        self.assertEqual(r3, u'c')
        self.assertEqual(r4, [u'b', u'c'])
        self.assertEqual(r5, { 'a': '1', 'b': '2', 'c': '3' })

    @redis_test
    def test_discard_transaction(self, transport, protocol):
        yield from protocol.set(u'my_key', u'a')

        transaction = yield from protocol.multi()
        yield from transaction.set(u'my_key', 'b')

        # Discard
        result = yield from transaction.discard()
        self.assertEqual(result, None)

        result = yield from protocol.get(u'my_key')
        self.assertEqual(result, u'a')

        # Calling anything on the transaction after discard should fail.
        with self.assertRaises(Error) as e:
            result = yield from transaction.get(u'my_key')
        self.assertEqual(e.exception.args[0], 'Transaction already finished or invalid.')

    @redis_test
    def test_nesting_transactions(self, transport, protocol):
        # That should fail.
        transaction = yield from protocol.multi()

        with self.assertRaises(Error) as e:
            transaction = yield from transaction.multi()
        self.assertEqual(e.exception.args[0], 'Multi calls can not be nested.')

    @redis_test
    def test_password(self, transport, protocol):
        # Set password
        result = yield from protocol.config_set('requirepass', 'newpassword')
        self.assertIsInstance(result, StatusReply)

        # Further redis queries should fail without re-authenticating.
        with self.assertRaises(ErrorReply) as e:
            yield from protocol.set('my-key', 'value')
        self.assertEqual(e.exception.args[0], 'NOAUTH Authentication required.')

        # Reconnect:
        result = yield from protocol.auth('newpassword')
        self.assertIsInstance(result, StatusReply)

        # Redis queries should work again.
        result = yield from protocol.set('my-key', 'value')
        self.assertIsInstance(result, StatusReply)

        # Try connecting through new Protocol instance.
        transport2, protocol2 = yield from connect(self.loop, lambda **kw: RedisProtocol(password='newpassword', **kw))
        result = yield from protocol2.set('my-key', 'value')
        self.assertIsInstance(result, StatusReply)

        # Reset password
        result = yield from protocol.config_set('requirepass', '')
        self.assertIsInstance(result, StatusReply)

    @redis_test
    def test_condfig(self, transport, protocol):
        # Config get
        result = yield from protocol.config_get('loglevel')
        self.assertIsInstance(result, ConfigPairReply)
        self.assertEqual(result.parameter, 'loglevel')
        self.assertIsInstance(result.value, str)

        # Config set
        result = yield from protocol.config_set('loglevel', result.value)
        self.assertIsInstance(result, StatusReply)

        # Resetstat
        result = yield from protocol.config_resetstat()
        self.assertIsInstance(result, StatusReply)

        # XXX: config_rewrite not tested.

    @redis_test
    def test_info(self, transport, protocol):
        result = yield from protocol.info()
        self.assertIsInstance(result, InfoReply)
        # TODO: implement and test InfoReply class

        result = yield from protocol.info('CPU')
        self.assertIsInstance(result, InfoReply)

    @redis_test
    def test_scan(self, transport, protocol):
        # Run scan command
        cursor = yield from protocol.scan(match='*')
        self.assertIsInstance(cursor, Cursor)

        # Walk through cursor
        received = []
        while True:
            i = yield from cursor.fetchone()
            if not i: break

            self.assertIsInstance(i, str)
            received.append(i)

        # The amount of keys should equal 'dbsize'
        dbsize = yield from protocol.dbsize()
        self.assertEqual(dbsize, len(received))

        # Test fetchall
        cursor = yield from protocol.scan(match='*')
        received2 = yield from cursor.fetchall()
        self.assertIsInstance(received2, list)
        self.assertEqual(set(received), set(received2))

    @redis_test
    def test_set_scan(self, transport, protocol):
        """ Test sscan """
        size = 1000
        items = [ 'value-%i' % i for i in range(size) ]

        # Create a huge set
        yield from protocol.delete(['my-set'])
        yield from protocol.sadd('my-set', items)

        # Scan this set.
        cursor = yield from protocol.sscan('my-set')

        received = []
        while True:
            i = yield from cursor.fetchone()
            if not i: break

            self.assertIsInstance(i, str)
            received.append(i)

        # Check result
        self.assertEqual(len(received), size)
        self.assertEqual(set(received), set(items))

        # Test fetchall
        cursor = yield from protocol.sscan('my-set')
        received2 = yield from cursor.fetchall()
        self.assertIsInstance(received2, set)
        self.assertEqual(set(received), received2)

    @redis_test
    def test_dict_scan(self, transport, protocol):
        """ Test hscan """
        size = 1000
        items = { 'key-%i' % i: 'values-%i' % i for i in range(size) }

        # Create a huge set
        yield from protocol.delete(['my-dict'])
        yield from protocol.hmset('my-dict', items)

        # Scan this set.
        cursor = yield from protocol.hscan('my-dict')

        received = {}
        while True:
            i = yield from cursor.fetchone()
            if not i: break

            self.assertIsInstance(i, dict)
            received.update(i)

        # Check result
        self.assertEqual(len(received), size)
        self.assertEqual(received, items)

        # Test fetchall
        cursor = yield from protocol.hscan('my-dict')
        received2 = yield from cursor.fetchall()
        self.assertIsInstance(received2, dict)
        self.assertEqual(received, received2)

    @redis_test
    def test_sorted_dict_scan(self, transport, protocol):
        """ Test zscan """
        size = 1000
        items = { 'key-%i' % i: (i + 0.1) for i in range(size) }

        # Create a huge set
        yield from protocol.delete(['my-z'])
        yield from protocol.zadd('my-z', items)

        # Scan this set.
        cursor = yield from protocol.zscan('my-z')

        received = {}
        while True:
            i = yield from cursor.fetchone()
            if not i: break

            self.assertIsInstance(i, dict)
            received.update(i)

        # Check result
        self.assertEqual(len(received), size)
        self.assertEqual(received, items)

        # Test fetchall
        cursor = yield from protocol.zscan('my-z')
        received2 = yield from cursor.fetchall()
        self.assertIsInstance(received2, dict)
        self.assertEqual(received, received2)

    @redis_test
    def test_alternate_gets(self, transport, protocol):
        """
        Test _asdict/_asset/_aslist suffixes.
        """
        # Prepare
        yield from protocol.set(u'my_key', u'a')
        yield from protocol.set(u'my_key2', u'b')

        yield from protocol.delete([ u'my_set' ])
        yield from protocol.sadd(u'my_set', [u'value1'])
        yield from protocol.sadd(u'my_set', [u'value2'])

        yield from protocol.delete([ u'my_hash' ])
        yield from protocol.hmset(u'my_hash', {'a':'1', 'b':'2', 'c':'3'})

        # Test mget_aslist
        result = yield from protocol.mget_aslist(['my_key', 'my_key2'])
        self.assertEqual(result, [u'a', u'b'])
        self.assertIsInstance(result, list)

        # Test keys_aslist
        result = yield from protocol.keys_aslist('some-prefix-')
        self.assertIsInstance(result, list)

        # Test smembers
        result = yield from protocol.smembers_asset(u'my_set')
        self.assertEqual(result, { u'value1', u'value2' })
        self.assertIsInstance(result, set)

        # Test hgetall_asdict
        result = yield from protocol.hgetall_asdict('my_hash')
        self.assertEqual(result, {'a':'1', 'b':'2', 'c':'3'})
        self.assertIsInstance(result, dict)

        # test all inside a transaction.
        transaction = yield from protocol.multi()
        f1 = yield from transaction.mget_aslist(['my_key', 'my_key2'])
        f2 = yield from transaction.smembers_asset(u'my_set')
        f3 = yield from transaction.hgetall_asdict('my_hash')
        yield from transaction.exec()

        result1 = yield from f1
        result2 = yield from f2
        result3 = yield from f3

        self.assertEqual(result1, [u'a', u'b'])
        self.assertIsInstance(result1, list)

        self.assertEqual(result2, { u'value1', u'value2' })
        self.assertIsInstance(result2, set)

        self.assertEqual(result3, {'a':'1', 'b':'2', 'c':'3'})
        self.assertIsInstance(result3, dict)

    @redis_test
    def test_cancellation(self, transport, protocol):
        """ Test CancelledError: when a query gets cancelled. """
        yield from protocol.delete(['key'])

        # Start a coroutine that runs a blocking command for 3seconds
        @asyncio.coroutine
        def run():
            yield from protocol.brpop(['key'], 3)
        f = asyncio.async(run(), loop=self.loop)

        # We cancel the coroutine before the answer arrives.
        yield from asyncio.sleep(.5, loop=self.loop)
        f.cancel()

        # Now there's a cancelled future in protocol._queue, the
        # protocol._push_answer function should notice that and ignore the
        # incoming result from our `brpop` in this case.
        yield from protocol.set('key', 'value')


class RedisBytesProtocolTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.protocol_class = lambda **kw: RedisProtocol(encoder=BytesEncoder(), **kw)

    @redis_test
    def test_bytes_protocol(self, transport, protocol):
        # When passing string instead of bytes, this protocol should raise an exception.
        with self.assertRaises(TypeError):
            result = yield from protocol.set('key', 'value')

        # Setting bytes
        result = yield from protocol.set(b'key', b'value')
        self.assertEqual(result, StatusReply('OK'))

        # Getting bytes
        result = yield from protocol.get(b'key')
        self.assertEqual(result, b'value')


class NoTypeCheckingTest(unittest.TestCase):
    def test_protocol(self):
        # Override protocol, disabling type checking.
        def factory():
            return RedisProtocol(encoder=BytesEncoder(), enable_typechecking=False)

        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def test():
            transport, protocol = yield from loop.create_connection(factory, HOST, PORT)

            # Setting values should still work.
            result = yield from protocol.set(b'key', b'value')
            self.assertEqual(result, StatusReply('OK'))

        loop.run_until_complete(test())


class RedisConnectionTest(unittest.TestCase):
    """ Test connection class. """
    def setUp(self):
        self.loop = asyncio.get_event_loop()

    def test_connection(self):
        @asyncio.coroutine
        def test():
            # Create connection
            connection = yield from Connection.create(host=HOST, port=PORT)
            self.assertEqual(repr(connection), "Connection(host=%r, port=%r)" % (HOST, PORT))

            # Test get/set
            yield from connection.set('key', 'value')
            result = yield from connection.get('key')
            self.assertEqual(result, 'value')

        self.loop.run_until_complete(test())


class RedisPoolTest(unittest.TestCase):
    """ Test connection pooling. """
    def setUp(self):
        self.loop = asyncio.get_event_loop()

    def test_pool(self):
        """ Test creation of Connection instance. """
        @asyncio.coroutine
        def test():
            # Create pool
            connection = yield from Pool.create(host=HOST, port=PORT)
            self.assertEqual(repr(connection), "Pool(host=%r, port=%r, poolsize=1)" % (HOST, PORT))

            # Test get/set
            yield from connection.set('key', 'value')
            result = yield from connection.get('key')
            self.assertEqual(result, 'value')

            # Test default poolsize
            self.assertEqual(connection.poolsize, 1)

        self.loop.run_until_complete(test())

    def test_connection_in_use(self):
        """
        When a blocking call is running, it's impossible to use the same
        protocol for another call.
        """
        @asyncio.coroutine
        def test():
            # Create connection
            connection = yield from Pool.create(host=HOST, port=PORT)
            self.assertEqual(connection.connections_in_use, 0)

            # Wait for ever. (This blocking pop doesn't return.)
            yield from connection.delete([ 'unknown-key' ])
            asyncio.async(connection.blpop(['unknown-key']), loop=self.loop)
            yield from asyncio.sleep(.1, loop=self.loop) # Sleep to make sure that the above coroutine started executing.

            # Run command in other thread.
            with self.assertRaises(NoAvailableConnectionsInPoolError) as e:
                yield from connection.set('key', 'value')
            self.assertIn('No available connections in the pool', e.exception.args[0])

            self.assertEqual(connection.connections_in_use, 1)

        self.loop.run_until_complete(test())

    def test_parallel_requests(self):
        """
        Test a blocking pop and a set using a connection pool.
        """
        @asyncio.coroutine
        def test():
            # Create connection
            connection = yield from Pool.create(host=HOST, port=PORT, poolsize=2)
            yield from connection.delete([ 'my-list' ])

            results = []

            # Sink: receive items using blocking pop
            @asyncio.coroutine
            def sink():
                for i in range(0, 5):
                    reply = yield from connection.blpop(['my-list'])
                    self.assertIsInstance(reply, BlockingPopReply)
                    self.assertIsInstance(reply.value, str)
                    results.append(reply.value)
                    self.assertIn(u"BlockingPopReply(list_name='my-list', value='", repr(reply))

            # Source: Push items on the queue
            @asyncio.coroutine
            def source():
                for i in range(0, 5):
                    yield from connection.rpush('my-list', [str(i)])
                    yield from asyncio.sleep(.5, loop=self.loop)

            # Run both coroutines.
            f1 = asyncio.async(source(), loop=self.loop)
            f2 = asyncio.async(sink(), loop=self.loop)
            yield from gather(f1, f2)

            # Test results.
            self.assertEqual(results, [ str(i) for i in range(0, 5) ])

        self.loop.run_until_complete(test())

    def test_select_db(self):
        """
        Connect to two different DBs.
        """
        @asyncio.coroutine
        def test():
            c1 = yield from Pool.create(host=HOST, port=PORT, poolsize=10, db=1)
            c2 = yield from Pool.create(host=HOST, port=PORT, poolsize=10, db=2)

            c3 = yield from Pool.create(host=HOST, port=PORT, poolsize=10, db=1)
            c4 = yield from Pool.create(host=HOST, port=PORT, poolsize=10, db=2)

            yield from c1.set('key', 'A')
            yield from c2.set('key', 'B')

            r1 = yield from c3.get('key')
            r2 = yield from c4.get('key')

            self.assertEqual(r1, 'A')
            self.assertEqual(r2, 'B')

        self.loop.run_until_complete(test())

    def test_in_use_flag(self):
        """
        Do several blocking calls and see whether in_use increments.
        """
        @asyncio.coroutine
        def test():
            # Create connection
            connection = yield from Pool.create(host=HOST, port=PORT, poolsize=10)
            for i in range(0, 10):
                yield from connection.delete([ 'my-list-%i' % i ])

            @asyncio.coroutine
            def sink(i):
                the_list, result = yield from connection.blpop(['my-list-%i' % i])

            for i in range(0, 10):
                self.assertEqual(connection.connections_in_use, i)
                asyncio.async(sink(i), loop=self.loop)
                yield from asyncio.sleep(.1, loop=self.loop) # Sleep to make sure that the above coroutine started executing.

            # One more blocking call should fail.
            with self.assertRaises(NoAvailableConnectionsInPoolError) as e:
                yield from connection.delete([ 'my-list-one-more' ])
                yield from connection.blpop(['my-list-one-more'])
            self.assertIn('No available connections in the pool', e.exception.args[0])

        self.loop.run_until_complete(test())

    def test_lua_script_in_pool(self):
        @asyncio.coroutine
        def test():
            # Create connection
            connection = yield from Pool.create(host=HOST, port=PORT, poolsize=3)

            # Register script
            script = yield from connection.register_script("return 100")
            self.assertIsInstance(script, Script)

            # Run script
            scriptreply = yield from script.run()
            result = yield from scriptreply.return_value()
            self.assertEqual(result, 100)

        self.loop.run_until_complete(test())

    def test_transactions(self):
        """
        Do several transactions in parallel.
        """
        @asyncio.coroutine
        def test():
            # Create connection
            connection = yield from Pool.create(host=HOST, port=PORT, poolsize=3)

            t1 = yield from connection.multi()
            t2 = yield from connection.multi()
            yield from connection.multi()

            # Fourth transaction should fail. (Pool is full)
            with self.assertRaises(NoAvailableConnectionsInPoolError) as e:
                yield from connection.multi()
            self.assertIn('No available connections in the pool', e.exception.args[0])

            # Run commands in transaction
            yield from t1.set(u'key', u'value')
            yield from t2.set(u'key2', u'value2')

            # Commit.
            yield from t1.exec()
            yield from t2.exec()

            # Check
            result1 = yield from connection.get(u'key')
            result2 = yield from connection.get(u'key2')

            self.assertEqual(result1, u'value')
            self.assertEqual(result2, u'value2')

        self.loop.run_until_complete(test())

    def test_watch(self):
        """
        Test a transaction, using watch
        """
        # Test using the watched key inside the transaction.
        @asyncio.coroutine
        def test():
            # Setup
            connection = yield from Pool.create(host=HOST, port=PORT, poolsize=3)
            yield from connection.set(u'key', u'0')
            yield from connection.set(u'other_key', u'0')

            # Test
            t = yield from connection.multi(watch=['other_key'])
            yield from t.set(u'key', u'value')
            yield from t.set(u'other_key', u'my_value')
            yield from t.exec()

            # Check
            result = yield from connection.get(u'key')
            self.assertEqual(result, u'value')
            result = yield from connection.get(u'other_key')
            self.assertEqual(result, u'my_value')

        self.loop.run_until_complete(test())

        # Test using the watched key outside the transaction.
        # (the transaction should fail in this case.)
        @asyncio.coroutine
        def test():
            # Setup
            connection = yield from Pool.create(host=HOST, port=PORT, poolsize=3)
            yield from connection.set(u'key', u'0')
            yield from connection.set(u'other_key', u'0')

            # Test
            t = yield from connection.multi(watch=['other_key'])
            yield from connection.set('other_key', 'other_value')
            yield from t.set(u'other_key', u'value')

            with self.assertRaises(TransactionError):
                yield from t.exec()

            # Check
            result = yield from connection.get(u'other_key')
            self.assertEqual(result, u'other_value')

        self.loop.run_until_complete(test())

    def test_connection_reconnect(self):
        """
        Test whether the connection reconnects.
        (needs manual interaction.)
        """
        @asyncio.coroutine
        def test():
            connection = yield from Pool.create(host=HOST, port=PORT, poolsize=1)
            yield from connection.set('key', 'value')

            transport = connection._connections[0].transport
            transport.close()

            yield from asyncio.sleep(1, loop=self.loop) # Give asyncio time to reconnect.

            # Test get/set
            yield from connection.set('key', 'value')

        self.loop.run_until_complete(test())

    def test_connection_lost(self):
        """
        When the transport is closed, any further commands should raise
        NotConnectedError. (Unless the transport would be auto-reconnecting and
        have established a new connection.)
        """
        @asyncio.coroutine
        def test():
            # Create connection
            transport, protocol = yield from connect(self.loop, RedisProtocol)
            yield from protocol.set('key', 'value')

            # Close transport
            self.assertEqual(protocol.is_connected, True)
            transport.close()
            yield from asyncio.sleep(.5, loop=self.loop)
            self.assertEqual(protocol.is_connected, False)

            # Test get/set
            with self.assertRaises(NotConnectedError):
                yield from protocol.set('key', 'value')

        self.loop.run_until_complete(test())

        # Test connection lost in connection pool.
        @asyncio.coroutine
        def test():
            # Create connection
            connection = yield from Pool.create(host=HOST, port=PORT, poolsize=1, auto_reconnect=False)
            yield from connection.set('key', 'value')

            # Close transport
            transport = connection._connections[0].transport
            transport.close()
            yield from asyncio.sleep(.5, loop=self.loop)

            # Test get/set
            with self.assertRaises(NoAvailableConnectionsInPoolError) as e:
                yield from connection.set('key', 'value')
            self.assertIn('No available connections in the pool: size=1, in_use=0, connected=0', e.exception.args[0])

        self.loop.run_until_complete(test())


class NoGlobalLoopTest(unittest.TestCase):
    """
    If we set the global loop variable to None, everything should still work.
    """
    def test_no_global_loop(self):
        old_loop = asyncio.get_event_loop()
        try:
            # Remove global loop and create a new one.
            asyncio.set_event_loop(None)
            new_loop = asyncio.new_event_loop()

            # ** Run code on the new loop. **

            # Create connection
            connection = new_loop.run_until_complete(Connection.create(host=HOST, port=PORT, loop=new_loop))
            self.assertIsInstance(connection, Connection)

            # Delete keys
            new_loop.run_until_complete(connection.delete(['key1', 'key2']))

            # Get/set
            new_loop.run_until_complete(connection.set('key1', 'value'))
            result = new_loop.run_until_complete(connection.get('key1'))

            # hmset/hmget (something that uses a MultiBulkReply)
            new_loop.run_until_complete(connection.hmset('key2', { 'a': 'b', 'c': 'd' }))
            result = new_loop.run_until_complete(connection.hgetall_asdict('key2'))
            self.assertEqual(result, { 'a': 'b', 'c': 'd' })
        finally:
            asyncio.set_event_loop(old_loop)


class RedisProtocolWithoutGlobalEventloopTest(RedisProtocolTest):
    """ Run all the tests from `RedisProtocolTest` again without a global event loop. """
    def setUp(self):
        super().setUp()

        # Remove global loop and create a new one.
        self._old_loop = asyncio.get_event_loop()
        asyncio.set_event_loop(None)
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        asyncio.set_event_loop(self._old_loop)


class RedisBytesWithoutGlobalEventloopProtocolTest(RedisBytesProtocolTest):
    """ Run all the tests from `RedisBytesProtocolTest`` again without a global event loop. """
    def setUp(self):
        super().setUp()

        # Remove global loop and create a new one.
        self._old_loop = asyncio.get_event_loop()
        asyncio.set_event_loop(None)
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        asyncio.set_event_loop(self._old_loop)


if __name__ == '__main__':
    unittest.main()
