#!/usr/bin/env python

import asyncio
import gc
import os
import unittest

try:
    import hiredis
except ImportError:
    hiredis = None

from asyncio_redis import (
        Connection,
        ConnectionLostError,
        Error,
        ErrorReply,
        HiRedisProtocol,
        NoAvailableConnectionsInPoolError,
        NoRunningScriptError,
        NotConnectedError,
        Pool,
        RedisProtocol,
        Script,
        ScriptKilledError,
        Subscription,
        TimeoutError,
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
from asyncio_redis.cursors import Cursor
from asyncio_redis.encoders import BytesEncoder

PORT = int(os.environ.get('REDIS_PORT', 6379))
HOST = os.environ.get('REDIS_HOST', 'localhost')
START_REDIS_SERVER = bool(os.environ.get('START_REDIS_SERVER', False))


async def connect(protocol=RedisProtocol):
    """ Connect to redis server. Return transport/protocol pair. """
    loop = asyncio.get_event_loop()
    if PORT:
        return await loop.create_connection(lambda: protocol(), HOST, PORT)
    else:
        return await loop.create_unix_connection(lambda: protocol(), HOST)


def redis_test(function):
    """Decorator for async test methods in RedisProtocolTest
    """
    def wrapper(self):
        async def c():
            # Create connection
            transport, protocol = await connect(self.protocol_class)

            # Run test
            try:
                await function(self, transport, protocol)

            # Close connection
            finally:
                transport.close()

            # Run potential pending clean up callbacks
            await asyncio.sleep(0)

        self.loop.run_until_complete(c())

    return wrapper


def async_test(function):
    """Decorator for other async test methods
    """
    def wrapper(self):
        loop = getattr(self, "loop", asyncio.get_event_loop())
        loop.run_until_complete(function(self))

    return wrapper


class TestCase(unittest.TestCase):
    def tearDown(self):
        # Collect garbage on tearDown. (This can print ResourceWarnings.)
        gc.collect()


class RedisProtocolTest(TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.protocol_class = RedisProtocol

    @redis_test
    async def test_ping(self, transport, protocol):
        result = await protocol.ping()
        self.assertEqual(result, StatusReply('PONG'))
        self.assertEqual(repr(result), "StatusReply(status='PONG')")

    @redis_test
    async def test_echo(self, transport, protocol):
        result = await protocol.echo('my string')
        self.assertEqual(result, 'my string')

    @redis_test
    async def test_set_and_get(self, transport, protocol):
        # Set
        value = await protocol.set('my_key', 'my_value')
        self.assertEqual(value, StatusReply('OK'))

        # Get
        value = await protocol.get('my_key')
        self.assertEqual(value, 'my_value')

        # Getset
        value = await protocol.getset('my_key', 'new_value')
        self.assertEqual(value, 'my_value')

        value = await protocol.get('my_key')
        self.assertEqual(value, 'new_value')

    @redis_test
    async def test_extended_set(self, transport, protocol):
        await protocol.delete(['my_key', 'other_key'])
        # set with expire only if not exists
        value = await protocol.set('my_key', 'my_value',
                                   expire=10, only_if_not_exists=True)
        self.assertEqual(value, StatusReply('OK'))
        value = await protocol.ttl('my_key')
        self.assertIn(value, (10, 9))

        # check NX flag for SET command
        value = await protocol.set('my_key', 'my_value',
                                   expire=10, only_if_not_exists=True)
        self.assertIsNone(value)

        # check XX flag for SET command
        value = await protocol.set('other_key', 'some_value', only_if_exists=True)

        self.assertIsNone(value)

        # set with pexpire only if key exists
        value = await protocol.set('my_key', 'other_value',
                                   pexpire=20000, only_if_exists=True)
        self.assertEqual(value, StatusReply('OK'))

        value = await protocol.get('my_key')

        self.assertEqual(value, 'other_value')

        value = await protocol.ttl('my_key')
        self.assertIn(value, (20, 19))

    @redis_test
    async def test_setex(self, transport, protocol):
        # Set
        value = await protocol.setex('my_key', 10, 'my_value')
        self.assertEqual(value, StatusReply('OK'))

        # TTL
        value = await protocol.ttl('my_key')
        self.assertIn(value, (10, 9)) # may be some delay

        # Get
        value = await protocol.get('my_key')
        self.assertEqual(value, 'my_value')

    @redis_test
    async def test_setnx(self, transport, protocol):
        await protocol.delete(['my_key'])

        # Setnx while key does not exists
        value = await protocol.setnx('my_key', 'my_value')
        self.assertEqual(value, True)

        # Get
        value = await protocol.get('my_key')
        self.assertEqual(value, 'my_value')

        # Setnx if key exists
        value = await protocol.setnx('my_key', 'other_value')
        self.assertEqual(value, False)

        # Get old value
        value = await protocol.get('my_key')
        self.assertEqual(value, 'my_value')

    @redis_test
    async def test_special_characters(self, transport, protocol):
        # Test some special unicode values and spaces.
        value = 'my value with special chars " # éçåø´¨åø´h '

        result = await protocol.set('my key with spaces', value)
        result = await protocol.get('my key with spaces')
        self.assertEqual(result, value)

        # Test newlines
        value = 'ab\ncd\ref\r\ngh'
        result = await protocol.set('my-key', value)
        result = await protocol.get('my-key')
        self.assertEqual(result, value)

    @redis_test
    async def test_mget(self, transport, protocol):
        # mget
        await protocol.set('my_key', 'a')
        await protocol.set('my_key2', 'b')
        result = await protocol.mget([ 'my_key', 'my_key2', 'not_exists'])
        self.assertIsInstance(result, ListReply)
        result = await result.aslist()
        self.assertEqual(result, ['a', 'b', None])

    @redis_test
    async def test_strlen(self, transport, protocol):
        await protocol.delete([ 'my_key' ])
        await protocol.delete([ 'my_key2' ])
        await protocol.delete([ 'my_key3' ])
        await protocol.set('my_key', 'my_value')
        await protocol.hset('my_key3', 'a', 'b')

        # strlen
        value = await protocol.strlen('my_key')
        self.assertEqual(value, len('my_value'))

        value = await protocol.strlen('my_key2')
        self.assertEqual(value, 0)

        with self.assertRaises(ErrorReply):
            await protocol.strlen('my_key3')
        # Redis exception: b'ERR Operation against a key holding the wrong kind of value')

    @redis_test
    async def test_exists_and_delete(self, transport, protocol):
        # Set
        await protocol.set('my_key', 'aaa')
        value = await protocol.append('my_key', 'bbb')
        self.assertEqual(value, 6) # Total length
        value = await protocol.get('my_key')
        self.assertEqual(value, 'aaabbb')

    @redis_test
    async def test_exists_and_delete2(self, transport, protocol):
        # Exists
        value = await protocol.exists('unknown_key')
        self.assertEqual(value, False)

        # Set
        value = await protocol.set('known_key', 'value')
        value = await protocol.exists('known_key')
        self.assertEqual(value, True)

        # Delete
        value = await protocol.set('known_key2', 'value')
        value = await protocol.delete([ 'known_key', 'known_key2' ])
        self.assertEqual(value, 2)

        value = await protocol.delete([ 'known_key' ])
        self.assertEqual(value, 0)

        value = await protocol.exists('known_key')
        self.assertEqual(value, False)

    @redis_test
    async def test_rename(self, transport, protocol):
        # Set
        value = await protocol.set('old_key', 'value')
        value = await protocol.exists('old_key')
        self.assertEqual(value, True)

        # Rename
        value = await protocol.rename('old_key', 'new_key')
        self.assertEqual(value, StatusReply('OK'))

        value = await protocol.exists('old_key')
        self.assertEqual(value, False)
        value = await protocol.exists('new_key')
        self.assertEqual(value, True)

        value = await protocol.get('old_key')
        self.assertEqual(value, None)
        value = await protocol.get('new_key')
        self.assertEqual(value, 'value')

        # RenameNX
        await protocol.delete([ 'key3' ])
        value = await protocol.renamenx('new_key', 'key3')
        self.assertEqual(value, 1)

        await protocol.set('key4', 'existing-value')
        value = await protocol.renamenx('key3', 'key4')
        self.assertEqual(value, 0)

    @redis_test
    async def test_expire(self, transport, protocol):
        # Set
        value = await protocol.set('key', 'value')

        # Expire (10s)
        value = await protocol.expire('key', 10)
        self.assertEqual(value, 1)

        value = await protocol.exists('key')
        self.assertEqual(value, True)

        # TTL
        value = await protocol.ttl('key')
        self.assertIsInstance(value, int)
        self.assertLessEqual(value, 10)

        # PTTL
        value = await protocol.pttl('key')
        self.assertIsInstance(value, int)
        self.assertLessEqual(value, 10 * 1000)

        # Pexpire
        value = await protocol.pexpire('key', 10*1000)
        self.assertEqual(value, 1) # XXX: check this
        value = await protocol.pttl('key')
        self.assertLessEqual(value, 10 * 1000)

        # Expire (1s) and wait
        value = await protocol.expire('key', 1)
        value = await protocol.exists('key')
        self.assertEqual(value, True)

        await asyncio.sleep(2)

        value = await protocol.exists('key')
        self.assertEqual(value, False)

        # Test persist
        await protocol.set('key', 'value')
        await protocol.expire('key', 1)
        value = await protocol.persist('key')
        self.assertEqual(value, 1)
        value = await protocol.persist('key')
        self.assertEqual(value, 0)

        await asyncio.sleep(2)

        value = await protocol.exists('key')
        self.assertEqual(value, True)

        # Test expireat
        value = await protocol.expireat('key', 1293840000)
        self.assertIsInstance(value, int)

        # Test pexpireat
        value = await protocol.pexpireat('key', 1555555555005)
        self.assertIsInstance(value, int)

    @redis_test
    async def test_set(self, transport, protocol):
        # Create set
        value = await protocol.delete([ 'our_set' ])
        value = await protocol.sadd('our_set', ['a', 'b'])
        value = await protocol.sadd('our_set', ['c'])
        self.assertEqual(value, 1)

        # scard
        value = await protocol.scard('our_set')
        self.assertEqual(value, 3)

        # Smembers
        value = await protocol.smembers('our_set')
        self.assertIsInstance(value, SetReply)
        self.assertEqual(repr(value), "SetReply(length=3)")
        value = await value.asset()
        self.assertEqual(value, { 'a', 'b', 'c' })

        # sismember
        value = await protocol.sismember('our_set', 'a')
        self.assertEqual(value, True)
        value = await protocol.sismember('our_set', 'd')
        self.assertEqual(value, False)

        # Intersection, union and diff
        await protocol.delete([ 'set2' ])
        await protocol.sadd('set2', ['b', 'c', 'd', 'e'])

        value = await protocol.sunion([ 'our_set', 'set2' ])
        self.assertIsInstance(value, SetReply)
        value = await value.asset()
        self.assertEqual(value, {'a', 'b', 'c', 'd', 'e'})

        value = await protocol.sinter([ 'our_set', 'set2' ])
        value = await value.asset()
        self.assertEqual(value, {'b', 'c'})

        value = await protocol.sdiff([ 'our_set', 'set2' ])
        self.assertIsInstance(value, SetReply)
        value = await value.asset()
        self.assertEqual(value, {'a'})
        value = await protocol.sdiff([ 'set2', 'our_set' ])
        value = await value.asset()
        self.assertEqual(value, {'d', 'e'})

        # Interstore
        value = await protocol.sinterstore('result', ['our_set', 'set2'])
        self.assertEqual(value, 2)
        value = await protocol.smembers('result')
        self.assertIsInstance(value, SetReply)
        value = await value.asset()
        self.assertEqual(value, {'b', 'c'})

        # Unionstore
        value = await protocol.sunionstore('result', ['our_set', 'set2'])
        self.assertEqual(value, 5)
        value = await protocol.smembers('result')
        self.assertIsInstance(value, SetReply)
        value = await value.asset()
        self.assertEqual(value, {'a', 'b', 'c', 'd', 'e'})

        # Sdiffstore
        value = await protocol.sdiffstore('result', ['set2', 'our_set'])
        self.assertEqual(value, 2)
        value = await protocol.smembers('result')
        self.assertIsInstance(value, SetReply)
        value = await value.asset()
        self.assertEqual(value, {'d', 'e'})

    @redis_test
    async def test_srem(self, transport, protocol):
        await protocol.delete([ 'our_set' ])
        await protocol.sadd('our_set', ['a', 'b', 'c', 'd'])

        # Call srem
        result = await protocol.srem('our_set', ['b', 'c'])
        self.assertEqual(result, 2)

        result = await protocol.smembers('our_set')
        self.assertIsInstance(result, SetReply)
        result = await result.asset()
        self.assertEqual(result, {'a', 'd'})

    @redis_test
    async def test_spop(self, transport, protocol):
        async def setup():
            await protocol.delete([ 'my_set' ])
            await protocol.sadd('my_set', ['value1'])
            await protocol.sadd('my_set', ['value2'])

        # Test spop
        await setup()
        result = await protocol.spop('my_set')
        self.assertIn(result, ['value1', 'value2'])
        result = await protocol.smembers('my_set')
        self.assertIsInstance(result, SetReply)
        result = await result.asset()
        self.assertEqual(len(result), 1)

        # Test srandmember
        await setup()
        result = await protocol.srandmember('my_set')
        self.assertIsInstance(result, SetReply)
        result = await result.asset()
        self.assertIn(list(result)[0], ['value1', 'value2'])
        result = await protocol.smembers('my_set')
        self.assertIsInstance(result, SetReply)
        result = await result.asset()
        self.assertEqual(len(result), 2)

        # Popping from non-existing key should return None.
        await protocol.delete([ 'my_set' ])
        result = await protocol.spop('my_set')
        self.assertEqual(result, None)

    @redis_test
    async def test_type(self, transport, protocol):
        # Setup
        await protocol.delete([ 'key1' ])
        await protocol.delete([ 'key2' ])
        await protocol.delete([ 'key3' ])

        await protocol.set('key1', 'value')
        await protocol.lpush('key2', ['value'])
        await protocol.sadd('key3', ['value'])

        # Test types
        value = await protocol.type('key1')
        self.assertEqual(value, StatusReply('string'))

        value = await protocol.type('key2')
        self.assertEqual(value, StatusReply('list'))

        value = await protocol.type('key3')
        self.assertEqual(value, StatusReply('set'))

    @redis_test
    async def test_list(self, transport, protocol):
        # Create list
        await protocol.delete([ 'my_list' ])
        value = await protocol.lpush('my_list', ['v1', 'v2'])
        value = await protocol.rpush('my_list', ['v3', 'v4'])
        self.assertEqual(value, 4)

        # lrange
        value = await protocol.lrange('my_list')
        self.assertIsInstance(value, ListReply)
        self.assertEqual(repr(value), "ListReply(length=4)")
        value = await value.aslist()
        self.assertEqual(value, [ 'v2', 'v1', 'v3', 'v4'])

        # lset
        value = await protocol.lset('my_list', 3, 'new-value')
        self.assertEqual(value, StatusReply('OK'))

        value = await protocol.lrange('my_list')
        self.assertIsInstance(value, ListReply)
        value = await value.aslist()
        self.assertEqual(value, [ 'v2', 'v1', 'v3', 'new-value'])

        # lindex
        value = await protocol.lindex('my_list', 1)
        self.assertEqual(value, 'v1')
        value = await protocol.lindex('my_list', 10) # Unknown index
        self.assertEqual(value, None)

        # Length
        value = await protocol.llen('my_list')
        self.assertEqual(value, 4)

        # Remove element from list.
        value = await protocol.lrem('my_list', value='new-value')
        self.assertEqual(value, 1)

        # Pop
        value = await protocol.rpop('my_list')
        self.assertEqual(value, 'v3')
        value = await protocol.lpop('my_list')
        self.assertEqual(value, 'v2')
        value = await protocol.lpop('my_list')
        self.assertEqual(value, 'v1')
        value = await protocol.lpop('my_list')
        self.assertEqual(value, None)

        # Blocking lpop
        test_order = []

        async def blpop():
            test_order.append('#1')
            value = await protocol.blpop(['my_list'])
            self.assertIsInstance(value, BlockingPopReply)
            self.assertEqual(value.list_name, 'my_list')
            self.assertEqual(value.value, 'value')
            test_order.append('#3')
        f = asyncio.ensure_future(blpop())

        transport2, protocol2 = await connect()

        test_order.append('#2')
        await protocol2.rpush('my_list', ['value'])
        await f
        self.assertEqual(test_order, ['#1', '#2', '#3'])

        # Blocking rpop
        async def brpop():
            value = await protocol.brpop(['my_list'])
            self.assertIsInstance(value, BlockingPopReply)
            self.assertEqual(value.list_name, 'my_list')
            self.assertEqual(value.value, 'value2')
        f = asyncio.ensure_future(brpop())

        await protocol2.rpush('my_list', ['value2'])
        await f

        transport2.close()

    @redis_test
    async def test_brpoplpush(self, transport, protocol):
        await protocol.delete([ 'from' ])
        await protocol.delete([ 'to' ])
        await protocol.lpush('to', ['1'])

        async def brpoplpush():
            result = await protocol.brpoplpush('from', 'to')
            self.assertEqual(result, 'my_value')
        f = asyncio.ensure_future(brpoplpush())

        transport2, protocol2 = await connect()
        await protocol2.rpush('from', ['my_value'])
        await f

        transport2.close()

    @redis_test
    async def test_blocking_timeout(self, transport, protocol):
        await protocol.delete(['from'])
        await protocol.delete(['to'])

        # brpoplpush
        with self.assertRaises(TimeoutError) as e:
            await protocol.brpoplpush('from', 'to', 1)
        self.assertIn('Timeout in brpoplpush', e.exception.args[0])

        # brpop
        with self.assertRaises(TimeoutError) as e:
            await protocol.brpop(['from'], 1)
        self.assertIn('Timeout in blocking pop', e.exception.args[0])

        # blpop
        with self.assertRaises(TimeoutError) as e:
            await protocol.blpop(['from'], 1)
        self.assertIn('Timeout in blocking pop', e.exception.args[0])

    @redis_test
    async def test_linsert(self, transport, protocol):
        # Prepare
        await protocol.delete([ 'my_list' ])
        await protocol.rpush('my_list', ['1'])
        await protocol.rpush('my_list', ['2'])
        await protocol.rpush('my_list', ['3'])

        # Insert after
        result = await protocol.linsert('my_list', '1', 'A')
        self.assertEqual(result, 4)
        result = await protocol.lrange('my_list')
        self.assertIsInstance(result, ListReply)
        result = await result.aslist()
        self.assertEqual(result, ['1', 'A', '2', '3'])

        # Insert before
        result = await protocol.linsert('my_list', '3', 'B', before=True)
        self.assertEqual(result, 5)
        result = await protocol.lrange('my_list')
        self.assertIsInstance(result, ListReply)
        result = await result.aslist()
        self.assertEqual(result, ['1', 'A', '2', 'B', '3'])

    @redis_test
    async def test_rpoplpush(self, transport, protocol):
        # Prepare
        await protocol.delete([ 'my_list' ])
        await protocol.delete([ 'my_list2' ])
        await protocol.lpush('my_list', ['value'])
        await protocol.lpush('my_list2', ['value2'])

        value = await protocol.llen('my_list')
        value2 = await protocol.llen('my_list2')
        self.assertEqual(value, 1)
        self.assertEqual(value2, 1)

        # rpoplpush
        result = await protocol.rpoplpush('my_list', 'my_list2')
        self.assertEqual(result, 'value')
        result = await protocol.rpoplpush('my_list', 'my_list2')
        self.assertEqual(result, None)

    @redis_test
    async def test_pushx(self, transport, protocol):
        await protocol.delete([ 'my_list' ])

        # rpushx
        result = await protocol.rpushx('my_list', 'a')
        self.assertEqual(result, 0)

        await protocol.rpush('my_list', ['a'])
        result = await protocol.rpushx('my_list', 'a')
        self.assertEqual(result, 2)

        # lpushx
        await protocol.delete([ 'my_list' ])
        result = await protocol.lpushx('my_list', 'a')
        self.assertEqual(result, 0)

        await protocol.rpush('my_list', ['a'])
        result = await protocol.lpushx('my_list', 'a')
        self.assertEqual(result, 2)

    @redis_test
    async def test_ltrim(self, transport, protocol):
        await protocol.delete([ 'my_list' ])
        await protocol.lpush('my_list', ['a'])
        await protocol.lpush('my_list', ['b'])
        result = await protocol.ltrim('my_list')
        self.assertEqual(result, StatusReply('OK'))

    @redis_test
    async def test_hashes(self, transport, protocol):
        await protocol.delete([ 'my_hash' ])

        # Set in hash
        result = await protocol.hset('my_hash', 'key', 'value')
        self.assertEqual(result, 1)
        result = await protocol.hset('my_hash', 'key2', 'value2')
        self.assertEqual(result, 1)

        # hlen
        result = await protocol.hlen('my_hash')
        self.assertEqual(result, 2)

        # hexists
        result = await protocol.hexists('my_hash', 'key')
        self.assertEqual(result, True)
        result = await protocol.hexists('my_hash', 'unknown_key')
        self.assertEqual(result, False)

        # Get from hash
        result = await protocol.hget('my_hash', 'key2')
        self.assertEqual(result, 'value2')
        result = await protocol.hget('my_hash', 'unknown-key')
        self.assertEqual(result, None)

        result = await protocol.hgetall('my_hash')
        self.assertIsInstance(result, DictReply)
        self.assertEqual(repr(result), "DictReply(length=2)")
        result = await result.asdict()
        self.assertEqual(result, {'key': 'value', 'key2': 'value2' })

        result = await protocol.hkeys('my_hash')
        self.assertIsInstance(result, SetReply)
        result = await result.asset()
        self.assertIsInstance(result, set)
        self.assertEqual(result, {'key', 'key2' })

        result = await protocol.hvals('my_hash')
        self.assertIsInstance(result, ListReply)
        result = await result.aslist()
        self.assertIsInstance(result, list)
        self.assertEqual(set(result), {'value', 'value2' })

        # HDel
        result = await protocol.hdel('my_hash', ['key2'])
        self.assertEqual(result, 1)
        result = await protocol.hdel('my_hash', ['key2'])
        self.assertEqual(result, 0)

        result = await protocol.hkeys('my_hash')
        self.assertIsInstance(result, SetReply)
        result = await result.asset()
        self.assertEqual(result, { 'key' })

    @redis_test
    async def test_keys(self, transport, protocol):
        # Create some keys in this 'namespace'
        await protocol.set('our-keytest-key1', 'a')
        await protocol.set('our-keytest-key2', 'a')
        await protocol.set('our-keytest-key3', 'a')

        # Test 'keys'
        multibulk = await protocol.keys('our-keytest-key*')
        all_keys = await asyncio.gather(*multibulk)
        self.assertEqual(set(all_keys), {
                            'our-keytest-key1',
                            'our-keytest-key2',
                            'our-keytest-key3' })

    @redis_test
    async def test_hmset_get(self, transport, protocol):
        await protocol.delete([ 'my_hash' ])
        await protocol.hset('my_hash', 'a', '1')

        # HMSet
        result = await protocol.hmset('my_hash', { 'b':'2', 'c': '3'})
        self.assertEqual(result, StatusReply('OK'))

        # HMGet
        result = await protocol.hmget('my_hash', ['a', 'b', 'c'])
        self.assertIsInstance(result, ListReply)
        result = await result.aslist()
        self.assertEqual(result, [ '1', '2', '3'])

        result = await protocol.hmget('my_hash', ['c', 'b'])
        self.assertIsInstance(result, ListReply)
        result = await result.aslist()
        self.assertEqual(result, [ '3', '2' ])

        # Hsetnx
        result = await protocol.hsetnx('my_hash', 'b', '4')
        self.assertEqual(result, 0) # Existing key. Not set
        result = await protocol.hget('my_hash', 'b')
        self.assertEqual(result, '2')

        result = await protocol.hsetnx('my_hash', 'd', '5')
        self.assertEqual(result, 1) # New key, set
        result = await protocol.hget('my_hash', 'd')
        self.assertEqual(result, '5')

    @redis_test
    async def test_hincr(self, transport, protocol):
        await protocol.delete([ 'my_hash' ])
        await protocol.hset('my_hash', 'a', '10')

        # hincrby
        result = await protocol.hincrby('my_hash', 'a', 2)
        self.assertEqual(result, 12)

        # hincrbyfloat
        result = await protocol.hincrbyfloat('my_hash', 'a', 3.7)
        self.assertEqual(result, 15.7)

    @redis_test
    async def test_pubsub(self, transport, protocol):
        async def listener():
            # Subscribe
            transport2, protocol2 = await connect()

            self.assertEqual(protocol2.in_pubsub, False)
            subscription = await protocol2.start_subscribe()
            self.assertIsInstance(subscription, Subscription)
            self.assertEqual(protocol2.in_pubsub, True)
            await subscription.subscribe(['our_channel'])

            value = await subscription.next_published()
            self.assertIsInstance(value, PubSubReply)
            self.assertEqual(value.channel, 'our_channel')
            self.assertEqual(value.value, 'message1')

            value = await subscription.next_published()
            self.assertIsInstance(value, PubSubReply)
            self.assertEqual(value.channel, 'our_channel')
            self.assertEqual(value.value, 'message2')
            self.assertEqual(repr(value), "PubSubReply(channel='our_channel', value='message2')")

            return transport2

        f = asyncio.ensure_future(listener())

        async def sender():
            value = await protocol.publish('our_channel', 'message1')
            self.assertGreaterEqual(value, 1) # Nr of clients that received the message
            value = await protocol.publish('our_channel', 'message2')
            self.assertGreaterEqual(value, 1)

            # Test pubsub_channels
            result = await protocol.pubsub_channels()
            self.assertIsInstance(result, ListReply)
            result = await result.aslist()
            self.assertIn('our_channel', result)

            result = await protocol.pubsub_channels_aslist('our_c*')
            self.assertIn('our_channel', result)

            result = await protocol.pubsub_channels_aslist('unknown-channel-prefix*')
            self.assertEqual(result, [])

            # Test pubsub numsub.
            result = await protocol.pubsub_numsub([ 'our_channel', 'some_unknown_channel' ])
            self.assertIsInstance(result, DictReply)
            result = await result.asdict()
            self.assertEqual(len(result), 2)
            self.assertGreater(int(result['our_channel']), 0)
                    # XXX: the cast to int is required, because the redis
                    #      protocol currently returns strings instead of
                    #      integers for the count. See:
                    #      https://github.com/antirez/redis/issues/1561
            self.assertEqual(int(result['some_unknown_channel']), 0)

            # Test pubsub numpat
            result = await protocol.pubsub_numpat()
            self.assertIsInstance(result, int)

        await asyncio.sleep(.5)
        await sender()
        transport2 = await f
        transport2.close()

    @redis_test
    async def test_pubsub_many(self, transport, protocol):
        """ Create a listener that listens to several channels. """
        async def listener():
            # Subscribe
            transport2, protocol2 = await connect()

            self.assertEqual(protocol2.in_pubsub, False)
            subscription = await protocol2.start_subscribe()
            await subscription.subscribe(['channel1', 'channel2'])
            await subscription.subscribe(['channel3', 'channel4'])

            results = []
            for i in range(4):
                results.append((await subscription.next_published()))

            self.assertEqual(results, [
                    PubSubReply('channel1', 'message1'),
                    PubSubReply('channel2', 'message2'),
                    PubSubReply('channel3', 'message3'),
                    PubSubReply('channel4', 'message4'),
                ])

            transport2.close()

        f = asyncio.ensure_future(listener())

        async def sender():
            # Should not be received
            await protocol.publish('channel5', 'message5')

            # These for should be received.
            await protocol.publish('channel1', 'message1')
            await protocol.publish('channel2', 'message2')
            await protocol.publish('channel3', 'message3')
            await protocol.publish('channel4', 'message4')

        await asyncio.sleep(.5)
        await sender()
        await f

    @redis_test
    async def test_pubsub_patterns(self, transport, protocol):
        """ Test a pubsub connection that subscribes to a pattern. """
        async def listener():
            # Subscribe to two patterns
            transport2, protocol2 = await connect()

            subscription = await protocol2.start_subscribe()
            await subscription.psubscribe(['h*llo', 'w?rld'])

            # Receive messages
            results = []
            for i in range(4):
                results.append((await subscription.next_published()))

            self.assertEqual(results, [
                    PubSubReply('hello', 'message1', pattern='h*llo'),
                    PubSubReply('heello', 'message2', pattern='h*llo'),
                    PubSubReply('world', 'message3', pattern='w?rld'),
                    PubSubReply('wArld', 'message4', pattern='w?rld'),
                ])

            transport2.close()

        f = asyncio.ensure_future(listener())

        async def sender():
            # Should not be received
            await protocol.publish('other-channel', 'message5')

            # These for should be received.
            await protocol.publish('hello', 'message1')
            await protocol.publish('heello', 'message2')
            await protocol.publish('world', 'message3')
            await protocol.publish('wArld', 'message4')

        await asyncio.sleep(.5)
        await sender()
        await f

    @redis_test
    async def test_incr(self, transport, protocol):
        await protocol.set('key1', '3')

        # Incr
        result = await protocol.incr('key1')
        self.assertEqual(result, 4)
        result = await protocol.incr('key1')
        self.assertEqual(result, 5)

        # Incrby
        result = await protocol.incrby('key1', 10)
        self.assertEqual(result, 15)

        # Decr
        result = await protocol.decr('key1')
        self.assertEqual(result, 14)

        # Decrby
        result = await protocol.decrby('key1', 4)
        self.assertEqual(result, 10)

    @redis_test
    async def test_bitops(self, transport, protocol):
        await protocol.set('a', 'fff')
        await protocol.set('b', '555')

        a = b'f'[0]
        b = b'5'[0]

        # Calculate set bits in the character 'f'
        set_bits = len([ c for c in bin(a) if c == '1' ])

        # Bitcount
        result = await protocol.bitcount('a')
        self.assertEqual(result, set_bits * 3)

        # And
        result = await protocol.bitop_and('result', ['a', 'b'])
        self.assertEqual(result, 3)
        result = await protocol.get('result')
        self.assertEqual(result, chr(a & b) * 3)

        # Or
        result = await protocol.bitop_or('result', ['a', 'b'])
        self.assertEqual(result, 3)
        result = await protocol.get('result')
        self.assertEqual(result, chr(a | b) * 3)

        # Xor
        result = await protocol.bitop_xor('result', ['a', 'b'])
        self.assertEqual(result, 3)
        result = await protocol.get('result')
        self.assertEqual(result, chr(a ^ b) * 3)

        # Not
        result = await protocol.bitop_not('result', 'a')
        self.assertEqual(result, 3)

            # Check result using bytes protocol
        bytes_transport, bytes_protocol = await connect(
            lambda **kw: RedisProtocol(encoder=BytesEncoder(), **kw)
        )
        result = await bytes_protocol.get(b'result')
        self.assertIsInstance(result, bytes)
        self.assertEqual(result, bytes((~a % 256, ~a % 256, ~a % 256)))

        bytes_transport.close()

    @redis_test
    async def test_setbit(self, transport, protocol):
        await protocol.set('a', 'fff')

        value = await protocol.getbit('a', 3)
        self.assertIsInstance(value, bool)
        self.assertEqual(value, False)

        value = await protocol.setbit('a', 3, True)
        self.assertIsInstance(value, bool)
        self.assertEqual(value, False) # Set returns the old value.

        value = await protocol.getbit('a', 3)
        self.assertIsInstance(value, bool)
        self.assertEqual(value, True)

    @redis_test
    async def test_zscore(self, transport, protocol):
        await protocol.delete([ 'myzset' ])

        # Test zscore return value for NIL server response
        value = await protocol.zscore('myzset', 'key')
        self.assertIsNone(value)

        # zadd key 4.0
        result = await protocol.zadd('myzset', { 'key': 4})
        self.assertEqual(result, 1)

        # Test zscore value for existing zset members
        value = await protocol.zscore('myzset', 'key')
        self.assertEqual(value, 4.0)

    @redis_test
    async def test_zset(self, transport, protocol):
        await protocol.delete([ 'myzset' ])

        # Test zadd
        result = await protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })
        self.assertEqual(result, 3)

        # Test zcard
        result = await protocol.zcard('myzset')
        self.assertEqual(result, 3)

        # Test zrank
        result = await protocol.zrank('myzset', 'key')
        self.assertEqual(result, 0)
        result = await protocol.zrank('myzset', 'key3')
        self.assertEqual(result, 2)

        result = await protocol.zrank('myzset', 'unknown-key')
        self.assertEqual(result, None)

        # Test revrank
        result = await protocol.zrevrank('myzset', 'key')
        self.assertEqual(result, 2)
        result = await protocol.zrevrank('myzset', 'key3')
        self.assertEqual(result, 0)

        result = await protocol.zrevrank('myzset', 'unknown-key')
        self.assertEqual(result, None)

        # Test zrange
        result = await protocol.zrange('myzset')
        self.assertIsInstance(result, ZRangeReply)
        self.assertEqual(repr(result), "ZRangeReply(length=3)")
        self.assertEqual((await result.asdict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        result = await protocol.zrange('myzset')
        self.assertIsInstance(result, ZRangeReply)

        etalon = [ ('key', 4.0), ('key2', 5.0), ('key3', 5.5) ]
        for i, f in enumerate(result): # Ordering matter
            d = await f
            self.assertEqual(d, etalon[i])

        # Test zrange_asdict
        result = await protocol.zrange_asdict('myzset')
        self.assertEqual(result, { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        # Test zrange with negative indexes
        result = await protocol.zrange('myzset', -2, -1)
        self.assertEqual((await result.asdict()),
                {'key2': 5.0, 'key3': 5.5 })
        result = await protocol.zrange('myzset', -2, -1)
        self.assertIsInstance(result, ZRangeReply)

        for f in result:
            d = await f
            self.assertIn(d, [ ('key2', 5.0), ('key3', 5.5) ])

        # Test zrangebyscore
        result = await protocol.zrangebyscore('myzset')
        self.assertEqual((await result.asdict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        result = await protocol.zrangebyscore('myzset', min=ZScoreBoundary(4.5))
        self.assertEqual((await result.asdict()),
                { 'key2': 5.0, 'key3': 5.5 })

        result = await protocol.zrangebyscore('myzset', max=ZScoreBoundary(5.5))
        self.assertEqual((await result.asdict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })
        result = await protocol.zrangebyscore('myzset',
                        max=ZScoreBoundary(5.5, exclude_boundary=True))
        self.assertEqual((await result.asdict()),
                { 'key': 4.0, 'key2': 5.0 })

        result = await protocol.zrangebyscore('myzset', limit=1)
        self.assertEqual((await result.asdict()),
                { 'key': 4.0 })

        result = await protocol.zrangebyscore('myzset', offset=1)
        self.assertEqual((await result.asdict()),
                { 'key2': 5.0, 'key3': 5.5 })

        # Test zrevrangebyscore (identical to zrangebyscore, unless we call aslist)
        result = await protocol.zrevrangebyscore('myzset')
        self.assertIsInstance(result, DictReply)
        self.assertEqual((await result.asdict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        self.assertEqual((await protocol.zrevrangebyscore_asdict('myzset')),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        result = await protocol.zrevrangebyscore('myzset', min=ZScoreBoundary(4.5))
        self.assertEqual((await result.asdict()),
                { 'key2': 5.0, 'key3': 5.5 })

        result = await protocol.zrevrangebyscore('myzset', max=ZScoreBoundary(5.5))
        self.assertIsInstance(result, DictReply)
        self.assertEqual((await result.asdict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })
        result = await protocol.zrevrangebyscore('myzset',
                        max=ZScoreBoundary(5.5, exclude_boundary=True))
        self.assertEqual((await result.asdict()),
                { 'key': 4.0, 'key2': 5.0 })

        result = await protocol.zrevrangebyscore('myzset', limit=1)
        self.assertEqual((await result.asdict()),
                { 'key3': 5.5 })

        result = await protocol.zrevrangebyscore('myzset', offset=1)
        self.assertEqual((await result.asdict()),
                { 'key': 4.0, 'key2': 5.0 })


    @redis_test
    async def test_zrevrange(self, transport, protocol):
        await protocol.delete([ 'myzset' ])

        # Test zadd
        result = await protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })
        self.assertEqual(result, 3)

        # Test zrevrange
        result = await protocol.zrevrange('myzset')
        self.assertIsInstance(result, ZRangeReply)
        self.assertEqual(repr(result), "ZRangeReply(length=3)")
        self.assertEqual((await result.asdict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        self.assertEqual((await protocol.zrevrange_asdict('myzset')),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        result = await protocol.zrevrange('myzset')
        self.assertIsInstance(result, ZRangeReply)

        etalon = [ ('key3', 5.5), ('key2', 5.0), ('key', 4.0) ]
        for i, f in enumerate(result): # Ordering matter
            d = await f
            self.assertEqual(d, etalon[i])

    @redis_test
    async def test_zset_zincrby(self, transport, protocol):
        await protocol.delete([ 'myzset' ])
        await protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })

        # Test zincrby
        result = await protocol.zincrby('myzset', 1.1, 'key')
        self.assertEqual(result, 5.1)

        result = await protocol.zrange('myzset')
        self.assertEqual((await result.asdict()),
                { 'key': 5.1, 'key2': 5.0, 'key3': 5.5 })

    @redis_test
    async def test_zset_zrem(self, transport, protocol):
        await protocol.delete([ 'myzset' ])
        await protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })

        # Test zrem
        result = await protocol.zrem('myzset', ['key'])
        self.assertEqual(result, 1)

        result = await protocol.zrem('myzset', ['key'])
        self.assertEqual(result, 0)

        result = await protocol.zrange('myzset')
        self.assertEqual((await result.asdict()),
                { 'key2': 5.0, 'key3': 5.5 })

    @redis_test
    async def test_zset_zrembyscore(self, transport, protocol):
        # Test zremrangebyscore (1)
        await protocol.delete([ 'myzset' ])
        await protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })

        result = await protocol.zremrangebyscore('myzset', min=ZScoreBoundary(5.0))
        self.assertEqual(result, 2)
        result = await protocol.zrange('myzset')
        self.assertEqual((await result.asdict()), { 'key': 4.0 })

        # Test zremrangebyscore (2)
        await protocol.delete([ 'myzset' ])
        await protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })

        result = await protocol.zremrangebyscore('myzset', max=ZScoreBoundary(5.0))
        self.assertEqual(result, 2)
        result = await protocol.zrange('myzset')
        self.assertEqual((await result.asdict()), { 'key3': 5.5 })

    @redis_test
    async def test_zset_zremrangebyrank(self, transport, protocol):
        async def setup():
            await protocol.delete([ 'myzset' ])
            await protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })

        # Test zremrangebyrank (1)
        await setup()
        result = await protocol.zremrangebyrank('myzset')
        self.assertEqual(result, 3)
        result = await protocol.zrange('myzset')
        self.assertEqual((await result.asdict()), { })

        # Test zremrangebyrank (2)
        await setup()
        result = await protocol.zremrangebyrank('myzset', min=2)
        self.assertEqual(result, 1)
        result = await protocol.zrange('myzset')
        self.assertEqual((await result.asdict()), { 'key': 4.0, 'key2': 5.0 })

        # Test zremrangebyrank (3)
        await setup()
        result = await protocol.zremrangebyrank('myzset', max=1)
        self.assertEqual(result, 2)
        result = await protocol.zrange('myzset')
        self.assertEqual((await result.asdict()), { 'key3': 5.5 })

    @redis_test
    async def test_zunionstore(self, transport, protocol):
        await protocol.delete([ 'set_a', 'set_b' ])
        await protocol.zadd('set_a', { 'key': 4, 'key2': 5, 'key3': 5.5 })
        await protocol.zadd('set_b', { 'key': -1, 'key2': 1.1, 'key4': 9 })

        # Call zunionstore
        result = await protocol.zunionstore('union_key', [ 'set_a', 'set_b' ])
        self.assertEqual(result, 4)
        result = await protocol.zrange('union_key')
        result = await result.asdict()
        self.assertEqual(result, { 'key': 3.0, 'key2': 6.1, 'key3': 5.5, 'key4': 9.0 })

        # Call zunionstore with weights.
        result = await protocol.zunionstore('union_key', [ 'set_a', 'set_b' ], [1, 1.5])
        self.assertEqual(result, 4)
        result = await protocol.zrange('union_key')
        result = await result.asdict()
        self.assertEqual(result, { 'key': 2.5, 'key2': 6.65, 'key3': 5.5, 'key4': 13.5 })

    @redis_test
    async def test_zinterstore(self, transport, protocol):
        await protocol.delete([ 'set_a', 'set_b' ])
        await protocol.zadd('set_a', { 'key': 4, 'key2': 5, 'key3': 5.5 })
        await protocol.zadd('set_b', { 'key': -1, 'key2': 1.5, 'key4': 9 })

        # Call zinterstore
        result = await protocol.zinterstore('inter_key', [ 'set_a', 'set_b' ])
        self.assertEqual(result, 2)
        result = await protocol.zrange('inter_key')
        result = await result.asdict()
        self.assertEqual(result, { 'key': 3.0, 'key2': 6.5 })

        # Call zinterstore with weights.
        result = await protocol.zinterstore('inter_key', [ 'set_a', 'set_b' ], [1, 1.5])
        self.assertEqual(result, 2)
        result = await protocol.zrange('inter_key')
        result = await result.asdict()
        self.assertEqual(result, { 'key': 2.5, 'key2': 7.25, })

    @redis_test
    async def test_randomkey(self, transport, protocol):
        await protocol.set('key1', 'value')
        result = await protocol.randomkey()
        self.assertIsInstance(result, str)

    @redis_test
    async def test_dbsize(self, transport, protocol):
        result = await protocol.dbsize()
        self.assertIsInstance(result, int)

    @redis_test
    async def test_client_names(self, transport, protocol):
        # client_setname
        result = await protocol.client_setname('my-connection-name')
        self.assertEqual(result, StatusReply('OK'))

        # client_getname
        result = await protocol.client_getname()
        self.assertEqual(result, 'my-connection-name')

        # client list
        result = await protocol.client_list()
        self.assertIsInstance(result, ClientListReply)

    @redis_test
    async def test_lua_script(self, transport, protocol):
        code = """
        local value = redis.call('GET', KEYS[1])
        value = tonumber(value)
        return value * ARGV[1]
        """
        await protocol.set('foo', '2')

        # Register script
        script = await protocol.register_script(code)
        self.assertIsInstance(script, Script)

        # Call script.
        result = await script.run(keys=['foo'], args=['5'])
        self.assertIsInstance(result, EvalScriptReply)
        result = await result.return_value()
        self.assertEqual(result, 10)

        # Test evalsha directly
        result = await protocol.evalsha(script.sha, keys=['foo'], args=['5'])
        self.assertIsInstance(result, EvalScriptReply)
        result = await result.return_value()
        self.assertEqual(result, 10)

        # Test script exists
        result = await protocol.script_exists([ script.sha, script.sha, 'unknown-script' ])
        self.assertEqual(result, [ True, True, False ])

        # Test script flush
        result = await protocol.script_flush()
        self.assertEqual(result, StatusReply('OK'))

        result = await protocol.script_exists([ script.sha, script.sha, 'unknown-script' ])
        self.assertEqual(result, [ False, False, False ])

        # Test another script where evalsha returns a string.
        code2 = """
        return "text"
        """
        script2 = await protocol.register_script(code2)
        result = await protocol.evalsha(script2.sha)
        self.assertIsInstance(result, EvalScriptReply)
        result = await result.return_value()
        self.assertIsInstance(result, str)
        self.assertEqual(result, 'text')

    @redis_test
    async def test_script_return_types(self, transport, protocol):
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
            script = await protocol.register_script(code)

            # Call script.
            scriptreply = await script.run()
            result = await scriptreply.return_value()
            self.assertEqual(result, return_value)

    @redis_test
    async def test_script_kill(self, transport, protocol):
        # Test script kill (when nothing is running.)
        with self.assertRaises(NoRunningScriptError):
            result = await protocol.script_kill()

        # Test script kill (when a while/true is running.)

        async def run_while_true():
            code = """
            local i = 0
            while true do
                i = i + 1
            end
            """
            transport, protocol = await connect(RedisProtocol)

            script = await protocol.register_script(code)
            with self.assertRaises(ScriptKilledError):
                await script.run()

            transport.close()

        # (start script)
        f = asyncio.ensure_future(run_while_true())
        await asyncio.sleep(.5)

        result = await protocol.script_kill()
        self.assertEqual(result, StatusReply('OK'))

        # Wait for the other coroutine to finish.
        await f

    @redis_test
    async def test_transaction(self, transport, protocol):
        # Prepare
        await protocol.set('my_key', 'a')
        await protocol.set('my_key2', 'b')
        await protocol.set('my_key3', 'c')
        await protocol.delete([ 'my_hash' ])
        await protocol.hmset('my_hash', {'a':'1', 'b':'2', 'c':'3'})

        # Start transaction
        self.assertEqual(protocol.in_transaction, False)
        transaction = await protocol.multi()
        self.assertIsInstance(transaction, Transaction)
        self.assertEqual(protocol.in_transaction, True)

        # Run commands
        f1 = await transaction.get('my_key')
        f2 = await transaction.mget(['my_key', 'my_key2'])
        f3 = await transaction.get('my_key3')
        f4 = await transaction.mget(['my_key2', 'my_key3'])
        f5 = await transaction.hgetall('my_hash')

        for f in [ f1, f2, f3, f4, f5]:
            self.assertIsInstance(f, asyncio.Future)

        # Calling subscribe inside transaction should fail.
        with self.assertRaises(Error) as e:
            await transaction.start_subscribe()
        self.assertEqual(e.exception.args[0], 'Cannot start pubsub listener when a protocol is in use.')

        # Complete transaction
        result = await transaction.exec()
        self.assertEqual(result, None)
        self.assertEqual(protocol.in_transaction, False)

        # Read futures
        r1 = await f1
        r3 = await f3 # 2 & 3 switched by purpose. (order shouldn't matter.)
        r2 = await f2
        r4 = await f4
        r5 = await f5

        r2 = await r2.aslist()
        r4 = await r4.aslist()
        r5 = await r5.asdict()

        self.assertEqual(r1, 'a')
        self.assertEqual(r2, ['a', 'b'])
        self.assertEqual(r3, 'c')
        self.assertEqual(r4, ['b', 'c'])
        self.assertEqual(r5, { 'a': '1', 'b': '2', 'c': '3' })

    @redis_test
    async def test_run_command_outside_transaction(self, transport, protocol):
        # Start transaction.
        transaction = await protocol.multi()

        # Run command, but not as part of the transaction.
        # This should wait until the transaction finishes.
        f = asyncio.ensure_future(protocol.set('a', 'b'))

        # Close transaction.
        await transaction.exec()

        result = await f
        self.assertIsInstance(result, StatusReply)

    @redis_test
    async def test_discard_transaction(self, transport, protocol):
        await protocol.set('my_key', 'a')

        transaction = await protocol.multi()
        await transaction.set('my_key', 'b')

        # Discard
        result = await transaction.discard()
        self.assertEqual(result, None)

        result = await protocol.get('my_key')
        self.assertEqual(result, 'a')

        # Calling anything on the transaction after discard should fail.
        with self.assertRaises(Error) as e:
            result = await transaction.get('my_key')
        self.assertEqual(e.exception.args[0], 'Transaction already finished or invalid.')

    @redis_test
    async def test_nesting_transactions(self, transport, protocol):
        # That should fail.
        transaction = await protocol.multi()

        with self.assertRaises(Error) as e:
            transaction = await transaction.multi()
        self.assertEqual(e.exception.args[0], 'Multi calls can not be nested.')

    @redis_test
    async def test_password(self, transport, protocol):
        # Set password
        result = await protocol.config_set('requirepass', 'newpassword')
        self.assertIsInstance(result, StatusReply)

        # Further redis queries should fail without re-authenticating.
        with self.assertRaises(ErrorReply) as e:
            await protocol.set('my-key', 'value')
        self.assertEqual(e.exception.args[0], 'NOAUTH Authentication required.')

        # Reconnect:
        result = await protocol.auth('newpassword')
        self.assertIsInstance(result, StatusReply)

        # Redis queries should work again.
        result = await protocol.set('my-key', 'value')
        self.assertIsInstance(result, StatusReply)

        # Try connecting through new Protocol instance.
        transport2, protocol2 = await connect(
            lambda **kw: RedisProtocol(password='newpassword', **kw)
        )
        result = await protocol2.set('my-key', 'value')
        self.assertIsInstance(result, StatusReply)
        transport2.close()

        # Reset password
        result = await protocol.config_set('requirepass', '')
        self.assertIsInstance(result, StatusReply)

    @redis_test
    async def test_config(self, transport, protocol):
        # Config get
        result = await protocol.config_get('loglevel')
        self.assertIsInstance(result, ConfigPairReply)
        self.assertEqual(result.parameter, 'loglevel')
        self.assertIsInstance(result.value, str)

        # Config set
        result = await protocol.config_set('loglevel', result.value)
        self.assertIsInstance(result, StatusReply)

        # Resetstat
        result = await protocol.config_resetstat()
        self.assertIsInstance(result, StatusReply)

        # XXX: config_rewrite not tested.

    @redis_test
    async def test_info(self, transport, protocol):
        result = await protocol.info()
        self.assertIsInstance(result, InfoReply)
        # TODO: implement and test InfoReply class

        result = await protocol.info('CPU')
        self.assertIsInstance(result, InfoReply)

    @redis_test
    async def test_scan(self, transport, protocol):
        # Run scan command
        cursor = await protocol.scan(match='*')
        self.assertIsInstance(cursor, Cursor)

        # Walk through cursor
        received = []
        while True:
            i = await cursor.fetchone()
            if not i: break

            self.assertIsInstance(i, str)
            received.append(i)

        # The amount of keys should equal 'dbsize'
        dbsize = await protocol.dbsize()
        self.assertEqual(dbsize, len(received))

        # Test fetchall
        cursor = await protocol.scan(match='*')
        received2 = await cursor.fetchall()
        self.assertIsInstance(received2, list)
        self.assertEqual(set(received), set(received2))

    @redis_test
    async def test_set_scan(self, transport, protocol):
        """ Test sscan """
        size = 1000
        items = [ 'value-%i' % i for i in range(size) ]

        # Create a huge set
        await protocol.delete(['my-set'])
        await protocol.sadd('my-set', items)

        # Scan this set.
        cursor = await protocol.sscan('my-set')

        received = []
        while True:
            i = await cursor.fetchone()
            if not i: break

            self.assertIsInstance(i, str)
            received.append(i)

        # Check result
        self.assertEqual(len(received), size)
        self.assertEqual(set(received), set(items))

        # Test fetchall
        cursor = await protocol.sscan('my-set')
        received2 = await cursor.fetchall()
        self.assertIsInstance(received2, set)
        self.assertEqual(set(received), received2)

    @redis_test
    async def test_dict_scan(self, transport, protocol):
        """ Test hscan """
        size = 1000
        items = { 'key-%i' % i: 'values-%i' % i for i in range(size) }

        # Create a huge set
        await protocol.delete(['my-dict'])
        await protocol.hmset('my-dict', items)

        # Scan this set.
        cursor = await protocol.hscan('my-dict')

        received = {}
        while True:
            i = await cursor.fetchone()
            if not i: break

            self.assertIsInstance(i, dict)
            received.update(i)

        # Check result
        self.assertEqual(len(received), size)
        self.assertEqual(received, items)

        # Test fetchall
        cursor = await protocol.hscan('my-dict')
        received2 = await cursor.fetchall()
        self.assertIsInstance(received2, dict)
        self.assertEqual(received, received2)

    @redis_test
    async def test_sorted_dict_scan(self, transport, protocol):
        """ Test zscan """
        size = 1000
        items = { 'key-%i' % i: (i + 0.1) for i in range(size) }

        # Create a huge set
        await protocol.delete(['my-z'])
        await protocol.zadd('my-z', items)

        # Scan this set.
        cursor = await protocol.zscan('my-z')

        received = {}
        while True:
            i = await cursor.fetchone()
            if not i: break

            self.assertIsInstance(i, dict)
            received.update(i)

        # Check result
        self.assertEqual(len(received), size)
        self.assertEqual(received, items)

        # Test fetchall
        cursor = await protocol.zscan('my-z')
        received2 = await cursor.fetchall()
        self.assertIsInstance(received2, dict)
        self.assertEqual(received, received2)

    @redis_test
    async def test_alternate_gets(self, transport, protocol):
        """
        Test _asdict/_asset/_aslist suffixes.
        """
        # Prepare
        await protocol.set('my_key', 'a')
        await protocol.set('my_key2', 'b')

        await protocol.delete([ 'my_set' ])
        await protocol.sadd('my_set', ['value1'])
        await protocol.sadd('my_set', ['value2'])

        await protocol.delete([ 'my_hash' ])
        await protocol.hmset('my_hash', {'a':'1', 'b':'2', 'c':'3'})

        # Test mget_aslist
        result = await protocol.mget_aslist(['my_key', 'my_key2'])
        self.assertEqual(result, ['a', 'b'])
        self.assertIsInstance(result, list)

        # Test keys_aslist
        result = await protocol.keys_aslist('some-prefix-')
        self.assertIsInstance(result, list)

        # Test smembers
        result = await protocol.smembers_asset('my_set')
        self.assertEqual(result, { 'value1', 'value2' })
        self.assertIsInstance(result, set)

        # Test hgetall_asdict
        result = await protocol.hgetall_asdict('my_hash')
        self.assertEqual(result, {'a':'1', 'b':'2', 'c':'3'})
        self.assertIsInstance(result, dict)

        # test all inside a transaction.
        transaction = await protocol.multi()
        f1 = await transaction.mget_aslist(['my_key', 'my_key2'])
        f2 = await transaction.smembers_asset('my_set')
        f3 = await transaction.hgetall_asdict('my_hash')
        await transaction.exec()

        result1 = await f1
        result2 = await f2
        result3 = await f3

        self.assertEqual(result1, ['a', 'b'])
        self.assertIsInstance(result1, list)

        self.assertEqual(result2, { 'value1', 'value2' })
        self.assertIsInstance(result2, set)

        self.assertEqual(result3, {'a':'1', 'b':'2', 'c':'3'})
        self.assertIsInstance(result3, dict)

    @redis_test
    async def test_cancellation(self, transport, protocol):
        """ Test CancelledError: when a query gets cancelled. """
        await protocol.delete(['key'])

        # Start a task that runs a blocking command for 3seconds
        f = self.loop.create_task(protocol.brpop(['key'], 3))

        # We cancel the task before the answer arrives.
        await asyncio.sleep(.5)
        f.cancel()

        # Now there's a cancelled future in protocol._queue, the
        # protocol._push_answer function should notice that and ignore the
        # incoming result from our `brpop` in this case.
        await protocol.set('key', 'value')

    @redis_test
    async def test_watch_1(self, transport, protocol):
        """
        Test a transaction using watch.
        (Retrieve the watched value then use it inside the transaction.)
        """
        await protocol.set('key', 'val')

        # Test
        await protocol.watch(['key'])
        value = await protocol.get('key')

        t = await protocol.multi()

        await t.set('key', value + 'ue')

        await t.exec()

        # Check
        result = await protocol.get('key')
        self.assertEqual(result, 'value')

    @redis_test
    async def test_multi_watch_1(self, transport, protocol):
        """
        Test a transaction, using watch
        (Test using the watched key inside the transaction.)
        """
        await protocol.set('key', '0')
        await protocol.set('other_key', '0')

        # Test
        self.assertEqual(protocol.in_transaction, False)
        t = await protocol.multi(watch=['other_key'])
        self.assertEqual(protocol.in_transaction, True)

        await t.set('key', 'value')
        await t.set('other_key', 'my_value')
        await t.exec()

        # Check
        self.assertEqual(protocol.in_transaction, False)

        result = await protocol.get('key')
        self.assertEqual(result, 'value')
        result = await protocol.get('other_key')
        self.assertEqual(result, 'my_value')

    @redis_test
    async def test_multi_watch_2(self, transport, protocol):
        """
        Test using the watched key outside the transaction.
        (the transaction should fail in this case.)
        """
        # Setup
        transport2, protocol2 = await connect()

        await protocol.set('key', '0')
        await protocol.set('other_key', '0')

        # Test
        t = await protocol.multi(watch=['other_key'])
        await protocol2.set('other_key', 'other_value')
        await t.set('other_key', 'value')

        with self.assertRaises(TransactionError):
            await t.exec()

        # Check
        self.assertEqual(protocol.in_transaction, False)
        result = await protocol.get('other_key')
        self.assertEqual(result, 'other_value')

        transport2.close()


class RedisBytesProtocolTest(TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.protocol_class = lambda **kw: RedisProtocol(encoder=BytesEncoder(), **kw)

    @redis_test
    async def test_bytes_protocol(self, transport, protocol):
        # When passing string instead of bytes, this protocol should raise an exception.
        with self.assertRaises(TypeError):
            result = await protocol.set('key', 'value')

        # Setting bytes
        result = await protocol.set(b'key', b'value')
        self.assertEqual(result, StatusReply('OK'))

        # Getting bytes
        result = await protocol.get(b'key')
        self.assertEqual(result, b'value')

    @redis_test
    async def test_pubsub(self, transport, protocol):
        """ Test pubsub with BytesEncoder. Channel names and data are now bytes. """
        async def listener():
            # Subscribe
            transport2, protocol2 = await connect(
                lambda **kw: RedisProtocol(encoder=BytesEncoder(), **kw),
            )

            subscription = await protocol2.start_subscribe()
            await subscription.subscribe([b'our_channel'])
            value = await subscription.next_published()
            self.assertEqual(value.channel, b'our_channel')
            self.assertEqual(value.value, b'message1')

            return transport2

        async def sender():
            await protocol.publish(b'our_channel', b'message1')

        f = asyncio.ensure_future(listener())
        await asyncio.sleep(.5)
        await sender()
        transport2 = await f
        transport2.close()


class NoTypeCheckingTest(TestCase):
    @async_test
    async def test_protocol(self):
        transport, protocol = await connect(
            lambda **kw: RedisProtocol(
                encoder=BytesEncoder(), enable_typechecking=False, **kw
            ),
        )

        # Setting values should still work.
        result = await protocol.set(b'key', b'value')
        self.assertEqual(result, StatusReply('OK'))

        transport.close()


class RedisConnectionTest(TestCase):
    """ Test connection class. """
    def setUp(self):
        self.loop = asyncio.get_event_loop()

    @async_test
    async def test_connection(self):
        # Create connection
        connection = await Connection.create(host=HOST, port=PORT)
        self.assertEqual(
            repr(connection), f"Connection(host='{HOST}', port={PORT})"
        )
        self.assertEqual(connection._closing, False)

        # Test get/set
        await connection.set('key', 'value')
        result = await connection.get('key')
        self.assertEqual(result, 'value')

        connection.close()

        # Test closing flag
        self.assertEqual(connection._closing, True)


class RedisPoolTest(TestCase):
    """ Test connection pooling. """
    def setUp(self):
        self.loop = asyncio.get_event_loop()

    @async_test
    async def test_pool(self):
        """ Test creation of Connection instance. """
        # Create pool
        connection = await Pool.create(host=HOST, port=PORT)
        self.assertEqual(
            repr(connection), f"Pool(host='{HOST}', port={PORT}, poolsize=1)"
        )

        # Test get/set
        await connection.set('key', 'value')
        result = await connection.get('key')
        self.assertEqual(result, 'value')

        # Test default poolsize
        self.assertEqual(connection.poolsize, 1)

        connection.close()

    @async_test
    async def test_connection_in_use(self):
        """
        When a blocking call is running, it's impossible to use the same
        protocol for another call.
        """
        # Create connection
        connection = await Pool.create(host=HOST, port=PORT)
        self.assertEqual(connection.connections_in_use, 0)

        # Wait for ever. (This blocking pop doesn't return.)
        await connection.delete([ 'unknown-key' ])
        f = self.loop.create_task(connection.blpop(['unknown-key']))
        # Sleep to make sure that the above task started executing
        await asyncio.sleep(.1)

        # Run command in other thread.
        with self.assertRaises(NoAvailableConnectionsInPoolError) as e:
            await connection.set('key', 'value')
        self.assertIn('No available connections in the pool', e.exception.args[0])

        self.assertEqual(connection.connections_in_use, 1)

        connection.close()

        # Consume this future (which now contains ConnectionLostError)
        with self.assertRaises(ConnectionLostError):
            await f

    @async_test
    async def test_parallel_requests(self):
        """
        Test a blocking pop and a set using a connection pool.
        """
        # Create connection
        connection = await Pool.create(host=HOST, port=PORT, poolsize=2)
        await connection.delete([ 'my-list' ])

        results = []

        # Sink: receive items using blocking pop
        async def sink():
            for i in range(0, 5):
                reply = await connection.blpop(['my-list'])
                self.assertIsInstance(reply, BlockingPopReply)
                self.assertIsInstance(reply.value, str)
                results.append(reply.value)
                self.assertIn("BlockingPopReply(list_name='my-list', value='", repr(reply))

        # Source: Push items on the queue
        async def source():
            for i in range(0, 5):
                await connection.rpush('my-list', [str(i)])
                await asyncio.sleep(.5)

        # Run both coroutines
        await asyncio.gather(source(), sink())

        # Test results.
        self.assertEqual(results, [ str(i) for i in range(0, 5) ])

        connection.close()

    @async_test
    async def test_select_db(self):
        """
        Connect to two different DBs.
        """
        c1 = await Pool.create(host=HOST, port=PORT, poolsize=10, db=1)
        c2 = await Pool.create(host=HOST, port=PORT, poolsize=10, db=2)

        c3 = await Pool.create(host=HOST, port=PORT, poolsize=10, db=1)
        c4 = await Pool.create(host=HOST, port=PORT, poolsize=10, db=2)

        await c1.set('key', 'A')
        await c2.set('key', 'B')

        r1 = await c3.get('key')
        r2 = await c4.get('key')

        self.assertEqual(r1, 'A')
        self.assertEqual(r2, 'B')

        for c in [ c1, c2, c3, c4]:
            c.close()

    @async_test
    async def test_in_use_flag(self):
        """
        Do several blocking calls and see whether in_use increments.
        """
        # Create connection
        connection = await Pool.create(host=HOST, port=PORT, poolsize=10)
        for i in range(0, 10):
            await connection.delete([ 'my-list-%i' % i ])

        async def sink(i):
            await connection.blpop(['my-list-%i' % i])

        futures = []
        for i in range(0, 10):
            self.assertEqual(connection.connections_in_use, i)
            futures.append(self.loop.create_task(sink(i)))
            # Sleep to make sure that the above coroutine started executing
            await asyncio.sleep(.1)

        # One more blocking call should fail.
        with self.assertRaises(NoAvailableConnectionsInPoolError) as e:
            await connection.delete([ 'my-list-one-more' ])
            await connection.blpop(['my-list-one-more'])
        self.assertIn('No available connections in the pool', e.exception.args[0])

        connection.close()

        # Consume this futures (which now contain ConnectionLostError)
        with self.assertRaises(ConnectionLostError):
            await asyncio.gather(*futures)

    @async_test
    async def test_lua_script_in_pool(self):
        # Create connection
        connection = await Pool.create(host=HOST, port=PORT, poolsize=3)

        # Register script
        script = await connection.register_script("return 100")
        self.assertIsInstance(script, Script)

        # Run script
        scriptreply = await script.run()
        result = await scriptreply.return_value()
        self.assertEqual(result, 100)

        connection.close()

    @async_test
    async def test_transactions(self):
        """
        Do several transactions in parallel.
        """
        # Create connection
        connection = await Pool.create(host=HOST, port=PORT, poolsize=3)

        t1 = await connection.multi()
        t2 = await connection.multi()
        await connection.multi()

        # Fourth transaction should fail. (Pool is full)
        with self.assertRaises(NoAvailableConnectionsInPoolError) as e:
            await connection.multi()
        self.assertIn('No available connections in the pool', e.exception.args[0])

        # Run commands in transaction
        await t1.set('key', 'value')
        await t2.set('key2', 'value2')

        # Commit.
        await t1.exec()
        await t2.exec()

        # Check
        result1 = await connection.get('key')
        result2 = await connection.get('key2')

        self.assertEqual(result1, 'value')
        self.assertEqual(result2, 'value2')

        connection.close()

    @async_test
    async def test_connection_reconnect(self):
        """
        Test whether the connection reconnects.
        (needs manual interaction.)
        """
        connection = await Pool.create(host=HOST, port=PORT, poolsize=1)
        await connection.set('key', 'value')

        # Try the reconnect cycle several times. (Be sure that the
        # `connection_lost` callback doesn't set variables that avoid
        # reconnection a second time.)
        for i in range(3):
            transport = connection._connections[0].transport
            transport.close()

            await asyncio.sleep(1)  # Give asyncio time to reconnect

            # Test get/set
            await connection.set('key', 'value')

        connection.close()

    @async_test
    async def test_connection_lost(self):
        """
        When the transport is closed, any further commands should raise
        NotConnectedError. (Unless the transport would be auto-reconnecting and
        have established a new connection.)
        """
        # Create connection
        transport, protocol = await connect(RedisProtocol)
        await protocol.set('key', 'value')

        # Close transport
        self.assertEqual(protocol.is_connected, True)
        transport.close()
        await asyncio.sleep(.5)
        self.assertEqual(protocol.is_connected, False)

        # Test get/set
        with self.assertRaises(NotConnectedError):
            await protocol.set('key', 'value')

        transport.close()

    @async_test
    async def test_connection_lost_pool(self):
        # Create connection
        connection = await Pool.create(host=HOST, port=PORT, poolsize=1, auto_reconnect=False)
        await connection.set('key', 'value')

        # Close transport
        transport = connection._connections[0].transport
        transport.close()
        await asyncio.sleep(.5)

        # Test get/set
        with self.assertRaises(NoAvailableConnectionsInPoolError) as e:
            await connection.set('key', 'value')
        self.assertIn('No available connections in the pool: size=1, in_use=0, connected=0', e.exception.args[0])

        connection.close()


class NoGlobalLoopTest(TestCase):
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
            try:
                # Delete keys
                new_loop.run_until_complete(connection.delete(['key1', 'key2']))

                # Get/set
                new_loop.run_until_complete(connection.set('key1', 'value'))
                result = new_loop.run_until_complete(connection.get('key1'))
                self.assertEqual(result, 'value')

                # hmset/hmget (something that uses a MultiBulkReply)
                new_loop.run_until_complete(connection.hmset('key2', { 'a': 'b', 'c': 'd' }))
                result = new_loop.run_until_complete(connection.hgetall_asdict('key2'))
                self.assertEqual(result, { 'a': 'b', 'c': 'd' })
            finally:
                connection.close()
        finally:
            # Run loop briefly until socket has been closed. (call_soon behind the scenes.)
            new_loop.run_until_complete(asyncio.sleep(.1))

            new_loop.close()
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
        self.loop.close()
        asyncio.set_event_loop(self._old_loop)
        super().tearDown()


class RedisBytesWithoutGlobalEventloopProtocolTest(RedisBytesProtocolTest):
    """ Run all the tests from `RedisBytesProtocolTest`` again without a global event loop. """
    def setUp(self):
        super().setUp()

        # Remove global loop and create a new one.
        self._old_loop = asyncio.get_event_loop()
        asyncio.set_event_loop(None)
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()
        asyncio.set_event_loop(self._old_loop)
        super().tearDown()


async def start_redis_server():
    print(f'Running Redis server REDIS_HOST={HOST} REDIS_PORT={PORT}...')

    redis_srv = await asyncio.create_subprocess_exec(
        'redis-server',
        '--port', str(PORT),
        ('--bind' if PORT else '--unixsocket'), HOST,
        '--maxclients', '100',
        '--save', '""',
        '--loglevel', 'warning',
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await asyncio.sleep(.05)
    return redis_srv


@unittest.skipIf(hiredis is None, 'Hiredis not found.')
class HiRedisProtocolTest(RedisProtocolTest):
    def setUp(self):
        super().setUp()
        self.protocol_class = HiRedisProtocol


@unittest.skipIf(hiredis is None, 'Hiredis not found.')
class HiRedisBytesProtocolTest(RedisBytesProtocolTest):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.protocol_class = lambda **kw: HiRedisProtocol(encoder=BytesEncoder(), **kw)


if __name__ == '__main__':
    if START_REDIS_SERVER:
        loop = asyncio.get_event_loop()
        redis_srv = loop.run_until_complete(start_redis_server())

    try:
        unittest.main()
    finally:
        if START_REDIS_SERVER:
            redis_srv.terminate()
