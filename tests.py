#!/usr/bin/env python

from asyncio import test_utils
from asyncio.futures import Future
from asyncio.protocols import SubprocessProtocol
from asyncio.tasks import gather

from asyncio_redis import RedisProtocol, Connection, Transaction, RedisException, StatusReply, ZRangeResult, ZScoreBoundary
from threading import Thread
from time import sleep

import asyncio
import subprocess
import unittest
import os

PORT = int(os.environ['REDIS_PORT'])


@asyncio.coroutine
def connect(loop):
    transport, protocol = yield from loop.create_connection(RedisProtocol, 'localhost', PORT)
    return transport, protocol


def redis_test(function):
    function = asyncio.coroutine(function)

    def wrapper(self):
        @asyncio.coroutine
        def c():
            # Create connection
            transport, protocol = yield from connect(self.loop)

            yield from function(self, transport, protocol)

        self.loop.run_until_complete(c())
    return wrapper

class RedisProtocolTest(unittest.TestCase):
    def setUp(self):
        #self.loop = test_utils.TestLoop()
        self.loop = asyncio.get_event_loop()


    def tearDown(self):
        #self.loop.close()
        pass

    @redis_test
    def test_ping(self, transport, protocol):
        result = yield from protocol.ping()
        self.assertEqual(result, StatusReply('PONG'))

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
    def test_mget(self, transport, protocol):
        # mget
        yield from protocol.set(u'my_key', u'a')
        yield from protocol.set(u'my_key2', u'b')
        result = yield from protocol.mget([ u'my_key', u'my_key2' ])
        self.assertEqual(result, [u'a', u'b'])

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

        with self.assertRaises(RedisException) as e:
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
    def test_exists_and_delete(self, transport, protocol):
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

        sleep(2)

        value = yield from protocol.exists(u'key')
        self.assertEqual(value, False)

        # Test persist
        yield from protocol.set(u'key', u'value')
        yield from protocol.expire(u'key', 1)
        value = yield from protocol.persist(u'key')
        self.assertEqual(value, 1)
        value = yield from protocol.persist(u'key')
        self.assertEqual(value, 0)

        sleep(2)

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
        self.assertEqual(value, set([u'a', u'b', u'c', u'd', u'e']))

        value = yield from protocol.sinter([ u'our_set', 'set2' ])
        self.assertEqual(value, set([u'b', u'c']))

        value = yield from protocol.sdiff([ u'our_set', 'set2' ])
        self.assertEqual(value, set([u'a']))
        value = yield from protocol.sdiff([ u'set2', u'our_set' ])
        self.assertEqual(value, set([u'd', u'e']))

        # Interstore
        value = yield from protocol.sinterstore(u'result', [u'our_set', 'set2'])
        self.assertEqual(value, 2)
        value = yield from protocol.smembers(u'result')
        self.assertEqual(value, set([u'b', u'c']))

        # Unionstore
        value = yield from protocol.sunionstore(u'result', [u'our_set', 'set2'])
        self.assertEqual(value, 5)
        value = yield from protocol.smembers(u'result')
        self.assertEqual(value, set([u'a', u'b', u'c', u'd', u'e']))

        # Sdiffstore
        value = yield from protocol.sdiffstore(u'result', [u'set2', 'our_set'])
        self.assertEqual(value, 2)
        value = yield from protocol.smembers(u'result')
        self.assertEqual(value, set([u'd', u'e']))

    @redis_test
    def test_srem(self, transport, protocol):
        yield from protocol.delete([ u'our_set' ])
        yield from protocol.sadd(u'our_set', [u'a', u'b', u'c', u'd'])

        # Call srem
        result = yield from protocol.srem(u'our_set', [u'b', u'c'])
        self.assertEqual(result, 2)

        result = yield from protocol.smembers(u'our_set')
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
        self.assertEqual(len(result), 1)

        # Test srandmember
        yield from setup()
        result = yield from protocol.srandmember(u'my_set')
        self.assertIn(result[0], [u'value1', u'value2'])
        result = yield from protocol.smembers(u'my_set')
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
        self.assertEqual(value, [ u'v2', 'v1', 'v3', 'v4'])

        # lset
        value = yield from protocol.lset(u'my_list', 3, 'new-value')
        self.assertEqual(value, StatusReply('OK'))

        value = yield from protocol.lrange(u'my_list')
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
            test_order.append('#3')
            self.assertEqual(value, [u'my_list', u'value'])
        f = asyncio.Task(blpop())

        transport2, protocol2 = yield from connect(self.loop)

        test_order.append('#2')
        yield from protocol2.rpush(u'my_list', [u'value'])
        yield from f
        self.assertEqual(test_order, ['#1', '#2', '#3'])

        # Blocking rpop
        @asyncio.coroutine
        def blpop():
            value = yield from protocol.brpop([u'my_list'])
            self.assertEqual(value, [u'my_list', u'value2'])
        f = asyncio.Task(blpop())

        yield from protocol2.rpush(u'my_list', [u'value2'])
        yield from f

    @redis_test
    def test_brpoplpush(self, transport, protocol):
        yield from protocol.delete([ u'from' ])
        yield from protocol.delete([ u'to' ])
        yield from protocol.lpush(u'to', [u'1'])

        @asyncio.coroutine
        def brpoplpush():
            value = yield from protocol.brpoplpush(u'from', u'to')
            self.assertEqual(value, u'my_value')
        f = asyncio.Task(brpoplpush())

        transport2, protocol2 = yield from connect(self.loop)
        yield from protocol2.rpush(u'from', [u'my_value'])
        yield from f

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
        self.assertEqual(result, [u'1', u'A', u'2', u'3'])

        # Insert before
        result = yield from protocol.linsert(u'my_list', u'3', u'B', before=True)
        self.assertEqual(result, 5)
        result = yield from protocol.lrange(u'my_list')
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
        self.assertEqual(result, {u'key': u'value', u'key2': u'value2' })

        result = yield from protocol.hkeys(u'my_hash')
        self.assertIsInstance(result, set)
        self.assertEqual(result, {u'key', u'key2' })

        result = yield from protocol.hvals(u'my_hash')
        self.assertIsInstance(result, list)
        self.assertEqual(set(result), {u'value', u'value2' })

        # HDel
        result = yield from protocol.hdel(u'my_hash', [u'key2'])
        self.assertEqual(result, 1)
        result = yield from protocol.hdel(u'my_hash', [u'key2'])
        self.assertEqual(result, 0)

        result = yield from protocol.hkeys(u'my_hash')
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
        self.assertEqual(result, [ u'1', u'2', u'3'])

        result = yield from protocol.hmget(u'my_hash', [u'c', u'b'])
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

            # Subscribe
            self.assertEqual(protocol2.in_pubsub, False)
            value = yield from protocol2.subscribe([u'our_channel'])
            self.assertEqual(value, [u'subscribe', u'our_channel', 1])
            self.assertEqual(protocol2.in_pubsub, True)

            value = yield from protocol2.get_next_published()
            self.assertEqual(value, [u'message', u'our_channel', u'message1']) # TODO: return something nicer!

            value = yield from protocol2.get_next_published()
            self.assertEqual(value, [u'message', u'our_channel', u'message2'])

        f = asyncio.Task(listener())

        @asyncio.coroutine
        def sender():
            value = yield from protocol.publish(u'our_channel', 'message1')
            self.assertEqual(value, 1) # Nr of clients that received the message
            value = yield from protocol.publish(u'our_channel', 'message2')
            self.assertEqual(value, 1)

        yield from asyncio.sleep(.5)
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

            # The current protocol is not able to handle this result, if we're
            # using an UTF-8 decoder. TODO: implement 'dummy' byte decoder.

        # result = yield from protocol.get('result')
        # self.assertEqual(result, chr(~ a) * 3)

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
        self.assertIsInstance(result, ZRangeResult)
        self.assertEqual((yield from result.get_as_dict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        result = yield from protocol.zrange('myzset')
        self.assertIsInstance(result, ZRangeResult)

        for f in result:
            d = yield from f

            self.assertIn(d, [
                {'key': 4.0},
                {'key2': 5.0},
                {'key3': 5.5} ])

        # Test zrangebyscore
        result = yield from protocol.zrangebyscore('myzset')
        self.assertEqual((yield from result.get_as_dict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        result = yield from protocol.zrangebyscore('myzset')
        self.assertEqual((yield from result.get_as_list()),
                [ 'key', 'key2', 'key3' ])

        result = yield from protocol.zrangebyscore('myzset', min=ZScoreBoundary(4.5))
        self.assertEqual((yield from result.get_as_dict()),
                { 'key2': 5.0, 'key3': 5.5 })

        result = yield from protocol.zrangebyscore('myzset', max=ZScoreBoundary(5.5))
        self.assertEqual((yield from result.get_as_dict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })
        result = yield from protocol.zrangebyscore('myzset',
                        max=ZScoreBoundary(5.5, exclude_boundary=True))
        self.assertEqual((yield from result.get_as_dict()),
                { 'key': 4.0, 'key2': 5.0 })

        # Test zrevrangebyscore (identical to zrangebyscore, unless we call get_as_list)
        result = yield from protocol.zrevrangebyscore('myzset')
        self.assertEqual((yield from result.get_as_dict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })

        result = yield from protocol.zrevrangebyscore('myzset')
        self.assertEqual((yield from result.get_as_list()),
                [ 'key3', 'key2', 'key' ])

        result = yield from protocol.zrevrangebyscore('myzset', min=ZScoreBoundary(4.5))
        self.assertEqual((yield from result.get_as_dict()),
                { 'key2': 5.0, 'key3': 5.5 })

        result = yield from protocol.zrevrangebyscore('myzset', max=ZScoreBoundary(5.5))
        self.assertEqual((yield from result.get_as_dict()),
                { 'key': 4.0, 'key2': 5.0, 'key3': 5.5 })
        result = yield from protocol.zrevrangebyscore('myzset',
                        max=ZScoreBoundary(5.5, exclude_boundary=True))
        self.assertEqual((yield from result.get_as_dict()),
                { 'key': 4.0, 'key2': 5.0 })

    @redis_test
    def test_zset_zincrby(self, transport, protocol):
        yield from protocol.delete([ 'myzset' ])
        yield from protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })

        # Test zincrby
        result = yield from protocol.zincrby('myzset', 1.1, 'key')
        self.assertEqual(result, 5.1)

        result = yield from protocol.zrange('myzset')
        self.assertEqual((yield from result.get_as_dict()),
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
        self.assertEqual((yield from result.get_as_dict()),
                { 'key2': 5.0, 'key3': 5.5 })

    @redis_test
    def test_zset_zrembyscore(self, transport, protocol):
        # Test zremrangebyscore (1)
        yield from protocol.delete([ 'myzset' ])
        yield from protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })

        result = yield from protocol.zremrangebyscore('myzset', min=ZScoreBoundary(5.0))
        self.assertEqual(result, 2)
        result = yield from protocol.zrange('myzset')
        self.assertEqual((yield from result.get_as_dict()), { 'key': 4.0 })

        # Test zremrangebyscore (2)
        yield from protocol.delete([ 'myzset' ])
        yield from protocol.zadd('myzset', { 'key': 4, 'key2': 5, 'key3': 5.5 })

        result = yield from protocol.zremrangebyscore('myzset', max=ZScoreBoundary(5.0))
        self.assertEqual(result, 2)
        result = yield from protocol.zrange('myzset')
        self.assertEqual((yield from result.get_as_dict()), { 'key3': 5.5 })

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
        self.assertEqual((yield from result.get_as_dict()), { })

        # Test zremrangebyrank (2)
        yield from setup()
        result = yield from protocol.zremrangebyrank('myzset', min=2)
        self.assertEqual(result, 1)
        result = yield from protocol.zrange('myzset')
        self.assertEqual((yield from result.get_as_dict()), { 'key': 4.0, 'key2': 5.0 })

        # Test zremrangebyrank (3)
        yield from setup()
        result = yield from protocol.zremrangebyrank('myzset', max=1)
        self.assertEqual(result, 2)
        result = yield from protocol.zrange('myzset')
        self.assertEqual((yield from result.get_as_dict()), { 'key3': 5.5 })

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
        with self.assertRaises(RedisException) as e:
            yield from protocol.set('a', 'b')
        self.assertEqual(e.exception.args[0], 'Cannot run command inside transaction')

        # Calling subscribe inside transaction should fail.
        with self.assertRaises(RedisException) as e:
            yield from transaction.subscribe(['channel'])
        self.assertEqual(e.exception.args[0], 'Cannot call subscribe inside a transaction.')

        # Complete transaction
        result = yield from transaction.exec()
        self.assertEqual(result, None)

        # Read futures
        r1 = yield from f1
        r3 = yield from f3 # 2 & 3 switched by purpose. (order shouldn't matter.)
        r2 = yield from f2
        r4 = yield from f4
        r5 = yield from f5

        self.assertEqual(r1, u'a')
        self.assertEqual(r2, [u'a', u'b'])
        self.assertEqual(r3, u'c')
        self.assertEqual(r4, [u'b', u'c'])
        self.assertEqual(r5, { 'a': '1', 'b': '2', 'c': '3' })

    @redis_test
    def test_discard_transaction(self, transport, protocol):
        yield from protocol.set(u'my_key', u'a')

        transaction = yield from protocol.multi()
        f = yield from transaction.set(u'my_key', 'b')

        # Discard
        result = yield from transaction.discard()
        self.assertEqual(result, None)

        result = yield from protocol.get(u'my_key')
        self.assertEqual(result, u'a')

        # Calling anything on the transaction after discard should fail.
        with self.assertRaises(RedisException) as e:
            result = yield from transaction.get(u'my_key')
        self.assertEqual(e.exception.args[0], 'Transaction already finished or invalid.')

    @redis_test
    def test_nesting_transactions(self, transport, protocol):
        # That should fail.
        transaction = yield from protocol.multi()

        with self.assertRaises(RedisException) as e:
            transaction = yield from transaction.multi()
        self.assertEqual(e.exception.args[0], 'Multi calls can not be nested.')


class RedisConnectionTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()

    def test_connection(self):
        """ Test creation of Connection instance. """
        @asyncio.coroutine
        def test():
            # Create connection
            connection = yield from Connection.create(port=PORT)

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
            connection = yield from Connection.create(port=PORT)
            self.assertEqual(connection.connections_in_use, 0)

            # Wait for ever. (This blocking pop doesn't return.)
            yield from connection.delete([ 'unknown-key' ])
            f = asyncio.Task(connection.blpop(['unknown-key']))
            yield from asyncio.sleep(.1) # Sleep to make sure that the above coroutine started executing.

            # Run command in other thread.
            with self.assertRaises(RedisException) as e:
                yield from connection.set('key', 'value')
            self.assertEqual(e.exception.args[0], 'All connection in the pool are in use. Please increase the poolsize.')

            self.assertEqual(connection.connections_in_use, 1)

        self.loop.run_until_complete(test())

    def test_parallel_requests(self):
        """
        Test a blocking pop and a set using a connection pool.
        """
        @asyncio.coroutine
        def test():
            # Create connection
            connection = yield from Connection.create(port=PORT, poolsize=2)
            yield from connection.delete([ 'my-list' ])

            results = []

            # Sink: receive items using blocking pop
            @asyncio.coroutine
            def sink():
                for i in range(0, 5):
                    the_list, result = yield from connection.blpop(['my-list'])
                    results.append(result)

            # Source: Push items on the queue
            @asyncio.coroutine
            def source():
                for i in range(0, 5):
                    result = yield from connection.rpush('my-list', [str(i)])
                    yield from asyncio.sleep(.5)

            # Run both coroutines.
            f1 = asyncio.Task(source())
            f2 = asyncio.Task(sink())
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
            c1 = yield from Connection.create(port=PORT, poolsize=10, db=1)
            c2 = yield from Connection.create(port=PORT, poolsize=10, db=2)

            c3 = yield from Connection.create(port=PORT, poolsize=10, db=1)
            c4 = yield from Connection.create(port=PORT, poolsize=10, db=2)

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
            connection = yield from Connection.create(port=PORT, poolsize=10)
            for i in range(0, 10):
                yield from connection.delete([ 'my-list-%i' % i ])

            @asyncio.coroutine
            def sink(i):
                the_list, result = yield from connection.blpop(['my-list-%i' % i])

            for i in range(0, 10):
                self.assertEqual(connection.connections_in_use, i)
                asyncio.Task(sink(i))
                yield from asyncio.sleep(.1) # Sleep to make sure that the above coroutine started executing.

            # One more blocking call should fail.
            with self.assertRaises(RedisException) as e:
                yield from connection.delete([ 'my-list-one-more' ])
                yield from connection.blpop(['my-list-one-more'])
            self.assertEqual(e.exception.args[0], 'All connection in the pool are in use. Please increase the poolsize.')

        self.loop.run_until_complete(test())

    def test_transactions(self):
        """
        Do several transactions in parallel.
        """
        @asyncio.coroutine
        def test():
            # Create connection
            connection = yield from Connection.create(port=PORT, poolsize=3)

            t1 = yield from connection.multi()
            t2 = yield from connection.multi()
            t3 = yield from connection.multi()

            # Fourth transaction should fail. (Pool is full)
            with self.assertRaises(RedisException) as e:
                yield from connection.multi()
            self.assertEqual(e.exception.args[0], 'All connection in the pool are in use. Please increase the poolsize.')

            # Run commands in transaction
            f1 = yield from t1.set(u'key', u'value')
            f2 = yield from t2.set(u'key2', u'value2')

            # Commit.
            yield from t1.exec()
            yield from t2.exec()

            # Check
            result1 = yield from connection.get(u'key')
            result2 = yield from connection.get(u'key2')

            self.assertEqual(result1, u'value')
            self.assertEqual(result2, u'value2')

        self.loop.run_until_complete(test())

    '''
    def test_connection_reconnect(self):
        """
        Test whether the connection reconnects.
        (needs manual interaction.)
        """
        @asyncio.coroutine
        def test():
            # Create connection
            connection = yield from Connection.create(port=PORT, poolsize=1)
            yield from connection.set('key', 'value')

            #input('Please shut down the redis server.')
            transport = connection._transport_protocol_pairs[0][0]
            transport.close()

            # Test get/set
            yield from connection.set('key', 'value')

        self.loop.run_until_complete(test())
    '''

if __name__ == '__main__':
    unittest.main()
