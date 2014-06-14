#!/usr/bin/env python
"""
Compare how fast HiRedisProtocol is compared to the pure Python implementation
for a few different benchmarks.
"""
import asyncio
import asyncio_redis
import time

from asyncio_redis.protocol import HiRedisProtocol


@asyncio.coroutine
def test1(connection):
    """ Del/get/set of keys """
    yield from connection.delete(['key'])
    yield from connection.set('key', 'value')
    result = yield from connection.get('key')
    assert result == 'value'


@asyncio.coroutine
def test2(connection):
    """ Get/set of large hashes (with _asdict) """
    d = { str(i):str(i) for i in range(100) }

    yield from connection.delete(['key'])
    yield from connection.hmset('key', d)
    result = yield from connection.hgetall_asdict('key')
    assert result == d


@asyncio.coroutine
def test3(connection):
    """ Get/set of large hashes (without _asdict, looping over all the items.) """
    d = { str(i):str(i) for i in range(100) }

    yield from connection.delete(['key'])
    yield from connection.hmset('key', d)

    result = yield from connection.hgetall('key')
    d2 = {}

    for f in result:
        k,v = yield from f
        d2[k] = v

    assert d2 == d


benchmarks = [
        (1000, test1),
        (100, test2),
        (100, test3),
]


def run():
    connection = yield from asyncio_redis.Connection.create(host='localhost', port=6379)
    hiredis_connection = yield from asyncio_redis.Connection.create(host='localhost', port=6379, protocol_class=HiRedisProtocol)

    try:
        for count, f in benchmarks:
            print(f.__doc__)

            # Benchmark without hredis
            start = time.time()
            for i in range(count):
                yield from f(connection)
            print('      Pure Python: ', time.time() - start)

            # Benchmark with hredis
            start = time.time()
            for i in range(count):
                yield from f(hiredis_connection)
            print('      hiredis:     ', time.time() - start)
            print()
    finally:
        connection.close()
        hiredis_connection.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
