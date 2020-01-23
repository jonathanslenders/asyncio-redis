#!/usr/bin/env python
"""
Compare how fast HiRedisProtocol is compared to the pure Python implementation
for a few different benchmarks.
"""
import asyncio
import time

import asyncio_redis
from asyncio_redis.protocol import HiRedisProtocol

try:
    import hiredis
except ImportError:
    hiredis = None


async def test1(connection):
    """ Del/get/set of keys """
    await connection.delete(["key"])
    await connection.set("key", "value")
    result = await connection.get("key")
    assert result == "value"


async def test2(connection):
    """ Get/set of a hash of 100 items (with _asdict) """
    d = {str(i): str(i) for i in range(100)}

    await connection.delete(["key"])
    await connection.hmset("key", d)
    result = await connection.hgetall_asdict("key")
    assert result == d


async def test3(connection):
    """ Get/set of a hash of 100 items (without _asdict) """
    d = {str(i): str(i) for i in range(100)}

    await connection.delete(["key"])
    await connection.hmset("key", d)

    result = await connection.hgetall("key")
    d2 = {}

    for f in result:
        k, v = await f
        d2[k] = v

    assert d2 == d


async def test4(connection):
    """ sadd/smembers of a set of 100 items. (with _asset) """
    s = {str(i) for i in range(100)}

    await connection.delete(["key"])
    await connection.sadd("key", list(s))

    s2 = await connection.smembers_asset("key")
    assert s2 == s


async def test5(connection):
    """ sadd/smembers of a set of 100 items. (without _asset) """
    s = {str(i) for i in range(100)}

    await connection.delete(["key"])
    await connection.sadd("key", list(s))

    result = await connection.smembers("key")
    s2 = set()

    for f in result:
        i = await f
        s2.add(i)

    assert s2 == s


benchmarks = [
    (1000, test1),
    (100, test2),
    (100, test3),
    (100, test4),
    (100, test5),
]


async def main():
    connection = await asyncio_redis.Connection.create(host="localhost", port=6379)
    if hiredis:
        hiredis_connection = await asyncio_redis.Connection.create(
            host="localhost", port=6379, protocol_class=HiRedisProtocol
        )

    try:
        for count, f in benchmarks:
            print("%ix %s" % (count, f.__doc__))

            # Benchmark without hredis
            start = time.time()
            for i in range(count):
                await f(connection)
            print("      Pure Python: ", time.time() - start)

            # Benchmark with hredis
            if hiredis:
                start = time.time()
                for i in range(count):
                    await f(hiredis_connection)
                print("      hiredis:     ", time.time() - start)
                print()
            else:
                print("      hiredis:     (not available)")
    finally:
        connection.close()
        if hiredis:
            hiredis_connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
