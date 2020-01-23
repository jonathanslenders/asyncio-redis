#!/usr/bin/env python
"""
Benchmank how long it takes to set 10,000 keys in the database.
"""
import asyncio
import logging
import time

import asyncio_redis


async def main():
    # Enable logging
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.INFO)

    # connection = await asyncio_redis.Connection.create(host='localhost', port=6379)
    connection = await asyncio_redis.Pool.create(
        host="localhost", port=6379, poolsize=50
    )

    try:
        # === Benchmark 1 ==
        print(
            "1. How much time does it take to set 10,000 values in Redis? (without pipelining)"
        )
        print("Starting...")
        start = time.time()

        # Do 10,000 set requests
        for i in range(10 * 1000):
            await connection.set(
                "key", "value"
            )  # By using await here, we wait for the answer.

        print("Done. Duration=", time.time() - start)
        print()

        # === Benchmark 2 (should be at least 3x as fast) ==

        print(
            "2. How much time does it take if we use asyncio.gather, and pipeline requests?"
        )
        print("Starting...")
        start = time.time()

        # Do 10,000 set requests
        futures = [
            asyncio.Task(connection.set("key", "value")) for x in range(10 * 1000)
        ]
        await asyncio.gather(*futures)

        print("Done. Duration=", time.time() - start)

    finally:
        connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
