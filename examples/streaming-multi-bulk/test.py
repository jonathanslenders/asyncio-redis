#!/usr/bin/env python
"""
Example of how an 'smembers' call gets streamed when it's a big reply, covering
multiple IP packets.
"""
import asyncio
import logging

import asyncio_redis


async def main():
    # Enable logging
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.INFO)

    connection = await asyncio_redis.Connection.create(host="localhost", port=6379)

    # Create a set that contains a million items
    print("Creating big set contains a million items (Can take about half a minute)")

    await connection.delete(["my-big-set"])

    # We will suffix all the items with a very long key, just to be sure
    # that this needs many IP packets, in order to send or receive this.
    long_string = "abcdefghij" * 1000  # len=10k

    for prefix in range(10):
        print("Callidng redis sadd:", prefix, "/10")
        await connection.sadd(
            "my-big-set",
            ("%s-%s-%s" % (prefix, i, long_string) for i in range(10 * 1000)),
        )
    print("Done\n")

    # Now stream the values from the database:
    print("Streaming values, calling smembers")

    # The following smembers call will block until the first IP packet
    # containing the head of the multi bulk reply comes in. This will
    # contain the size of the multi bulk reply and that's enough
    # information to create a SetReply instance. Probably the first packet
    # will also contain the first X members, so we don't have to wait for
    # these anymore.
    set_reply = await connection.smembers("my-big-set")
    print("Got: ", set_reply)

    # Stream the items, this will probably wait for the next IP packets to come in.
    count = 0
    for f in set_reply:
        await f
        count += 1
        if count % 1000 == 0:
            print("Received %i items" % count)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
