#!/usr/bin/env python
"""
Simple example that sets a key, and retrieves it again.
"""
import asyncio

from asyncio_redis import RedisProtocol


async def main():
    # Create connection
    transport, protocol = await loop.create_connection(RedisProtocol, "localhost", 6379)

    # Set a key
    await protocol.set("key", "value")

    # Retrieve a key
    result = await protocol.get("key")

    # Print result
    print("Succeeded", result == "value")

    transport.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
