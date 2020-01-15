#!/usr/bin/env python
"""
Example of how the connection should reconnect to the server.
It's a loop that publishes 'message' in 'our-channel'.
"""
import asyncio
import logging
import asyncio_redis


async def main():
    # Enable logging
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.INFO)

    connection = await asyncio_redis.Connection.create(host='localhost', port=6379)

    try:
        while True:
            await asyncio.sleep(.5)

            try:
                # Try to send message
                await connection.publish('our-channel', 'message')
            except Exception as e:
                print ('errero', repr(e))
    finally:
        connection.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
