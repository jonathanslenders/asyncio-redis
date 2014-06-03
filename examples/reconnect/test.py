#!/usr/bin/env python
"""
Example of how the connection should reconnect to the server.
It's a loop that publishes 'message' in 'our-channel'.
"""
import asyncio
import logging
import asyncio_redis

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    # Enable logging
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.INFO)

    def run():
        connection = yield from asyncio_redis.Connection.create(host='localhost', port=6379)

        try:
            while True:
                yield from asyncio.sleep(.5)

                try:
                    # Try to send message
                    yield from connection.publish('our-channel', 'message')
                except Exception as e:
                    print ('errero', repr(e))
        finally:
            connection.close()

    loop.run_until_complete(run())
