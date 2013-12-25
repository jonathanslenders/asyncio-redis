import asyncio
import logging
import asyncio_redis

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    # Enable logging
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.INFO)

    def run():
        connection = yield from asyncio.Connection.create('localhost', 9999)

        while True:
            yield from asyncio.sleep(.5)

            try:
                # Try to send message
                yield from connection.publish('our-channel', 'message')
            except Exception as e:
                print ('errero', repr(e))

    loop.run_until_complete(run())
