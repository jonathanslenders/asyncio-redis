import asyncio
import logging
import asyncio_redis

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    # Enable logging
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.INFO)

    def run():
        # Create a new redis connection (this will also auto reconnect)
        connection = yield from asyncio_redis.Connection.create('localhost', 6379)

        # Subscribe to a channel.
        subscriber = yield from connection.start_subscribe()
        yield from subscriber.subscribe([ 'our-channel' ])

        # Print published values in a while/true loop.
        while True:
            reply = yield from subscriber.get_next_published()
            print('Received: ', repr(reply.value), 'on channel', reply.channel)

    loop.run_until_complete(run())
