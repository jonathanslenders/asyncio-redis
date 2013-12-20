import asyncio
from asyncio_redis import RedisProtocol

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    def run():
        transport, protocol = yield from loop.create_connection(RedisProtocol, 'localhost', 6379)
        subscriber = yield from protocol.start_subscribe()
        yield from subscriber.subscribe([ 'our-channel' ])

        while True:
            reply = yield from subscriber.get_next_published()
            print('Received: ', repr(reply.value), 'on channel', reply.channel)

    loop.run_until_complete(run())
