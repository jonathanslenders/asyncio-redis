import asyncio
from asyncio_redis import RedisProtocol

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    def run():
        transport, protocol = yield from loop.create_connection(RedisProtocol, 'localhost', 6379)
        while True:
            text = input('Enter message: ')
            yield from protocol.publish('our-channel', text)
            print('Sent.')

    loop.run_until_complete(run())
