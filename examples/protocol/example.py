import asyncio
from asyncio_redis import RedisProtocol

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    def run():
        # Create connection
        transport, protocol = yield from loop.create_connection(RedisProtocol, 'localhost', 6379)

        # Set a key
        yield from protocol.set('key', 'value')

        # Retrieve a key
        result = yield from protocol.get('key')

        # Print result
        print ('Succeeded', result == 'value')

    loop.run_until_complete(run())
