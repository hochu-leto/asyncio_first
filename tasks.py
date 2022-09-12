import asyncio

import aiohttp


async def some_task():
    print('begin task')
    async with aiohttp.ClientSession() as se:
        print((await se.get('http://yandex.ru')).status)
    print('end task')


async def some_coro():
    print('begin coro')
    async with aiohttp.ClientSession() as se:
        print((await se.get('http://yandex.ru')).status)
    print('end coro')


async def main():
    asyncio.create_task(some_task())
    await some_coro()
    await some_coro()
    print('eeeeend')

asyncio.run(main())
