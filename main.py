import asyncio
import time

import aiohttp
import requests


async def call_api(url: str, sess: aiohttp.ClientSession()):
    async with sess.get(url) as re:
        if re.status == 200:
            return await re.json()


def main():
    curr = requests.get('http://api.coincap.io/v2/assets').json()
    curr = [item['id'] for item in curr['data']]
    for i in curr:
        r = requests.get(f'http://api.coincap.io/v2/assets/{i}').json()['data']
        print(f'{r["name"]} = {r["priceUsd"]} USD')


async def main_async():
    async with aiohttp.ClientSession() as s:
        curr = await call_api('http://api.coincap.io/v2/assets', s)
        curr = [item['id'] for item in curr['data']]
        cor = []
        for i in curr:
            resp = call_api(f'http://api.coincap.io/v2/assets/{i}', s)

            cor.append(resp)
        api = await asyncio.gather(*cor)
        for ap in api:
            r = ap['data']
            print(f'{r["name"]} = {r["priceUsd"]} USD')


if __name__ == '__main__':
    st = time.time()
    asyncio.run(main_async())
    print(time.time() - st)
