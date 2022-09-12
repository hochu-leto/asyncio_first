import asyncio
import time
from pprint import pprint

from sqlalchemy import Integer, String, Column, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import config

import aiohttp
import requests
from more_itertools import chunked

CURENCY = 100


async def call_api(url: str, sess: aiohttp.ClientSession()):
    async with sess.get(url) as re:
        if re.status == 200:
            return await re.json()


def main2():
    curr = requests.get('https://swapi.dev/api/people/').json()
    pprint(curr)
    # curr = [item['id'] for item in curr['results']]
    for i in range(curr['count']):
        r = requests.get(f'https://swapi.dev/api/people//{i}').json()

        print(f'{r} ')


async def main_async():
    async with aiohttp.ClientSession() as s:
        curr = await call_api('https://swapi.dev/api/people/', s)
        coros = (call_api(f'https://swapi.dev/api/people/{i}', s)
               for i in range(curr['count']))
        # coros =[]
        # for i in range(curr['count']):
        #     coros.append(call_api(f'https://swapi.dev/api/people/{i}', s))

        api = await asyncio.gather(*coros)
        c = 0
        for r in api:

            if r and 'name' in r.keys():
                c += 1
                print(f'{r["name"]}')

        print(f'{curr["count"]=}  {c=}')

engine = create_async_engine(config.PG_DSN_ALC, echo=True)
Base = declarative_base()


class Person(Base):

    __tablename__ = 'star_wars'

    id = Column(Integer, primary_key=True)
    name = Column(String(128), index=True)
    admin = Column(Boolean, default=False)


async def get_async_session(
    drop: bool = False, create: bool = False
):

    async with engine.begin() as conn:
        if drop:
            await conn.run_sync(Base.metadata.drop_all)
        if create:
            print(1)
            await conn.run_sync(Base.metadata.create_all)
    async_session_maker = sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )

    return async_session_maker


async def main():
    await get_async_session(True, True)


if __name__ == '__main__':
    st = time.time()
    asyncio.run(main_async())
    print(time.time() - st)
