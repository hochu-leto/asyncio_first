import time
from pprint import pprint
from sqlalchemy import Integer, String, Column, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import asyncio
import random
import asyncpg
from faker import Faker
import config
import aiohttp
from more_itertools import chunked

CURENCY = 100

engine = create_async_engine(config.PG_DSN_ALC, echo=True)
Base = declarative_base()


async def call_api(url: str, sess: aiohttp.ClientSession()):
    async with sess.get(url) as re:
        if re.status == 200:
            return await re.json()


async def main_async():
    async with aiohttp.ClientSession() as s:
        curr = await call_api('https://swapi.dev/api/people/', s)
        coros = []
        for i in range(curr['count']):
            coros.append(call_api(f'https://swapi.dev/api/people/{i}', s))

        api = await asyncio.gather(*coros)
        c = 0
        for r in api:

            if r and 'name' in r.keys():
                c += 1
                print(f'{r["name"]}')

        print(f'{curr["count"]=}  {c=}')


class Person(Base):

    __tablename__ = 'star_wars'

    id = Column(Integer, primary_key=True)
    name = Column(String(128), index=True)
    admin = Column(Boolean, default=False)
    # id - ID
    # персонажа
    # birth_year
    # eye_color
    # films - строка
    # с
    # названиями
    # фильмов
    # через
    # запятую
    # gender
    # hair_color
    # height
    # homeworld
    # mass
    # name
    # skin_color
    # species - строка
    # с
    # названиями
    # типов
    # через
    # запятую
    # starships - строка
    # с
    # названиями
    # кораблей
    # через
    # запятую
    # vehicles

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

fake = Faker()


def gen_users_data(quantity: int):

    for _ in range(quantity):
        yield (
            fake.name(),
            random.choice([False, True])
        )


async def insert_users(pool: asyncpg.Pool, user_list):
    query = 'INSERT INTO users (name, admin) VALUES ($1, $2)'
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(query, user_list)


async def main():
    pool = await asyncpg.create_pool(config.PG_DSN, min_size=20, max_size=20)
    tasks = []
    for users_chunk in chunked(gen_users_data(10000), 1000):
        tasks.append(asyncio.create_task(insert_users(pool, users_chunk)))

    await asyncio.gather(*tasks)
    await pool.close()


async def main_create_table():
    await get_async_session(True, True)


if __name__ == '__main__':
    st = time.time()
    asyncio.run(main_async())
    print(time.time() - st)
