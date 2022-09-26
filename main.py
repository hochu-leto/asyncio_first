import time
from aiopg.sa import create_engine
from sqlalchemy import Integer, String, Column, Table, MetaData
import asyncio
import aiohttp


async def call_api(url: str, sess):
    async with sess.get(url) as re:
        if re.status == 200:
            return await re.json()


async def main_async():
    engine = await create_engine(
        user="app", database="app", host="127.0.0.1", port='5431', password="secret"
    )
    async with engine:
        async with engine.acquire() as conn:
            await create_tables(conn)
            async with aiohttp.ClientSession() as s:
                curr = await call_api('https://swapi.dev/api/people/', s)
                for i in range(curr['count']):
                    person = call_api(f'https://swapi.dev/api/people/{i}', s)
                    await fill_data(conn, await person, s, i)


star_wars = Table('star_wars',
                  MetaData(),
                  Column('id', Integer, primary_key=True),
                  Column('birth_year', String(8), index=True),
                  Column('eye_color', String(128), index=True),
                  Column('films', String, index=True),  # - строка с названиями фильмов через запятую
                  Column('gender', String(128), index=True),
                  Column('hair_color', String, index=True),
                  Column('height', String(128), index=True),
                  Column('homeworld', String, index=True),
                  Column('mass', String(128), index=True),
                  Column('name', String, index=True),
                  Column('skin_color', String(128), index=True),
                  Column('species', String, index=True),  # - строка с названиями типов через запятую
                  Column('starships', String, index=True),  # - строка с названиями кораблей через запятую
                  Column('vehicles', String, index=True))


async def create_tables(conn):
    await conn.execute("DROP TABLE IF EXISTS star_wars")
    await conn.execute(
        """CREATE TABLE star_wars (
                   id serial PRIMARY KEY,
                   birth_year varchar(8),
                  eye_color varchar(128),
                  films varchar(255),
                  gender varchar(128),
                  hair_color varchar(255),
                  height varchar(128), 
                  homeworld varchar(255),
                  mass varchar(128),
                  name varchar(255),
                  skin_color varchar(128),
                  species varchar(255),
                  starships varchar(255),
                  vehicles varchar(255))"""
    )


async def fill_data(conn, person, session, id):
    async with conn.begin():
        if person:
            spec = ''
            for p in person['species']:
                spec += (await call_api(p, session))['name'] + ", "
            ship = ''
            for p in person['starships']:
                ship += (await call_api(p, session))['name'] + ", "
            film = ''
            for p in person['films']:
                film += (await call_api(p, session))['title'] + ', '
            await conn.execute(
                star_wars.insert().values(
                    id=id,
                    birth_year=person['birth_year'],
                    starships=ship,
                    mass=person['mass'],
                    eye_color=person['eye_color'],
                    films=film,
                    gender=person['gender'],
                    hair_color=person['hair_color'],
                    height=person['height'],
                    homeworld=person['homeworld'],
                    name=person['name'],
                    skin_color=person['skin_color'],
                    species=spec,
                    vehicles=person['vehicles']
                ))


if __name__ == '__main__':
    st = time.time()
    api = asyncio.run(main_async())
    print(time.time() - st)
