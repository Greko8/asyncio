# import logging
import asyncio
import aiohttp
from models import Session, SwapiPeople, init_db
from more_itertools import chunked

CHUNK_SIZE = 10

async def internal_data(url_list, session):
    string = []
    for url in url_list:
        response = await session.get(url)
        data = await response.json(content_type=None)
        string.append(data["name"] if "name" in data else data["title"])
    return ", ".join(string)


# get_person_logger = logging.getLogger("get_person")
async def get_person(person_id, session):
    url = f"https://swapi.dev/api/people/{person_id}/"
    response = await session.get(url)
    data = await response.json()

    if response.status == 404:
        return

    del data["created"]
    del data["edited"]
    del data["url"]

    data["films"] = await internal_data(data["films"], session)
    data["species"] = await internal_data(data["species"], session)
    data["starships"] = await internal_data(data["starships"], session)
    data["vehicles"] = await internal_data(data["vehicles"], session)

    return data

async def insert_to_db(poeple_list: list):
    async with Session() as session:
        people = [SwapiPeople(**data) for data in poeple_list if data is not None]
        session.add_all(people)
        await session.commit()

async def main():
    await init_db()
    session = aiohttp.ClientSession()

    for people_id_chunk in chunked(range(1, 100), CHUNK_SIZE):
        coros = [get_person(person_id, session) for person_id in people_id_chunk]
        result = await asyncio.gather(*coros)
        asyncio.create_task(insert_to_db(result))

    await session.close()

    set_of_tasks = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*set_of_tasks)

if __name__ == "__main__":
    asyncio.run(main())