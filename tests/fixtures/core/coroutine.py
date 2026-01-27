import asyncio


async def simple_background():
    while True:
        await asyncio.sleep(0.1)

