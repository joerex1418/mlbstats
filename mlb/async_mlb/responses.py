import asyncio
import aiohttp
import nest_asyncio
nest_asyncio.apply()
# __import__('IPython').embed()

import time
# import pandas as pd

async def get_responses(urls:list):
    retrieved_responses = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(session.get(url, ssl=True))

        responses = await asyncio.gather(*tasks)
        
        for response in responses:
            resp = await response.json()
            retrieved_responses.append(resp)
        
        await session.close()
    
    return retrieved_responses

def runit(urls:list):
    start = time.time()
    # retrieved = asyncio.run(get_responses(urls))

    loop = asyncio.get_event_loop()
    retrieved = loop.run_until_complete(get_responses(urls))
    print(f"--- {time.time() - start } seconds ---")

    return retrieved
