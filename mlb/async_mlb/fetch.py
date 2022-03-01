import lxml
import asyncio
import aiohttp
from bs4 import BeautifulSoup as bs
import nest_asyncio
nest_asyncio.apply()

import time
# import pandas as pd

async def fetch(urls:list):
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

def runit(urls:list,**kwargs):
    start = time.time()
    # retrieved = asyncio.run(fetch(urls))

    loop = asyncio.get_event_loop()
    retrieved = loop.run_until_complete(fetch(urls))
    if kwargs.get("_log") is True:
        print(f"--- {time.time() - start } seconds ---")

    return retrieved
