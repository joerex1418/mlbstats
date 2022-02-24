import asyncio
import aiohttp

import time
# import pandas as pd

async def get_responses(urls:list):
    retrieved_responses = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(session.get(url, ssl=False))

        responses = await asyncio.gather(*tasks)
        
        for response in responses:
            resp = await response.json()
            retrieved_responses.append(resp)
    
    return retrieved_responses

def runit(urls:list) -> list[aiohttp.ClientResponse]:
    start = time.time()
    retrieved = asyncio.run(get_responses(urls))
    print(f"--- {time.time() - start } seconds ---")
    return retrieved
