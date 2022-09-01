import asyncio
import aiohttp
import nest_asyncio
nest_asyncio.apply()

import time
def _determine_loop():
    try:
        return asyncio.get_event_loop()
    except:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return asyncio.get_event_loop()

class FetchedResponse:
    def __init__(self,_url,_headers,_json) -> None:
        self.url: str = _url
        self.headers: dict = _headers
        self.json: dict = _json

async def fetch(urls:list):
    retrieved_responses = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(session.get(url, ssl=True))

        responses = await asyncio.gather(*tasks)
        
        for response in responses:
            resp_url = str(response.url)
            resp_headers = dict(response.headers)
            resp_json = await response.json()
            
            sync_resp = FetchedResponse(resp_url,resp_headers,resp_json)
            
            retrieved_responses.append(sync_resp)
        
        await session.close()
    
    return retrieved_responses

def runit(urls:list,**kwargs) -> list[FetchedResponse]:
    start = time.time()
    loop = _determine_loop()
    retrieved = loop.run_until_complete(fetch(urls))
    if kwargs.get("log",kwargs.get("logtime")):
        print(f"--- {time.time() - start } seconds ---")

    return retrieved
