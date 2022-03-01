# import lxml
# import asyncio
# import aiohttp
# from bs4 import BeautifulSoup as bs
# import nest_asyncio
# nest_asyncio.apply()

# import time
# import pandas as pd

# async def _parse(data,ct):
#     pass

# async def _fetch(_mlbam):
#     retrieved_responses = []
#     async with aiohttp.ClientSession() as session:
#         tasks = []

#         for url in urls:
#             tasks.append(session.get(url, ssl=False))

#         responses = await asyncio.gather(*tasks)
        
#         for response in responses:
            
#             resp = await response.text()

#             retrieved_responses.append(resp)
        
#         await session.close()
    
#     return retrieved_responses

# def runit(_mlbam,_log=False):
#     start = time.time()

#     loop = asyncio.get_event_loop()
#     retrieved = loop.run_until_complete(_fetch(_mlbam))
    
#     if _log is True:
#         print(f"--- {time.time() - start } seconds ---")

#     return retrieved
