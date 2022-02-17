#%%
import asyncio
import requests
import aiohttp
import time

from requests.models import Response
# url = "https://widgets.sports-reference.com/wg.fcgi?site=br&url=%2Fplayers%2F{}%2F{}.shtml&div=div_batting_standard"
url = "https://www.baseball-reference.com/players/{}/{}.shtml"
results = []
bbrefIDs = [
    ("abreujo01","J. Abreu"),
    ("madrini01","N. Madrigal"),
    ("anderti01","T. Anderson"),
    ("engelad01","A. Engel"),
    ("moncayo01","Y. Moncada"),
    ("grandya01","Y. Grandal"),
    ("merceye01","Y. Mercedes"),
    ("adamja01","J. Adam"),
    ("allarko01","K. Allard"),
    ("gehrilo01","L.Gehrig"),
    ("mantlmi01","M. Mantle")]
start = time.time()

print("let's try this")
async def get_pages(bbrefIDs):
    async with aiohttp.ClientSession() as session:
        for bbrefID,player in bbrefIDs:
            print("Working on {}".format(player))
            response = await session.get(url.format(bbrefID[0],bbrefID),ssl=False)
            results.append(await response.text())
            
asyncio.run(get_pages(bbrefIDs))

print("--- {} seconds ---".format(time.time()-start))
#%%