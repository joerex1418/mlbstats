import asyncio
import aiohttp
import pandas as pd
import time
# from pprint import pprint

from ..constants import BASE
from ..constants import HITTING_CATEGORIES
from ..constants import PITCHING_CATEGORIES
from ..constants import FIELDING_CATEGORIES
from ..constants import BAT_FIELDS
from ..constants import BAT_FIELDS_ADV
from ..constants import PITCH_FIELDS
from ..constants import PITCH_FIELDS_ADV
from ..constants import FIELD_FIELDS
from ..constants import STATDICT

from ..constants import POSITION_DICT


async def parse_data(response):
    leaders = {}
    for category in response["leagueLeaders"]:
        try:
            category_name = category["leaderCategory"]
            entries_in_category = []
            for leader in category["leaders"]:
                entry = {}
                entry["stat"] = category["leaderCategory"]
                entry["rank"] = leader["rank"]
                entry["name"] = leader["person"]["fullName"]
                entry["mlbam"] = leader["person"]["id"]
                entry["value"] = leader["value"]
                entry["season"] = leader["season"]
                entry["team_name"] = leader["team"]["name"]
                entry["team_mlbam"] = leader["team"]["id"]
                entry["league_name"] = leader["league"]["name"]
                entry["league_mlbam"] = leader["league"]["id"]
                entry["gameType"] = category["gameType"]["id"]

                entries_in_category.append(entry)

            leaders[category_name] = entries_in_category
        except:
            leaders[category_name] = []
            print(category["statGroup"])
            print(category_name)
    return leaders

async def get_leaders(tm_mlbam=None,league_mlbam=None,season=None,gameTypes=None,sitCodes=None,limit=None,startDate=None,endDate=None,group_by_team=False):
    '''statTypes: season, statsSingleSeason, byDateRange'''
    parsed_data = []
    
    if tm_mlbam is None:
        teamQuery = ""
    else:
        teamQuery = f"teamId={tm_mlbam}&"

    if league_mlbam is None:
        leagueIds = "103,104"

    if gameTypes is None:
        gameTypes = "R"
    elif type(gameTypes) is list:
        gameTypes = ",".join(gameTypes)
    else:
        gameTypes = gameTypes.replace(" ","")

    if limit is None:
        limit = 1000

    if sitCodes is None:
        sitCodes = ""
    elif type(sitCodes) is list:
        sitCodes = f"&sitCodes={','.join(sitCodes)}"
    else:
        sitCodes = f"&sitCodes={sitCodes.replace(' ','')}"


    h_cats = ",".join(HITTING_CATEGORIES)
    p_cats = ",".join(PITCHING_CATEGORIES)
    f_cats = ",".join(FIELDING_CATEGORIES)

    if group_by_team is False:
        leader_ep = BASE + "/stats/leaders?leaderCategories="
    else:
        leader_ep = BASE + "/teams/stats/leaders?leaderCategories="

    hit_base = leader_ep + f"{h_cats}&statGroup=hitting&"
    pitch_base = leader_ep + f"{p_cats}&statGroup=pitching&"
    field_base = leader_ep + f"{f_cats}&statGroup=fielding&"
    
    # might need to remove 'sitCodes'

    async with aiohttp.ClientSession() as session:
        if startDate is not None and endDate is not None:
            if endDate is not None:
                sitCodes = ""
                statType = "byDateRange"
                hit_base +   f"statType={statType}&{teamQuery}gameTypes={gameTypes}&leagueIds={leagueIds}&startDate={startDate}&endDate={endDate}&limit={limit}{sitCodes}",
                pitch_base + f"statType={statType}&{teamQuery}gameTypes={gameTypes}&leagueIds={leagueIds}&startDate={startDate}&endDate={endDate}&limit={limit}{sitCodes}",
                field_base + f"statType={statType}&{teamQuery}gameTypes={gameTypes}&leagueIds={leagueIds}&startDate={startDate}&endDate={endDate}&limit={limit}{sitCodes}"
            else:
                print("startDate and endDate must be used together")
                return []

        elif season is None:
            sitCodes = ""
            statType = "statsSingleSeason"
            urls = [
                hit_base +   f"statType={statType}&{teamQuery}gameTypes={gameTypes}&leagueIds={leagueIds}&limit={limit}{sitCodes}",
                pitch_base + f"statType={statType}&{teamQuery}gameTypes={gameTypes}&leagueIds={leagueIds}&limit={limit}{sitCodes}",
                field_base + f"statType={statType}&{teamQuery}gameTypes={gameTypes}&leagueIds={leagueIds}&limit={limit}{sitCodes}"
                ]
        else:
            if sitCodes == "":
                statType = "season"
            else:
                statType = "statSplits"
            urls = [
                hit_base +   f"statType={statType}&{teamQuery}gameTypes={gameTypes}&leagueIds={leagueIds}&season={season}&limit={limit}{sitCodes}",
                pitch_base + f"statType={statType}&{teamQuery}gameTypes={gameTypes}&leagueIds={leagueIds}&season={season}&limit={limit}{sitCodes}",
                field_base + f"statType={statType}&{teamQuery}gameTypes={gameTypes}&leagueIds={leagueIds}&season={season}&limit={limit}{sitCodes}"
                ]

        tasks = []
        for url in urls:
            print(url)
            print("\n")
        for url in urls:
            tasks.append(session.get(url, ssl=False))

        responses = await asyncio.gather(*tasks)
        
        for response in responses:
            resp = await response.json()
            parsed = await parse_data(resp)
            parsed_data.append(parsed)

    parsed_data_dict = {
        "hitting":parsed_data[0],
        "pitching":parsed_data[1],
        "fielding":parsed_data[2]
    }
    return parsed_data_dict

def runit(tm_mlbam=None,league_mlbam=None,season=None,gameTypes=None,sitCodes=None,limit=None,startDate=None,endDate=None,group_by_team=False):
    start = time.time()
    retrieved = asyncio.run(get_leaders(tm_mlbam,league_mlbam,season,gameTypes,sitCodes,limit,startDate,endDate,group_by_team))
    print("--- {} seconds ---".format(time.time()-start))
    return retrieved


