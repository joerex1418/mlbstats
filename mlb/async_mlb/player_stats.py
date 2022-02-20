import asyncio
import aiohttp
# import pandas as pd
# import time

from ..constants import BASE
# from ..constants import BAT_FIELDS
# from ..constants import BAT_FIELDS_ADV
# from ..constants import PITCH_FIELDS
# from ..constants import PITCH_FIELDS_ADV
# from ..constants import FIELD_FIELDS
# from ..constants import STATDICT

# from ..constants import POSITION_DICT


def get_tasks(session,mlbam,season):

    endpoints = {
        "team":"/teams/stats?stats=season,seasonAdvanced&group=hitting,pitching,fielding&season={season}&leagueIds=103,104&hydrate=team(division)",

        "players":"/stats?stats=season,seasonAdvanced&group=hitting,pitching,fielding&playerPool=all&limit=2000&leagueIds=103,104&season={season}&hydrate=team",

        "standings":"/standings?leagueId=103,104&sportId=1&season={season}"
    }
    tasks = []
    for ep in endpoints.values():
        tasks.append(session.get(BASE + ep.format(season=season), ssl=False))
    return tasks

async def parse_data(response,idx,mlbam):
    pass

async def get_player_responses(mlbam,season):
    parsed_data = []

    statTypes = "season,seasonAdvanced"
    statGroups = "hitting,pitching,fielding"
    team_hydrations = "team(standings)"
    roster_hydrations = f"person(stats(type=[{statTypes}],group=[{statGroups}],season={season}))&season={season}"
    log_hydrations = "" # "team,decisions,gameInfo,venue,linescore,weather,series"

    async with aiohttp.ClientSession() as session:
        
        endpoints = (
            f"/players"
        )
        
        tasks = []
        for ep in endpoints:
            tasks.append(session.get(BASE + ep, ssl=False))

        responses = await asyncio.gather(*tasks)
        
        for idx, response in enumerate(responses):
            resp = await response.json()
            parsed = await parse_data(resp,idx,mlbam)
            parsed_data.append(parsed)

    parsed_data_dict = {
        "team_stats":parsed_data[0],
        "roster_stats":parsed_data[1],
        "game_log":parsed_data[2],
        "game_stats":parsed_data[3]
    }
    return parsed_data_dict

def runit(mlbam,season):
    # start = time.time()
    retrieved = asyncio.run(get_player_responses(mlbam,season))
    # print("--- {} seconds ---".format(time.time()-start))
    return retrieved


