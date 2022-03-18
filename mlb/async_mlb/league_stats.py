# import asyncio
# import aiohttp
# # import time


# def get_tasks(session,season):
#     base = "http://statsapi.mlb.com/api/v1"

#     endpoints = {
#         "team":"/teams/stats?stats=season,seasonAdvanced&group=hitting,pitching,fielding&season={season}&leagueIds=103,104&hydrate=team(division)",

#         "players":"/stats?stats=season,seasonAdvanced&group=hitting,pitching,fielding&playerPool=all&limit=2000&leagueIds=103,104&season={season}&hydrate=team",

#         "standings":"/standings?leagueId=103,104&sportId=1&season={season}"
#     }
#     tasks = []
#     for ep in endpoints.values():
#         tasks.append(session.get(base + ep.format(season=season), ssl=False))
#     return tasks

# async def parse_data(response):
#     pass

# async def get_league_responses(season):
#     retrieved = []
#     async with aiohttp.ClientSession() as session:
#         base = "http://statsapi.mlb.com/api/v1"
#         endpoints = {
#             "team":"/teams/stats?stats=season,seasonAdvanced&group=hitting,pitching,fielding&season={season}&leagueIds=103,104&hydrate=team(division)",

#             "players":"/stats?stats=season,seasonAdvanced&group=hitting,pitching,fielding&playerPool=all&limit=2000&leagueIds=103,104&season={season}", #&hydrate=team",

#             "standings":"/standings?leagueId=103,104&sportId=1&season={season}"
#         }
#         tasks = []
#         for ep in endpoints.values():
#             tasks.append(session.get(base + ep.format(season=season), ssl=False))

#         responses = await asyncio.gather(*tasks)
#         for response in responses:
#             resp = await response.json()
#             retrieved.append(resp)

#     return retrieved

# def runit(season):
#     # start = time.time()
#     retrieved = asyncio.run(get_league_responses(season))
#     # print("--- {} seconds ---".format(time.time()-start))
#     return retrieved

# if __name__ == "__main__":
#     runit(2021)

