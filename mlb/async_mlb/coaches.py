import asyncio
import aiohttp
from urllib.parse import urlparse, parse_qs

import pandas as pd

from ..mlbdata import get_teams_df

TEAMS = get_teams_df().sort_values(by='season',ascending=False)

def roster_json_to_df(rosters_list:list[dict]):
    data = []
    for roster_dict in rosters_list:
        try:
            for entry in roster_dict['roster']:
                entry: dict
                person = entry.pop('person',{})
                entry['season'] = roster_dict['season']
                entry['person_mlbam'] = person.get('id',0)
                entry['person_name'] = person.get('fullName','')
                entry['team_mlbam'] = roster_dict['team_mlbam']
                entry['team_name'] = roster_dict['team_name']
                data.append(entry)
        except:
            data.append({
                'season':roster_dict['season'],
                'person_mlbam': 0,
                'person_name': '',
                'team_mlbam':roster_dict['team_mlbam'],
                'team_name':roster_dict['team_name'],
            })
    df = pd.DataFrame(data)
    return df

async def parse_data(response:aiohttp.ClientResponse):
    url_components = urlparse(str(response.url))
    path = url_components.path
    params = parse_qs(url_components.query)
    
    last_slash_idx = path.rfind(r'/')
    first_slash_idx = path[:last_slash_idx].rfind(r'/')
    
    mlbam = path[first_slash_idx+1:last_slash_idx]
    season = params['season'][0]
    
    team_row = TEAMS[(TEAMS['mlbam']==int(mlbam)) & (TEAMS['season']==int(season))].iloc[0]
    
    roster: dict = await response.json()
    roster['season'] = int(season)
    roster['team_mlbam'] = roster.pop('teamId')
    roster['team_name'] = team_row['name_full']
    
    return roster

async def fetch_coaches():
    parsed_responses = []
    async with aiohttp.ClientSession() as sesh:
        tasks = []
        for idx,row in TEAMS.iterrows():
            mlbam, season = (row['mlbam'], row['season'])
            url = f'https://statsapi.mlb.com/api/v1/teams/{mlbam}/coaches?season={season}'
            tasks.append(sesh.get(url,ssl=False))
        client_responses = await asyncio.gather(*tasks)
        for response in client_responses:
            parsed_responses.append(await parse_data(response))
            
    return parsed_responses

def runit():
    retrieved = asyncio.run(fetch_coaches())
    return retrieved