import time
import asyncio
import aiohttp
import datetime as dt
from urllib.parse import urlparse, parse_qs
from requests import Request

import pandas as pd
import numpy as np

div_record_label = {200:'vs_west', 201:'vs_east', 202:'vs_central',
                    203:'vs_west', 204:'vs_east', 205:'vs_central'}

lg_record_label  = {103:'vs_al', 104:'vs_nl'}

rename_cols = {
    'leftHome':'left_home',
    'leftAway':'left_away',
    'rightHome':'right_home',
    'rightAway':'right_away',
    'lastTen':'last_ten',
    'oneRun':'one_run',
    'extraInning':'extra_inning'
    }

lg_names = {
    103:'AL', 104:'NL',
    200:'AL West', 201:'AL East', 202:'AL Central',
    203:'NL West', 204:'NL East', 205:'NL Central',
    }

def get_league_label_name(league_mlbam:int, division_mlbam:int):
    if division_mlbam == 0:
        return lg_names.get(league_mlbam,'')
    else:
        return lg_names.get(division_mlbam,'')



async def parse_data(response:aiohttp.ClientResponse,**kwargs):
    url_components = urlparse(str(response.url))
    params = parse_qs(url_components.query)
    season = params['season'][0]

    standings_json: dict = await response.json()
    
    data = []
    for record in standings_json.get('records',[{}]):
        league_mlbam = record.get('league',{}).get('id',0)
        division_mlbam = record.get('division',{}).get('id',0)
        league_name = get_league_label_name(league_mlbam,division_mlbam)
        
        for tmrec in record.get('teamRecords',[{}]):
            records = tmrec.get('records',{})
            row_data = {
                'season':season,
                'team_mlbam': tmrec.get('team',{}).get('id',0),
                'team_name': tmrec.get('team',{}).get('name',''),
                'league_mlbam': league_mlbam,
                'division_mlbam': division_mlbam,
                'league_name': league_name,
                'games_played': tmrec.get('gamesPlayed',0),
                'wins': tmrec.get('wins',0),
                'losses': tmrec.get('losses',0),
                'win_perc': tmrec.get('winningPercentage','-'),
                'runs_scored': tmrec.get('runsScored',0),
                'runs_allowed': tmrec.get('runsAllowed',0),
                'run_differential': tmrec.get('runDifferential',0),
                'sport_rank': int(tmrec.get('sportRank',0)),
                'league_rank': int(tmrec.get('leagueRank',0)),
                'division_rank': tmrec.get('divisionRank',np.nan),
                'division_champ': tmrec.get('divisionChamp',False),
                'division_leader': tmrec.get('divisionLeader',False),
                'clinch_indicator': tmrec.get('clinchIndicator','-'),
            }
            # LeagueRecords
            for r in records.get('leagueRecords',[{}]):
                try:
                    label = lg_record_label[r.get('league',{})['id']]
                    split_rec = f"{r['wins']}-{r['losses']}"
                    if split_rec == '0-0': continue
                    row_data[label] = split_rec
                except:
                    pass
            
            # DivisionRecords
            for r in records.get('divisionRecords',[{}]):
                try:
                    label = div_record_label[r.get('division',{})['id']]
                    split_rec = f"{r['wins']}-{r['losses']}"
                    if split_rec == '0-0': continue
                    row_data[label] = split_rec
                except:
                    pass
                    
            # SplitRecords
            for r in records.get('splitRecords',[{}]):
                try:
                    split_rec = f"{r['wins']}-{r['losses']}"
                    if split_rec == '0-0': continue
                    row_data[r['type']] = split_rec
                except:
                    pass
            
            data.append(row_data)
    
    df = pd.DataFrame(data).rename(columns=rename_cols)

    return df

async def fetch_standings(**kwargs):
    base_url = "https://statsapi.mlb.com/api/v1/standings"
    params = {
        'sportId':'1',
        'leagueId':'103,104',
        'hydrate':'standings',
        'standingsType':'regularSeason'
    }
    
    dfs = []
    
    async with aiohttp.ClientSession() as sesh:
        tasks = []
        for season in range(1876,dt.datetime.today().year + 1):
            params['season'] = str(season)
            url = Request("GET",base_url,params=params).prepare().url
            if kwargs.get('log'):
                print(url)
            tasks.append(sesh.get(url))
        client_responses = await asyncio.gather(*tasks)
        for response in client_responses:
            dfs.append(await parse_data(response,**kwargs))
    
    df = pd.concat(dfs)
    return df

def runit(**kwargs):
    start = time.time()
    retrieved = asyncio.run(fetch_standings(**kwargs))
    if kwargs.get("log"):
        print(f'-- {time.time() - start} seconds --')
    return retrieved