import datetime as dt
from typing import Union, Optional, TypeAlias, TypeVar

import nest_asyncio
import pandas as pd

from . import constants as c
from . import parsing
from . import mlb_dataclasses as dclass
from .async_mlb import fetch
from .utils import default_season

from . import helpers

from .functions import schedule, season_standings, league_stats

nest_asyncio.apply()

# DateOrStr = Union[str,Union[dt.datetime,dt.date]]

def fetch_home_page_content(**kwargs):
    date = dt.date.today()
    date = date.strftime(r'%Y-%m-%d')
    hydrations = "linescore,person,decisions,lineups(person),probablePitcher"
    
    scores_url = schedule(date=date,hydrate=hydrations,url_only=True,log=kwargs.get('log'))
    standings_url = season_standings(url_only=True,log=kwargs.get('log'))
    stats_url = league_stats(url_only=True,log=kwargs.get('log'))
    
    urls = [scores_url,standings_url,stats_url]
    
    fetched_responses = fetch(urls)
    for resp in fetched_responses:
        if '/schedule' in resp.url:
            scores = pd.DataFrame(data=parsing._parse_schedule_data(resp.json))
            official_dt_col = pd.to_datetime(scores["date_official"] + " " + scores["game_start"],format=r"%Y-%m-%d %I:%M %p")
            scores.insert(0,"official_dt",official_dt_col)
        elif '/standings' in resp.url:
            standings = pd.DataFrame(parsing._parse_season_standings_data(resp.json))
        elif '/stats' in resp.url:
            # stats = _new_stat_collection(resp.json)
            stats = dclass.StatTypeCollection.from_json(resp.json)
            
    return (scores, standings, stats)

def fetch_team_page_content(team_id:int,date:Union[str,Union[dt.datetime,dt.date]]=None,**kwargs):
    base = "https://statsapi.mlb.com/api/v1"
    
    if date is None:
        date_obj = dt.date.today()
        date_str = date_obj.strftime(r'%Y-%m-%d')
    else:
        if type(date) is str:
            date_obj = dt.datetime.strptime(date,r'%Y-%m-%d')
            date_str = date
        elif type(date) is dt.datetime:
            date_obj = date.date()
            date_str = date.strftime(r'%Y-%m-%d')
        elif type(date) is dt.date:
            date_obj = date
            date_str = date.strftime(r'%Y-%m-%d')
        else:
            date_obj = dt.date.today()
            date_str = date_obj.strftime(r'%Y-%m-%d')
    
    if kwargs.get('season') is None:
        season = default_season()
    else:
        season = kwargs['season']
    
    roster_type = 'fullRoster'
    urls = [
        base + f"/teams/{team_id}/roster?rosterType={roster_type}&hydrate=person(stats(type=[season,seasonAdvanced],group=hitting))",
        base + f"/teams/{team_id}/roster?rosterType={roster_type}&hydrate=person(stats(type=[season,seasonAdvanced],group=pitching))",
        base + f"/teams/{team_id}/roster?rosterType={roster_type}&hydrate=person(stats(type=[season,seasonAdvanced],group=fielding))",
        base + f"/teams/{team_id}/roster?rosterType=40Man",
        base + f"/teams/{team_id}/roster?rosterType=active",
        base + f"/teams/{team_id}/stats?stats=season,seasonAdvanced&group=hitting,pitching,fielding&season={season}",
        base + f"/teams/{team_id}?hydrate=nextSchedule(limit=5),venue(fieldInfo)",
        base + f"/draft/{season}?teamId={team_id}",
        base + f"/transactions?teamId={team_id}&startDate={date_obj-dt.timedelta(days=30)}&endDate={date_obj}",
    ]
            
    fetched_responses = fetch(urls)
    
    hitting_dfs, pitching_dfs, fielding_dfs = [], [], []
    hitting_adv_dfs, pitching_adv_dfs = [], []
    
    tm_hitting_dfs, tm_pitching_dfs, tm_fielding_dfs = [], [], []
    tm_hitting_adv_dfs, tm_pitching_adv_dfs = [], []
    
    players = {}
    for resp in fetched_responses:
        if kwargs.get('log') is True:
            print(resp.url)
        if '/roster' in resp.url and roster_type in resp.url:
            full = parsing._parse_roster(resp.json)
            for entry in resp.json.get('roster',[{}]):
                person = entry.get('person',{})
                stats = person.pop('stats',[{}])
                players[f"ID{person.get('id')}"] = person
                for s in stats:
                    if 'advanced' not in s.get('type',{}).get('displayName','').lower():
                        if s.get('group',{}).get('displayName') == "hitting":
                            hitting_dfs.append(parsing._parse_player_stats(s.get('splits',[{}]),include_team_name=False))
                        elif s.get('group',{}).get('displayName') == "pitching":
                            pitching_dfs.append(parsing._parse_player_stats(s.get('splits',[{}]),include_team_name=False))
                        elif s.get('group',{}).get('displayName') == "fielding":
                            df = parsing._parse_player_stats(s.get('splits',[{}]),include_team_name=False,keep_original_keys=True)
                            fielding_dfs.append(df)
                    else:
                        if s.get('group',{}).get('displayName') == "hitting":
                            hitting_adv_dfs.append(parsing._parse_player_stats(s.get('splits',[{}]),include_team_name=False))
                        elif s.get('group',{}).get('displayName') == "pitching":
                            pitching_adv_dfs.append(parsing._parse_player_stats(s.get('splits',[{}]),include_team_name=False))
        elif '/roster' in resp.url and '40Man' in resp.url:
            fortyman = parsing._parse_roster(resp.json)
        elif '/roster' in resp.url and 'active' in resp.url:
            active = parsing._parse_roster(resp.json)
        elif 'teams' in resp.url and 'stats=season,seasonAdvanced' in resp.url:
            for s in resp.json.get('stats',[{}]):
                if s.get('type',{}).get('displayName').lower().find('advanced') == -1:
                    if s.get('group',{}).get('displayName') == "hitting":
                        tm_hitting_dfs.append(parsing._parse_team_stats(s.get('splits',[{}]),True))
                    elif s.get('group',{}).get('displayName') == "pitching":
                        tm_pitching_dfs.append(parsing._parse_team_stats(s.get('splits',[{}]),True))
                    elif s.get('group',{}).get('displayName') == "fielding":
                        tm_fielding_dfs.append(parsing._parse_team_stats(s.get('splits',[{}]),True,keep_original_keys=True))
                else:
                    if s.get('group',{}).get('displayName') == "hitting":
                        tm_hitting_adv_dfs.append(parsing._parse_team_stats(s.get('splits',[{}]),True))
                    elif s.get('group',{}).get('displayName') == "pitching":
                        tm_pitching_adv_dfs.append(parsing._parse_team_stats(s.get('splits',[{}]),True))
        elif 'teams' in resp.url:
            team_info = resp.json.get('teams',[{}])[0]
            team_info = dclass.TeamInfo(
                name=dclass.TeamName(
                    team_info.get('id',0),
                    team_info.get('name',''),
                    team_info.get('locationName',''),
                    team_info.get('franchiseName',''),
                    team_info.get('clubName',''),
                    team_info.get('shortName',''),
                    team_info.get('abbreviation',''),
                ),
                venue=dclass.Venue(
                    team_info.get('venue',{}).get('id',0),
                    team_info.get('venue',{}).get('name',''),
                    ),
                league=dclass.Leagues.get(team_info.get('league',{}).get('id',0)),
                division=dclass.Leagues.get(team_info.get('division',{}).get('id',0)),
                first_year=team_info.get('firstYearOfPlay',''),
                season=str(team_info.get('season')),
            )
        elif '/draft' in resp.url:
            draft = resp.json
        elif '/transactions' in resp.url:
            transactions = parsing._parse_transaction_data(resp.json)
            transactions = pd.DataFrame(transactions).sort_values(by='date',ascending=False).reset_index(drop=True)
    
    stats = dclass.TeamStats(
        players=dclass.StatTypeCollection(
            hitting=pd.concat(hitting_dfs),
            pitching=pd.concat(pitching_dfs),
            _fielding=pd.concat(fielding_dfs).reset_index(drop=True),
            hitting_adv=pd.concat(hitting_adv_dfs),
            pitching_adv=pd.concat(pitching_adv_dfs)
        ),
        totals=dclass.StatTypeCollection(
            hitting=pd.concat(tm_hitting_dfs),
            pitching=pd.concat(tm_pitching_dfs),
            _fielding=pd.concat(tm_fielding_dfs).reset_index(drop=True),
            hitting_adv=pd.concat(tm_hitting_adv_dfs),
            pitching_adv=pd.concat(tm_pitching_adv_dfs)
        ),
    )
    player_list: list[dict] = [player_dict for player_dict in players.values()]
    # players = dclass.PlayerDirectory(players)
    players = dclass.PlayerDirectory.from_json(player_list)
    
    rosters = dclass.TeamRosters(full,fortyman,active)
    
    return helpers.AppObjects.TeamPageContent(
        players=players,
        stats=stats,
        rosters=rosters,
        team=team_info,
        draft=draft,
        transactions=transactions,
        date=date_obj
    )
