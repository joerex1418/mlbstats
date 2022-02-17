import requests

import pandas as pd

from bs4 import BeautifulSoup as bs
from bs4.element import Comment
from bs4.element import SoupStrainer

from .mlbdata import get_teams_df
from .mlbdata import get_yby_records

from .constants import BASE
from .constants import STATDICT
from .constants import BAT_FIELDS
from .constants import BAT_FIELDS_ADV
from .constants import PITCH_FIELDS
from .constants import PITCH_FIELDS_ADV
from .constants import FIELD_FIELDS
from .constants import LEAGUE_IDS_SHORT
from .constants import BBREF_SPLITS
from .constants import STAT_GROUPS
from .constants import LEADER_CATEGORIES
from .constants import LEADERS_SEASON
from .constants import LEADERS_ALLTIME_SINGLE_SEASON
from .constants import LEADERS_ALLTIME_CAREER


def team_stats(season,sesh=None):
    # teams_df = get_teams_df(season)[["yearID","fullName","lgAbbrv","locationName","clubName","mlbam","bbrefID"]].set_index("mlbam")
    # url = BASE + f"/teams/stats?stats=season,seasonAdvanced&group=hitting,pitching,fielding&season={season}&leagueIds=103,104&hydrate=team(division)"

    # retrieving from ybyRecords table because it has division and league IDs
    yby_df = get_yby_records()
    yby_df = yby_df[yby_df["season"]==season]
    teams_df = get_teams_df(season)[["yearID","fullName","lgAbbrv","locationName","clubName","mlbam","bbrefID"]].set_index("mlbam")
    url = BASE + f"/teams/stats?stats=season,seasonAdvanced&group=hitting,pitching,fielding&season={season}&leagueIds=103,104"
    
    if sesh is None:
        response = requests.get(url).json()["stats"]
    else:
        response = sesh.get(url).json()["stats"]
    
    league_stats_dict = {
        "hitting":None,
        "hittingAdvanced":None,
        "pitching":None,
        "pitchingAdvanced":None,
        "fielding":None
    }
    for group_type in response:
        statGroup = group_type["group"]["displayName"]
        statType = group_type["type"]["displayName"]
        if statGroup == "hitting" and statType == "season":
            stat_rows = []
            for tm_obj in group_type["splits"]:
                team = tm_obj["team"]
                mlbam = team["id"]

                tm_series = yby_df[yby_df["tm_mlbam"]==mlbam]

                bbrefID = teams_df.loc[mlbam].bbrefID
                headers = ["mlbam","bbrefID","fullName","lg_mlbam","League","div_mlbam","Div"]

                try:
                    div_mlbam = tm_series.div_mlbam
                    div = tm_series.div_short
                except:
                    div_mlbam = '--'
                    div = '--'
                
                try:
                    lg_mlbam = tm_series.lg_mlbam
                    lg = tm_series.lg_abbrv
                except:
                    lg_mlbam = '--'
                    lg = '--'

                team_stats = [mlbam,bbrefID,team["name"],lg_mlbam,lg,div_mlbam,div]
                tm_stats = tm_obj["stat"]
                for stat_field in BAT_FIELDS:
                    headers.append(stat_field)
                    try:
                        team_stats.append(tm_stats[stat_field])
                    except:
                        team_stats.append("--")
                stat_rows.append(team_stats)
            league_stats_dict["hitting"] = pd.DataFrame(data=stat_rows,columns=headers).rename(columns=STATDICT)
        elif statGroup == "pitching" and statType == "season":
            stat_rows = []
            for tm_obj in group_type["splits"]:
                team = tm_obj["team"]
                mlbam = team["id"]
                bbrefID = teams_df.loc[mlbam].bbrefID
                headers = ["mlbam","bbrefID","fullName","lg_mlbam","League","div_mlbam","Div"]

                try:
                    division = team["division"]
                    div_mlbam = division["id"]
                    div = LEAGUE_IDS_SHORT[div_mlbam]
                except:
                    div_mlbam = '--'
                    div = '--'
                
                try:
                    lg_mlbam = team["league"]["id"]
                    lg = LEAGUE_IDS_SHORT[lg_mlbam]
                except:
                    lg_mlbam = '--'
                    lg = '--'

                team_stats = [mlbam,bbrefID,team["name"],lg_mlbam,lg,div_mlbam,div]
                tm_stats = tm_obj["stat"]
                for stat_field in PITCH_FIELDS:
                    headers.append(stat_field)
                    try:
                        team_stats.append(tm_stats[stat_field])
                    except:
                        team_stats.append("--")
                stat_rows.append(team_stats)
            league_stats_dict["pitching"] = pd.DataFrame(data=stat_rows,columns=headers).rename(columns=STATDICT).drop(columns=["IR","IRS","GS"])
        elif statGroup == "fielding" and statType == "season":
            stat_rows = []
            for tm_obj in group_type["splits"]:
                team = tm_obj["team"]
                mlbam = team["id"]
                bbrefID = teams_df.loc[mlbam].bbrefID
                headers = ["mlbam","bbrefID","fullName","lg_mlbam","League","div_mlbam","Div"]

                try:
                    division = team["division"]
                    div_mlbam = division["id"]
                    div = LEAGUE_IDS_SHORT[div_mlbam]
                except:
                    div_mlbam = '--'
                    div = '--'
                
                try:
                    lg_mlbam = team["league"]["id"]
                    lg = LEAGUE_IDS_SHORT[lg_mlbam]
                except:
                    lg_mlbam = '--'
                    lg = '--'

                team_stats = [mlbam,bbrefID,team["name"],lg_mlbam,lg,div_mlbam,div]
                tm_stats = tm_obj["stat"]
                for stat_field in FIELD_FIELDS:
                    headers.append(stat_field)
                    try:
                        team_stats.append(tm_stats[stat_field])
                    except:
                        team_stats.append("--")
                stat_rows.append(team_stats)
            league_stats_dict["fielding"] = pd.DataFrame(data=stat_rows,columns=headers).rename(columns=STATDICT).drop(columns=["GS"])
        elif statGroup == "hitting" and statType == "seasonAdvanced":
            stat_rows = []
            for tm_obj in group_type["splits"]:
                team = tm_obj["team"]
                mlbam = team["id"]
                bbrefID = teams_df.loc[mlbam].bbrefID
                headers = ["mlbam","bbrefID","fullName","lg_mlbam","League","div_mlbam","Div"]

                try:
                    division = team["division"]
                    div_mlbam = division["id"]
                    div = LEAGUE_IDS_SHORT[div_mlbam]
                except:
                    div_mlbam = '--'
                    div = '--'
                
                try:
                    lg_mlbam = team["league"]["id"]
                    lg = LEAGUE_IDS_SHORT[lg_mlbam]
                except:
                    lg_mlbam = '--'
                    lg = '--'

                team_stats = [mlbam,bbrefID,team["name"],lg_mlbam,lg,div_mlbam,div]
                tm_stats = tm_obj["stat"]
                for stat_field in BAT_FIELDS_ADV:
                    headers.append(stat_field)
                    try:
                        team_stats.append(tm_stats[stat_field])
                    except:
                        team_stats.append("--")
                stat_rows.append(team_stats)
            league_stats_dict["hittingAdvanced"] = pd.DataFrame(data=stat_rows,columns=headers).rename(columns=STATDICT)
        elif statGroup == "pitching" and statType == "seasonAdvanced":
            stat_rows = []
            for tm_obj in group_type["splits"]:
                team = tm_obj["team"]
                mlbam = team["id"]
                bbrefID = teams_df.loc[mlbam].bbrefID
                headers = ["mlbam","bbrefID","fullName","lg_mlbam","League","div_mlbam","Div"]
                try:
                    division = team["division"]
                    div_mlbam = division["id"]
                    div = LEAGUE_IDS_SHORT[div_mlbam]
                except:
                    div_mlbam = '--'
                    div = '--'
                
                try:
                    lg_mlbam = team["league"]["id"]
                    lg = LEAGUE_IDS_SHORT[lg_mlbam]
                except:
                    lg_mlbam = '--'
                    lg = '--'
                team_stats = [mlbam,bbrefID,team["name"],lg_mlbam,lg,div_mlbam,div]
                tm_stats = tm_obj["stat"]
                for stat_field in PITCH_FIELDS_ADV:
                    headers.append(stat_field)
                    try:
                        team_stats.append(tm_stats[stat_field])
                    except:
                        team_stats.append("--")
                stat_rows.append(team_stats)
            league_stats_dict["pitchingAdvanced"] = pd.DataFrame(data=stat_rows,columns=headers).rename(columns=STATDICT)
        else:pass

    return league_stats_dict

def player_stats(season,sesh=None):
    groups = "hitting,pitching,fielding"
    url = BASE + f"/stats?stats=season,seasonAdvanced&group={groups}&playerPool=all&limit=3000&leagueIds=103,104&season={season}"
    # Handles whether or not a requests session is used
    if sesh is None:
        response = requests.get(url).json()["stats"]
    else:
        response = sesh.get(url).json()["stats"]


    # Storing dataframes in this dict
    league_stats_dict = {
        "hitting":None,
        "hittingAdvanced":None,
        "pitching":None,
        "pitchingAdvanced":None,
        "fielding":None
                                }
    for group_type in response:
        statType = group_type['type']['displayName']
        statGroup = group_type['group']['displayName']

        if statType == 'season' and statGroup == 'hitting':
            stat_rows = []
            columns = ['Season','Player','mlbam','tm_mlbam','Lg','lg_mlbam','div_mlbam','Div'] #,'tm_bbrefID','tm_name']
            for stat in BAT_FIELDS:
                columns.append(STATDICT[stat])
            for entry in group_type['splits']:
                player = entry['player']
                team = entry['team']
                lg = entry['league']['name']
                lg_mlbam = entry['league']['id']
                try:
                    div_mlbam = team['division']['id']
                    div = LEAGUE_IDS_SHORT[div_mlbam]
                except:
                    div_mlbam = '--'
                    div = '--'
                stats = entry['stat']

                season = int(entry['season'])
                name = player['fullName']
                mlbam = player['id']
                tm_mlbam = team['id']

                row_data = [season,name,mlbam,tm_mlbam,lg,lg_mlbam,div_mlbam,div]  # tm_bbrefID,tm_name] # don't need tm_bbrefID or tm_name at this point
                for stat in BAT_FIELDS:
                    try:
                        row_data.append(stats[stat])
                    except:
                        row_data.append('--')
                stat_rows.append(row_data)
            league_stats_dict['hitting'] = pd.DataFrame(data=stat_rows,columns=columns)

        elif statType == 'season' and statGroup == 'pitching':
            stat_rows = []
            columns = ['Season','Player','mlbam','tm_mlbam','Lg','lg_mlbam','div_mlbam','Div']
            for stat in PITCH_FIELDS:
                columns.append(STATDICT[stat])
            for entry in group_type['splits']:
                player = entry['player']
                team = entry['team']
                lg = entry['league']['name']
                lg_mlbam = entry['league']['id']
                try:
                    div_mlbam = team['division']['id']
                    div = LEAGUE_IDS_SHORT[div_mlbam]
                except:
                    div_mlbam = '--'
                    div = '--'
                stats = entry['stat']

                season = int(entry['season'])
                name = player['fullName']
                mlbam = player['id']
                tm_mlbam = team['id']

                row_data = [season,name,mlbam,tm_mlbam,lg,lg_mlbam,div_mlbam,div]
                for stat in PITCH_FIELDS:
                    try:
                        row_data.append(stats[stat])
                    except:
                        row_data.append('--')
                stat_rows.append(row_data)
            league_stats_dict['pitching'] = pd.DataFrame(data=stat_rows,columns=columns)

        elif statType == 'season' and statGroup == 'fielding':
            stat_rows = []
            columns = ['Season','Player','mlbam','tm_mlbam','Lg','lg_mlbam','div_mlbam','Div']
            for stat in FIELD_FIELDS:
                columns.append(STATDICT[stat])
            for entry in group_type['splits']:
                player = entry['player']
                team = entry['team']
                lg = entry['league']['name']
                lg_mlbam = entry['league']['id']
                try:
                    div_mlbam = team['division']['id']
                    div = LEAGUE_IDS_SHORT[div_mlbam]
                except:
                    div_mlbam = '--'
                    div = '--'
                stats = entry['stat']

                season = int(entry['season'])
                name = player['fullName']
                mlbam = player['id']
                tm_mlbam = team['id']

                row_data = [season,name,mlbam,tm_mlbam,lg,lg_mlbam,div_mlbam,div]
                for stat in FIELD_FIELDS:
                    try:
                        row_data.append(stats[stat])
                    except:
                        row_data.append('--')
                stat_rows.append(row_data)
            league_stats_dict['fielding'] = pd.DataFrame(data=stat_rows,columns=columns)

        elif statType == 'seasonAdvanced' and statGroup == 'hitting':
            stat_rows = []
            columns = ['Season','Player','mlbam','tm_mlbam','Lg','lg_mlbam','div_mlbam','Div'] #,'tm_bbrefID','tm_name']
            for stat in BAT_FIELDS_ADV:
                columns.append(STATDICT[stat])
            for entry in group_type['splits']:
                player = entry['player']
                team = entry['team']
                lg = entry['league']['name']
                lg_mlbam = entry['league']['id']
                try:
                    div_mlbam = team['division']['id']
                    div = LEAGUE_IDS_SHORT[div_mlbam]
                except:
                    div_mlbam = '--'
                    div = '--'
                stats = entry['stat']

                season = int(entry['season'])
                name = player['fullName']
                mlbam = player['id']
                tm_mlbam = team['id']

                row_data = [season,name,mlbam,tm_mlbam,lg,lg_mlbam,div_mlbam,div]
                for stat in BAT_FIELDS_ADV:
                    try:
                        row_data.append(stats[stat])
                    except:
                        row_data.append('--')
                stat_rows.append(row_data)
            league_stats_dict['hittingAdvanced'] = pd.DataFrame(data=stat_rows,columns=columns)

        elif statType == 'seasonAdvanced' and statGroup == 'pitching':
            stat_rows = []
            columns = ['Season','Player','mlbam','tm_mlbam','Lg','lg_mlbam','div_mlbam','Div']
            for stat in PITCH_FIELDS_ADV:
                columns.append(STATDICT[stat])
            for entry in group_type['splits']:
                player = entry['player']
                team = entry['team']
                lg = entry['league']['name']
                lg_mlbam = entry['league']['id']
                try:
                    div_mlbam = team['division']['id']
                    div = LEAGUE_IDS_SHORT[div_mlbam]
                except:
                    div_mlbam = '--'
                    div = '--'
                stats = entry['stat']

                season = int(entry['season'])
                name = player['fullName']
                mlbam = player['id']
                tm_mlbam = team['id']

                row_data = [season,name,mlbam,tm_mlbam,lg,lg_mlbam,div_mlbam,div]
                for stat in PITCH_FIELDS_ADV:
                    try:
                        row_data.append(stats[stat])
                    except:
                        row_data.append('--')
                stat_rows.append(row_data)
            league_stats_dict['pitchingAdvanced'] = pd.DataFrame(data=stat_rows,columns=columns)


    return league_stats_dict

def standings(season,sesh=None):
    url = f"https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&sportId=1&season={season}"
    
    # Handles whether or not a requests session is used
    if sesh is None:
        records = requests.get(url).json()["records"]
    else:
        records = sesh.get(url).json()["records"]

    fields = ['gamesPlayed','wins','losses','winningPercentage','runsScored','runsAllowed','runDifferential','divisionRank','leagueRank','divisionGamesBack','leagueGamesBack']

    columns = ['season','Team','tm_mlbam','GP','W','L','W%','R','RA','RunDiff','divRank','lgRank','divGB','lgGB','Home','Away','vRHP','vLHP','vAL','vNL','vWest','vEast','vCentral','ExInns','oneRun','Day','Night','Grass','Turf']
    teamRecords = []
    for lgDiv in records:
        
        for team in lgDiv['teamRecords']:

            season = int(team['season'])
            tm_name = team['team']['name']
            tm_mlbam = team['team']['id']

            # Row entry creation
            row_data = [season,tm_name,tm_mlbam]

            for field in fields:
                row_data.append(team.get(field,''))

            recs = team['records']
            records_dict = {
                'home':'--',
                'away':'--',
                'vRHP':'--',
                'vLHP':'--',
                'vAL':'--',
                'vNL':'--',
                'vWest':'--',
                'vEast':'--',
                'vCentral':'--',
                'extraInning':'--',
                'oneRun':'--',
                'day':'--',
                'night':'--',
                'grass':'--',
                'turf':'--'
            }

            for rec in recs['splitRecords']:
                if rec['type'] == 'home':
                    records_dict['home'] = f"{rec['wins']}-{rec['losses']}"

                elif rec['type'] == 'away':
                    records_dict['away'] = f"{rec['wins']}-{rec['losses']}"

                elif rec['type'] == 'right':
                    records_dict['vRHP'] = f"{rec['wins']}-{rec['losses']}"

                elif rec['type'] == 'left':
                    records_dict['vLHP'] = f"{rec['wins']}-{rec['losses']}"

                elif rec['type'] == 'extraInning':
                    records_dict['extraInning'] = f"{rec['wins']}-{rec['losses']}"

                elif rec['type'] == 'oneRun':
                    records_dict['oneRun'] = f"{rec['wins']}-{rec['losses']}"

                elif rec['type'] == 'day':
                    records_dict['day'] = f"{rec['wins']}-{rec['losses']}"

                elif rec['type'] == 'night':
                    records_dict['night'] = f"{rec['wins']}-{rec['losses']}"

                elif rec['type'] == 'grass':
                    records_dict['grass'] = f"{rec['wins']}-{rec['losses']}"

                elif rec['type'] == 'turf':
                    records_dict['turf'] = f"{rec['wins']}-{rec['losses']}"
                else:pass

            try:
                for rec in recs['divisionRecords']:
                    if 'west' in rec['division']['name'].lower():
                        records_dict['vWest'] = f"{rec['wins']}-{rec['losses']}"

                    elif 'east' in rec['division']['name'].lower():
                        records_dict['vEast'] = f"{rec['wins']}-{rec['losses']}"

                    elif 'central' in rec['division']['name'].lower():
                        records_dict['vCentral'] = f"{rec['wins']}-{rec['losses']}"
            except:
                records_dict['vWest'] = "--"
                records_dict['vEast'] = "--"
                records_dict['vCentral'] = "--"

            for rec in recs['leagueRecords']:
                if rec['league']['id'] == 103:
                    records_dict['vAL'] = f"{rec['wins']}-{rec['losses']}"

                elif rec['league']['id'] == 104:
                    records_dict['vNL'] = f"{rec['wins']}-{rec['losses']}"

            for value in records_dict.values():
                row_data.append(value)
            
            teamRecords.append(row_data)
    df = pd.DataFrame(data=teamRecords,columns=columns).sort_values(by='W%',ascending=False).reset_index(drop=True)
    return df

def stats_and_standings(season,sesh=None):
    teams_df = get_teams_df(season)[["yearID","fullName","lgAbbrv","locationName","clubName","mlbam","bbrefID"]].set_index("mlbam")
    hydrate = "team(division,standings)"
    
    url = BASE + f"/teams/stats?stats=season,seasonAdvanced&group=hitting,pitching,fielding&season={season}&leagueIds=100,101,102,103,104,105,106&hydrate=team(division,standings)"
    
    if sesh is None:
        response = requests.get(url).json()["stats"]
    else:
        response = sesh.get(url).json()["stats"]
    
    team_records = []

    league_stats_dict = {
        "hitting":None,
        "hittingAdvanced":None,
        "pitching":None,
        "pitchingAdvanced":None,
        "fielding":None
    }
    for group_type in response:
        statGroup = group_type["group"]["displayName"]
        statType = group_type["type"]["displayName"]
        if statGroup == "hitting" and statType == "season":
            stat_rows = []
            for tm_obj in group_type["splits"]:
                team = tm_obj["team"]
                mlbam = team["id"]
                bbrefID = teams_df.loc[mlbam].bbrefID
                headers = ["mlbam","bbrefID","fullName","lg_mlbam","League","div_mlbam","Div"]

                try:
                    division = team["division"]
                    div_mlbam = division["id"]
                    div = LEAGUE_IDS_SHORT[div_mlbam]
                except:
                    div_mlbam = '--'
                    div = '--'
                
                try:
                    lg_mlbam = team["league"]["id"]
                    lg = LEAGUE_IDS_SHORT[lg_mlbam]
                except:
                    lg_mlbam = '--'
                    lg = '--'

                team_stats = [mlbam,bbrefID,team["name"],lg_mlbam,lg,div_mlbam,div]
                tm_stats = tm_obj["stat"]
                for stat_field in BAT_FIELDS:
                    headers.append(stat_field)
                    try:
                        team_stats.append(tm_stats[stat_field])
                    except:
                        team_stats.append("--")
                stat_rows.append(team_stats)
            league_stats_dict["hitting"] = pd.DataFrame(data=stat_rows,columns=headers).rename(columns=STATDICT)
        elif statGroup == "pitching" and statType == "season":
            stat_rows = []
            for tm_obj in group_type["splits"]:
                team = tm_obj["team"]
                mlbam = team["id"]
                bbrefID = teams_df.loc[mlbam].bbrefID
                headers = ["mlbam","bbrefID","fullName","lg_mlbam","League","div_mlbam","Div"]

                try:
                    division = team["division"]
                    div_mlbam = division["id"]
                    div = LEAGUE_IDS_SHORT[div_mlbam]
                except:
                    div_mlbam = '--'
                    div = '--'
                
                try:
                    lg_mlbam = team["league"]["id"]
                    lg = LEAGUE_IDS_SHORT[lg_mlbam]
                except:
                    lg_mlbam = '--'
                    lg = '--'

                team_stats = [mlbam,bbrefID,team["name"],lg_mlbam,lg,div_mlbam,div]
                tm_stats = tm_obj["stat"]
                for stat_field in PITCH_FIELDS:
                    headers.append(stat_field)
                    try:
                        team_stats.append(tm_stats[stat_field])
                    except:
                        team_stats.append("--")
                stat_rows.append(team_stats)
            league_stats_dict["pitching"] = pd.DataFrame(data=stat_rows,columns=headers).rename(columns=STATDICT).drop(columns=["IR","IRS","GS"])
        elif statGroup == "fielding" and statType == "season":
            stat_rows = []
            for tm_obj in group_type["splits"]:
                team = tm_obj["team"]
                mlbam = team["id"]
                bbrefID = teams_df.loc[mlbam].bbrefID
                headers = ["mlbam","bbrefID","fullName","lg_mlbam","League","div_mlbam","Div"]

                try:
                    division = team["division"]
                    div_mlbam = division["id"]
                    div = LEAGUE_IDS_SHORT[div_mlbam]
                except:
                    div_mlbam = '--'
                    div = '--'
                
                try:
                    lg_mlbam = team["league"]["id"]
                    lg = LEAGUE_IDS_SHORT[lg_mlbam]
                except:
                    lg_mlbam = '--'
                    lg = '--'

                team_stats = [mlbam,bbrefID,team["name"],lg_mlbam,lg,div_mlbam,div]
                tm_stats = tm_obj["stat"]
                for stat_field in FIELD_FIELDS:
                    headers.append(stat_field)
                    try:
                        team_stats.append(tm_stats[stat_field])
                    except:
                        team_stats.append("--")
                stat_rows.append(team_stats)
            league_stats_dict["fielding"] = pd.DataFrame(data=stat_rows,columns=headers).rename(columns=STATDICT).drop(columns=["GS"])
        elif statGroup == "hitting" and statType == "seasonAdvanced":
            stat_rows = []
            for tm_obj in group_type["splits"]:
                team = tm_obj["team"]
                mlbam = team["id"]
                bbrefID = teams_df.loc[mlbam].bbrefID
                headers = ["mlbam","bbrefID","fullName","lg_mlbam","League","div_mlbam","Div"]

                try:
                    division = team["division"]
                    div_mlbam = division["id"]
                    div = LEAGUE_IDS_SHORT[div_mlbam]
                except:
                    div_mlbam = '--'
                    div = '--'
                
                try:
                    lg_mlbam = team["league"]["id"]
                    lg = LEAGUE_IDS_SHORT[lg_mlbam]
                except:
                    lg_mlbam = '--'
                    lg = '--'

                team_stats = [mlbam,bbrefID,team["name"],lg_mlbam,lg,div_mlbam,div]
                tm_stats = tm_obj["stat"]
                for stat_field in BAT_FIELDS_ADV:
                    headers.append(stat_field)
                    try:
                        team_stats.append(tm_stats[stat_field])
                    except:
                        team_stats.append("--")
                stat_rows.append(team_stats)
            league_stats_dict["hittingAdvanced"] = pd.DataFrame(data=stat_rows,columns=headers).rename(columns=STATDICT)
        elif statGroup == "pitching" and statType == "seasonAdvanced":
            stat_rows = []
            for tm_obj in group_type["splits"]:
                team = tm_obj["team"]
                mlbam = team["id"]
                bbrefID = teams_df.loc[mlbam].bbrefID
                headers = ["mlbam","bbrefID","fullName","lg_mlbam","League","div_mlbam","Div"]
                try:
                    division = team["division"]
                    div_mlbam = division["id"]
                    div = LEAGUE_IDS_SHORT[div_mlbam]
                except:
                    div_mlbam = '--'
                    div = '--'
                
                try:
                    lg_mlbam = team["league"]["id"]
                    lg = LEAGUE_IDS_SHORT[lg_mlbam]
                except:
                    lg_mlbam = '--'
                    lg = '--'
                team_stats = [mlbam,bbrefID,team["name"],lg_mlbam,lg,div_mlbam,div]
                tm_stats = tm_obj["stat"]
                for stat_field in PITCH_FIELDS_ADV:
                    headers.append(stat_field)
                    try:
                        team_stats.append(tm_stats[stat_field])
                    except:
                        team_stats.append("--")
                stat_rows.append(team_stats)
            league_stats_dict["pitchingAdvanced"] = pd.DataFrame(data=stat_rows,columns=headers).rename(columns=STATDICT)
        else:pass

    return league_stats_dict

def bbrefSplits(bbrefID,season,s_type="b",sesh=None):
    url = f"https://www.baseball-reference.com/leagues/split.cgi?t={s_type}&lg={bbrefID}&year={season}"

    if sesh is None:
        req = requests.get(url)
    else:
        req = sesh.get(url)

    strainer = SoupStrainer("div")
    soup = bs(req.content,"lxml",parse_only=strainer)
    split_tables = {}
    comments = soup.findAll(text=lambda text:isinstance(text, Comment))
    for c in comments:
        for split_type in BBREF_SPLITS.keys():
            if "div_"+split_type in c:
                split_tables[BBREF_SPLITS[split_type]] = pd.read_html(c.extract())[0].drop(columns=["tOPS+","sOPS+"],errors='ignore')

    split_tables['platoon'] = split_tables['platoon'].fillna('--')
    return split_tables

def leaders_players(statType,categories=None,STAT_GROUPS=STAT_GROUPS,gameTypes="R",playerPool="qualified",season="2021"):

    if categories is None:
        categories = LEADER_CATEGORIES
    
    if statType == "season":
        url = BASE + LEADERS_SEASON.format(statCategories=categories,gameTypes=gameTypes,STAT_GROUPS=STAT_GROUPS,playerPool=playerPool,season=season)
    elif statType == "career":
        url = BASE + LEADERS_ALLTIME_SINGLE_SEASON.format(statCategories=categories,gameTypes=gameTypes,STAT_GROUPS=STAT_GROUPS,playerPool=playerPool)
    elif statType == "alltime":
        url = BASE + LEADERS_ALLTIME_CAREER.format(statCategories=categories,gameTypes=gameTypes,STAT_GROUPS=STAT_GROUPS,playerPool=playerPool)

    response = requests.get(url=url)
    print(response.url)
    return None
    leader_dict = {
        "hitting":{},
        "pitching":{},
        "fielding":{},
        "catching":{}
    }

    columns = ("rank","name","mlbam","value","team","tm_mlbam","lg_mlbam")

    for cat_group in response.json()["leagueLeaders"]:
        leaderCat = cat_group.get("leaderCategory")
        statGroup = cat_group.get("statGroup")
        if "leaders" in cat_group.keys():
            all_entries = []
            for entry in cat_group.get("leaders"):
                rank = entry.get("rank")
                p_name = entry.get("person").get("fullName")
                p_mlbam = entry.get("person").get("id")
                value = entry.get("value")
                tm_name = entry.get("team").get("name")
                tm_mlbam = entry.get("team").get("id")
                lg_mlbam = entry.get("league").get("id","")
                season = entry.get("season")

                entry_data = [rank,p_name,p_mlbam,value,tm_name,tm_mlbam,lg_mlbam]

                all_entries.append(entry_data)

            df = pd.DataFrame(data=all_entries,columns=columns)

            leader_dict[statGroup][leaderCat] = df
        else:
            pass
       
    
    return leader_dict

def leaders_teams(mlbam,season):
    pass

def hof_recipients(season):
    url = BASE + "/awards/MLBHOF/recipients?"