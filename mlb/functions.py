import lxml
import pytz
# import numpy as np
import requests
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup as bs
from types import SimpleNamespace as ns

from .async_mlb import get_leaders
from .async_mlb import get_responses

# from .mlbdata import get_people_df
from .mlbdata import get_season_info
from .mlbdata import get_venues_df
from .mlbdata import get_bios_df
from .mlbdata import get_teams_df
from .mlbdata import get_yby_records
from .mlbdata import get_standings_df

from .utils import curr_year
from .utils import curr_date
from .utils import default_season

# from .utils import utc_zone
from .utils import et_zone
from .utils import ct_zone
from .utils import mt_zone
from .utils import pt_zone

from .constants import BASE
from .constants import BAT_FIELDS
from .constants import BAT_FIELDS_ADV
from .constants import PITCH_FIELDS
from .constants import PITCH_FIELDS_ADV
from .constants import FIELD_FIELDS
from .constants import STATDICT
from .constants import LEAGUE_IDS
from .constants import sitCodes

# ===============================================================
# Data Objects
# ===============================================================


class team:
    def __new__(cls,mlbam,**data):
        self = object.__new__(cls)
        data.update(rost_hitting='rost_hitting_df')
        self.__dict__.update(data)

        self.stats = ns(
            **{"hitting":ns(
                **{
                    "roster":f"hitting roster stats for {mlbam}",
                    "total":f"hitting stats total for {mlbam}"}),
               "pitching":ns(
                **{
                    "roster":f"pitching roster stats for {mlbam}",
                    "total":f"pitching stats total for {mlbam}"})}
        )
        return self


# ===============================================================
# PLAYER Functions
# ===============================================================
def player_hitting(mlbam,*args,**kwargs) -> pd.DataFrame:
    """Get player career & year-by-year hitting stats

    Parameters
    ----------
    mlbam : str or int
        player's official MLB ID

    gameType : str, optional
        filter results by game type

    """

    if len(args) == 1:
        if type(args[0]) is list:
            gameType = ",".join(args[0])
        else:
            gameType = args[0]

        gameType = gameType.replace(" ","")
    else:
        gameType = None

    gameType = kwargs.get("gameType")

    stats = "career,yearByYear"
    url = BASE + f"/people/{mlbam}/stats?stats={stats}&group=hitting"

    if gameType is not None:
        url = url + f"&gameType={gameType}"

    resp = requests.get(url)

    season_col = []
    team_mlbam_col = []
    team_name_col = []
    league_mlbam_col = []
    league_col = []
    game_type_col = []


    stat_data = []

    for stat_type in resp.json()['stats']:
        if stat_type["type"]["displayName"] == "career":
            for s in stat_type["splits"]:
                career_stats = pd.Series(s["stat"])

                season_col.append("Career")
                team_mlbam_col.append("-")
                team_name_col.append("-")
                league_mlbam_col.append("-")
                league_col.append("-")
                game_type_col.append(s["gameType"])
                
                stat_data.append(career_stats)

        elif stat_type["type"]["displayName"] == "yearByYear":
            for s in stat_type["splits"]:
                tm_id = s.get("team",{}).get("id")
                tm_name = s.get("team",{}).get("name")
                lg_id = s.get("league",{}).get("id")

                season_stats = pd.Series(s["stat"])

                season_col.append(s["season"])
                team_mlbam_col.append(tm_id)
                team_name_col.append(tm_name)
                league_mlbam_col.append(lg_id)
                league_col.append(LEAGUE_IDS.get(str(lg_id)))
                game_type_col.append(s["gameType"])

                stat_data.append(season_stats)

        elif stat_type["type"]["displayName"] == "season":
            for s in stat_type["splits"]:
                tm_id = s.get("team",{}).get("id")
                tm_name = s.get("team",{}).get("name")
                lg_id = s.get("league",{}).get("id")

                season_stats = pd.Series(s["stat"])

                season_col.append(s["season"])
                team_mlbam_col.append(tm_id)
                team_name_col.append(tm_name)
                league_mlbam_col.append(lg_id)
                league_col.append(LEAGUE_IDS.get(str(lg_id)))
                game_type_col.append(s["gameType"])

                stat_data.append(season_stats)
    
    df = pd.DataFrame(data=stat_data).rename(columns=STATDICT)

    df.insert(0,"Season",season_col)
    df.insert(1,"team_mlbam",team_mlbam_col)
    df.insert(2,"team_name",team_name_col)
    df.insert(3,"league_mlbam",league_mlbam_col)
    df.insert(4,"league_name",league_col)
    df.insert(5,"game_type",game_type_col)

    return df

def player_pitching(mlbam,*args,**kwargs) -> pd.DataFrame:
    """Get pitching career & year-by-year stats for a player

    Parameters
    ----------
    mlbam : str or int
        player's official MLB ID

    gameType : str, optional
        filter results by game type

    """

    if len(args) == 1:
        if type(args[0]) is list:
            gameType = ",".join(args[0])
        else:
            gameType = args[0]

        gameType = gameType.replace(" ","")
        print(gameType)
    else:
        gameType = None

    stats = "career,yearByYear"
    url = BASE + f"/people/{mlbam}/stats?stats={stats}&group=pitching"

    if gameType is not None:
        url = url + f"&gameType={gameType}"

    resp = requests.get(url)

    season_col = []
    team_mlbam_col = []
    team_name_col = []
    league_mlbam_col = []
    league_col = []
    game_type_col = []


    stat_data = []

    for stat_type in resp.json()['stats']:
        if stat_type["type"]["displayName"] == "career":
            for s in stat_type["splits"]:
                career_stats = pd.Series(s["stat"])

                season_col.append("Career")
                team_mlbam_col.append("-")
                team_name_col.append("-")
                league_mlbam_col.append("-")
                league_col.append("-")
                game_type_col.append(s["gameType"])
                
                stat_data.append(career_stats)

        elif stat_type["type"]["displayName"] == "yearByYear":
            for s in stat_type["splits"]:
                tm_id = s.get("team",{}).get("id")
                tm_name = s.get("team",{}).get("name")
                lg_id = s.get("league",{}).get("id")

                season_stats = pd.Series(s["stat"])

                season_col.append(s["season"])
                team_mlbam_col.append(tm_id)
                team_name_col.append(tm_name)
                league_mlbam_col.append(lg_id)
                league_col.append(LEAGUE_IDS.get(str(lg_id)))
                game_type_col.append(s["gameType"])

                stat_data.append(season_stats)

        elif stat_type["type"]["displayName"] == "season":
            for s in stat_type["splits"]:
                tm_id = s.get("team",{}).get("id")
                tm_name = s.get("team",{}).get("name")
                lg_id = s.get("league",{}).get("id")

                season_stats = pd.Series(s["stat"])

                season_col.append(s["season"])
                team_mlbam_col.append(tm_id)
                team_name_col.append(tm_name)
                league_mlbam_col.append(lg_id)
                league_col.append(LEAGUE_IDS.get(str(lg_id)))
                game_type_col.append(s["gameType"])

                stat_data.append(season_stats)
    
    df = pd.DataFrame(data=stat_data).rename(columns=STATDICT)

    df.insert(0,"Season",season_col)
    df.insert(1,"team_mlbam",team_mlbam_col)
    df.insert(2,"team_name",team_name_col)
    df.insert(3,"league_mlbam",league_mlbam_col)
    df.insert(4,"league_name",league_col)
    df.insert(5,"game_type",game_type_col)

    return df

def player_fielding(mlbam,*args,**kwargs) -> pd.DataFrame:
    """Get player career & year-by-year fielding stats

    Parameters
    ----------
    mlbam : str or int
        player's official MLB ID

    gameType : str, optional
        filter results by game type

    """

    if len(args) == 1:
        if type(args[0]) is list:
            gameType = ",".join(args[0])
        else:
            gameType = args[0]

        gameType = gameType.replace(" ","")
        print(gameType)
    else:
        gameType = None

    stats = "career,yearByYear"
    url = BASE + f"/people/{mlbam}/stats?stats={stats}&group=fielding"

    if gameType is not None:
        url = url + f"&gameType={gameType}"

    resp = requests.get(url)

    season_col = []
    team_mlbam_col = []
    team_name_col = []
    league_mlbam_col = []
    league_col = []
    game_type_col = []
    pos_col = []

    stat_data = []

    for stat_type in resp.json()['stats']:
        if stat_type["type"]["displayName"] == "career":
            for s in stat_type["splits"]:
                pos_col.append(s["stat"]["position"]["abbreviation"])
                career_stats = pd.Series(s["stat"])

                season_col.append("Career")
                team_mlbam_col.append("-")
                team_name_col.append("-")
                league_mlbam_col.append("-")
                league_col.append("-")
                game_type_col.append(s["gameType"])
                
                stat_data.append(career_stats)

        elif stat_type["type"]["displayName"] == "yearByYear":
            for s in stat_type["splits"]:
                pos_col.append(s["stat"]["position"]["abbreviation"])
                tm_id = s.get("team",{}).get("id")
                tm_name = s.get("team",{}).get("name")
                lg_id = s.get("league",{}).get("id")

                season_stats = pd.Series(s["stat"])

                season_col.append(s["season"])
                team_mlbam_col.append(tm_id)
                team_name_col.append(tm_name)
                league_mlbam_col.append(lg_id)
                league_col.append(LEAGUE_IDS.get(str(lg_id)))
                game_type_col.append(s["gameType"])

                stat_data.append(season_stats)

        elif stat_type["type"]["displayName"] == "season":
            for s in stat_type["splits"]:
                pos_col.append(s["stat"]["position"]["abbreviation"])
                tm_id = s.get("team",{}).get("id")
                tm_name = s.get("team",{}).get("name")
                lg_id = s.get("league",{}).get("id")

                season_stats = pd.Series(s["stat"])

                season_col.append(s["season"])
                team_mlbam_col.append(tm_id)
                team_name_col.append(tm_name)
                league_mlbam_col.append(lg_id)
                league_col.append(LEAGUE_IDS.get(str(lg_id)))
                game_type_col.append(s["gameType"])

                stat_data.append(season_stats)
    
    df = pd.DataFrame(data=stat_data).rename(columns=STATDICT).drop(columns="position")

    df.insert(0,"Season",season_col)
    df.insert(1,"team_mlbam",team_mlbam_col)
    df.insert(2,"team_name",team_name_col)
    df.insert(3,"league_mlbam",league_mlbam_col)
    df.insert(4,"league_name",league_col)
    df.insert(5,"Pos",pos_col)
    df.insert(5,"game_type",game_type_col)

    return df

def player_hitting_advanced(mlbam,*args,**kwargs) -> pd.DataFrame:
    """Get advanced hitting stats for a player

    Parameters
    ----------
    mlbam : str or int
        player's official MLB ID

    gameType : str, optional
        filter results by game type
    """
    if len(args) == 1:
        if type(args[0]) is list:
            gameType = ",".join(args[0])
        else:
            gameType = args[0]

        gameType = gameType.replace(" ","")
        print(gameType)
    else:
        gameType = None

    stats = "careerAdvanced,yearByYearAdvanced"
    url = BASE + f"/people/{mlbam}/stats?stats={stats}&group=hitting"

    if gameType is not None:
        url = url + f"&gameType={gameType}"

    resp = requests.get(url)

    season_col = []
    team_mlbam_col = []
    team_name_col = []
    league_mlbam_col = []
    league_col = []
    game_type_col = []


    stat_data = []

    for stat_type in resp.json()['stats']:
        if stat_type["type"]["displayName"] == "careerAdvanced":
            for s in stat_type["splits"]:
                career_stats = pd.Series(s["stat"])

                season_col.append("Career")
                team_mlbam_col.append("-")
                team_name_col.append("-")
                league_mlbam_col.append("-")
                league_col.append("-")
                game_type_col.append(s["gameType"])
                
                stat_data.append(career_stats)

        elif stat_type["type"]["displayName"] == "yearByYearAdvanced":
            for s in stat_type["splits"]:
                tm_id = s.get("team",{}).get("id")
                tm_name = s.get("team",{}).get("name")
                lg_id = s.get("league",{}).get("id")

                season_stats = pd.Series(s["stat"])

                season_col.append(s["season"])
                team_mlbam_col.append(tm_id)
                team_name_col.append(tm_name)
                league_mlbam_col.append(lg_id)
                league_col.append(LEAGUE_IDS.get(str(lg_id)))
                game_type_col.append(s["gameType"])

                stat_data.append(season_stats)

        elif stat_type["type"]["displayName"] == "seasonAdvanced":
            for s in stat_type["splits"]:
                tm_id = s.get("team",{}).get("id")
                tm_name = s.get("team",{}).get("name")
                lg_id = s.get("league",{}).get("id")

                season_stats = pd.Series(s["stat"])

                season_col.append(s["season"])
                team_mlbam_col.append(tm_id)
                team_name_col.append(tm_name)
                league_mlbam_col.append(lg_id)
                league_col.append(LEAGUE_IDS.get(str(lg_id)))
                game_type_col.append(s["gameType"])

                stat_data.append(season_stats)
    
    df = pd.DataFrame(data=stat_data).rename(columns=STATDICT)

    df.insert(0,"Season",season_col)
    df.insert(1,"team_mlbam",team_mlbam_col)
    df.insert(2,"team_name",team_name_col)
    df.insert(3,"league_mlbam",league_mlbam_col)
    df.insert(4,"league_name",league_col)
    df.insert(5,"game_type",game_type_col)

    return df

def player_pitching_advanced(mlbam,*args,**kwargs) -> pd.DataFrame:
    """Get advanced player pitching stats

    Parameters
    ----------
    mlbam : str or int
        player's official MLB ID

    gameType : str, optional
        filter results by game type

    """

    if len(args) == 1:
        if type(args[0]) is list:
            gameType = ",".join(args[0])
        else:
            gameType = args[0]

        gameType = gameType.replace(" ","")
        print(gameType)
    else:
        gameType = None

    stats = "careerAdvanced,yearByYearAdvanced"
    url = BASE + f"/people/{mlbam}/stats?stats={stats}&group=pitching"

    if gameType is not None:
        url = url + f"&gameType={gameType}"

    resp = requests.get(url)

    season_col = []
    team_mlbam_col = []
    team_name_col = []
    league_mlbam_col = []
    league_col = []
    game_type_col = []


    stat_data = []

    for stat_type in resp.json()['stats']:
        if stat_type["type"]["displayName"] == "careerAdvanced":
            for s in stat_type["splits"]:
                career_stats = pd.Series(s["stat"])

                season_col.append("Career")
                team_mlbam_col.append("-")
                team_name_col.append("-")
                league_mlbam_col.append("-")
                league_col.append("-")
                game_type_col.append(s["gameType"])
                
                stat_data.append(career_stats)

        elif stat_type["type"]["displayName"] == "yearByYearAdvanced":
            for s in stat_type["splits"]:
                tm_id = s.get("team",{}).get("id")
                tm_name = s.get("team",{}).get("name")
                lg_id = s.get("league",{}).get("id")

                season_stats = pd.Series(s["stat"])

                season_col.append(s["season"])
                team_mlbam_col.append(tm_id)
                team_name_col.append(tm_name)
                league_mlbam_col.append(lg_id)
                league_col.append(LEAGUE_IDS.get(str(lg_id)))
                game_type_col.append(s["gameType"])

                stat_data.append(season_stats)

        elif stat_type["type"]["displayName"] == "seasonAdvanced":
            for s in stat_type["splits"]:
                tm_id = s.get("team",{}).get("id")
                tm_name = s.get("team",{}).get("name")
                lg_id = s.get("league",{}).get("id")

                season_stats = pd.Series(s["stat"])

                season_col.append(s["season"])
                team_mlbam_col.append(tm_id)
                team_name_col.append(tm_name)
                league_mlbam_col.append(lg_id)
                league_col.append(LEAGUE_IDS.get(str(lg_id)))
                game_type_col.append(s["gameType"])

                stat_data.append(season_stats)
    
    df = pd.DataFrame(data=stat_data).rename(columns=STATDICT)

    df.insert(0,"Season",season_col)
    df.insert(1,"team_mlbam",team_mlbam_col)
    df.insert(2,"team_name",team_name_col)
    df.insert(3,"league_mlbam",league_mlbam_col)
    df.insert(4,"league_name",league_col)
    df.insert(5,"game_type",game_type_col)

    return df

def player_game_logs(mlbam,season=None,statGroup=None,gameType=None,**kwargs) -> pd.DataFrame:
    """Get a player's game log stats for a specific season

    Parameters
    ----------
    mlbam : str or int
        player's official MLB ID
    
    seasons : str or int, optional (Default is the most recent season)
        filter by season

    statGroup : str, optional
        filter results by stat group ('hitting','pitching','fielding')

    gameType : str, optional
        filter results by game type

    startDate : str, optional
        include games AFTER a specified date
    
    endDate : str, optional
        include games BEFORE a specified date

    """

    params = {
        "stats":"gameLog"
    }
    if kwargs.get("seasons") is not None:
        season = kwargs["seasons"]

    if kwargs.get("group") is not None:
        statGroup = kwargs["group"]
    elif kwargs.get("groups") is not None:
        statGroup = kwargs["groups"]
    elif kwargs.get("statGroups") is not None:
        statGroup = kwargs["statGroups"]
    else:
        statGroup = statGroup

    if kwargs.get("gameTypes") is not None:
        gameType = kwargs["gameTypes"]
    elif kwargs.get("game_type") is not None:
        gameType = kwargs["game_type"]

    if season is not None:
        params["season"] = season
    if statGroup is not None:
        params["group"] = statGroup
    if gameType is not None:
        params["gameType"] = gameType
    if kwargs.get("startDate") is not None:
        params["startdate"] = kwargs["startDate"]
    if kwargs.get("endDate") is not None:
        params["endDate"] = kwargs["endDate"]

    url = BASE + f"/people/{mlbam}/stats?"
    resp = requests.get(url,params=params)

    data = []

    # "sg" referes to each stat group in the results
    for sg in resp.json()["stats"]:
        if sg["group"]["displayName"] == "hitting":
            # "g" refers to each game
            for g in sg.get("splits"):
                game = g.get("stat",{})

                positions = []
                player = g.get("player",{})
                mlbam = player.get("id")
                name = player.get("name")
                for pos in g.get("positionsPlayed",[]):
                    positions.append(pos.get("abbreviation"))
                positions = "|".join(positions)
                
                tm = g.get("team",{})
                tm_opp_mlbam = g.get("opponent",{}).get("id")
                tm_opp_name = g.get("opponent",{}).get("name")
                
                game["date"] = g.get("date","")
                game["isHome"] = g.get("isHome",False)
                game["isWin"] = g.get("isWin",False)
                game["gamePk"] = g.get("game",{}).get("gamePk")
                game["mlbam"] = mlbam
                game["name"] = name
                game["positions"] = positions
                game["tm_mlbam"] = tm.get("id")
                game["name"] = tm.get("name")
                game["opp_mlbam"] = tm_opp_mlbam
                game["opp_name"] = tm_opp_name

                data.append(pd.Series(game))

            break
    

    columns = ['date','isHome','isWin','gamePk','mlbam','name','positions','tm_mlbam','opp_mlbam','opp_name','G','GO','AO','R','2B','3B','HR','SO','BB','IBB','H','HBP','AVG','AB','OBP','SLG','OPS','CS','SB','SB%','GIDP','GITP','P','PA','TB','RBI','LOB','sB','sF','BABIP','GO/AO','CI','AB/HR']
    df = pd.DataFrame(data=data).rename(columns=STATDICT)[columns]

    return df

def player_date_range(mlbam,statGroup,startDate,endDate,gameType=None) -> pd.DataFrame:
    """Get a player's stats for a specified date range

    Parameters
    ----------
    mlbam : str or int, required
        player's official MLB ID
    
    startDate : str, required
        include games AFTER a specified date (format: "YYYY-mm-dd")
    
    endDate : str, required
        include games BEFORE a specified date (format: "YYYY-mm-dd")

    statGroup : str, required
        filter results by stat group ('hitting','pitching','fielding')

    gameType : str, optional
        filter results by game type (only one gameType can be specified per call)

    """

    mlbam = str(mlbam)

    params = {
        "stats":"byDateRange",
        "group":statGroup,
        "startDate":startDate,
        "endDate":endDate
    }

    if gameType is not None:
        params["gameType"] = gameType

    url = BASE + f"/people/{mlbam}/stats?"

    resp = requests.get(url,params=params)
    resp_json = resp.json()

    data = []

    for s in resp_json["stats"][0]["splits"]:
        if s.get("sport",{}).get("id") == 1:
            stats = s.get("stat")
            stats["tm_mlbam"] = s.get("team",{}).get("id")
            stats["tm_name"] = s.get("team",{}).get("name")
            data.append(pd.Series(stats))
    
    columns = ['tm_mlbam','Team','G','GO','AO','R','2B','3B','HR','SO','BB','IBB','H','HBP','AVG','AB','OBP','SLG','OPS','CS','SB','SB%','GIDP','GITP','P','PA','TB','RBI','LOB','sB','sF','BABIP','GO/AO','CI','AB/HR']
    df = pd.DataFrame(data).rename(columns=STATDICT)[columns]

    return df

def player_date_range_advanced(mlbam,statGroup,startDate,endDate,gameType=None) -> pd.DataFrame:
    """Get a player's stats for a specified date range

    Parameters
    ----------
    mlbam : str or int, required
        player's official MLB ID
    
    startDate : str, required
        include games AFTER a specified date (format: "YYYY-mm-dd")
    
    endDate : str, required
        include games BEFORE a specified date (format: "YYYY-mm-dd")

    statGroup : str, required
        filter results by stat group ('hitting','pitching','fielding')

    gameType : str, optional
        filter results by game type (only one gameType can be specified per call)

    """

    mlbam = str(mlbam)

    params = {
        "stats":"byDateRangeAdvanced",
        "group":statGroup,
        "startDate":startDate,
        "endDate":endDate
    }

    if gameType is not None:
        params["gameType"] = gameType

    url = BASE + f"/people/{mlbam}/stats?"

    resp = requests.get(url,params=params)
    resp_json = resp.json()

    data = []

    for s in resp_json["stats"][0]["splits"]:
        if s.get("sport",{}).get("id") == 1:
            stats = s.get("stat")
            stats["tm_mlbam"] = s.get("team",{}).get("id")
            stats["tm_name"] = s.get("team",{}).get("name")
            data.append(pd.Series(stats))
    
    columns = ['tm_mlbam','Team','PA','TB','sB','sF','BABIP','exBH','HBP','GIDP','P','P/PA','BB/PA','SO/PA','HR/PA','BB/SO','ISO','GO']
    df = pd.DataFrame(data).rename(columns=STATDICT)[columns]

    return df

def player_splits(mlbam,statGroup,sitCodes,season=None,gameType=None) -> pd.DataFrame:
    """Get a player's stats for a specified date range

    Parameters
    ----------
    mlbam : str or int, required
        player's official MLB ID

    statGroup : str, required
        filter results by stat group ('hitting','pitching','fielding')
    
    sitCodes : str, required
        situation code(s) to get stats for ("h" for home games, "a" for away games, "n" for night games, etc.)

    gameType : str, optional
        filter results by game type (only one gameType can be specified per call)

    """

    mlbam = str(mlbam)

    params = {
        "stats":"statSplits",
        "group":statGroup,
        "sitCodes":sitCodes,
    }

    if gameType is not None:
        params["gameType"] = gameType
    if season is not None:
        params["season"] = season
    else:
        params["season"] = default_season()

    url = BASE + f"/people/{mlbam}/stats?"

    resp = requests.get(url,params=params)
    resp_json = resp.json()

    data = []

    for s in resp_json["stats"][0]["splits"]:
        if s.get("sport",{}).get("id") == 1:
            stats = s.get("stat")
            stats["tm_mlbam"] = s.get("team",{}).get("id")
            stats["tm_name"] = s.get("team",{}).get("name")
            stats["season"] = s.get("season")
            stats["split_code"] = s.get("split",{}).get("code")
            stats["split"] = s.get("split",{}).get("description")
            data.append(pd.Series(stats))
    
    columns = ['season','split_code','split','tm_mlbam','Team','G','GO','AO','R','2B','3B','HR','SO','BB','IBB','H','HBP','AVG','AB','OBP','SLG','OPS','CS','SB','SB%','GIDP','GITP','P','PA','TB','RBI','LOB','sB','sF','BABIP','GO/AO','CI','AB/HR']
    df = pd.DataFrame(data).rename(columns=STATDICT)[columns]
    # df = pd.DataFrame(data).rename(columns=STATDICT)
    return df

def player_splits_advanced(mlbam,statGroup,sitCodes,season=None,gameType=None) -> pd.DataFrame:
    """Get a player's stats for a specified date range

    Parameters
    ----------
    mlbam : str or int, required
        player's official MLB ID

    statGroup : str, required
        filter results by stat group ('hitting','pitching','fielding')
    
    sitCodes : str, required
        situation code(s) to get stats for ("h" for home games, "a" for away games, "n" for night games, etc.)

    gameType : str, optional
        filter results by game type (only one gameType can be specified per call)

    """

    mlbam = str(mlbam)

    params = {
        "stats":"statSplitsAdvanced",
        "group":statGroup,
        "sitCodes":sitCodes,
    }

    if gameType is not None:
        params["gameType"] = gameType
    if season is not None:
        params["season"] = season
    else:
        params["season"] = default_season()

    url = BASE + f"/people/{mlbam}/stats?"

    resp = requests.get(url,params=params)
    resp_json = resp.json()

    data = []

    for s in resp_json["stats"][0]["splits"]:
        if s.get("sport",{}).get("id") == 1:
            stats = s.get("stat")
            stats["tm_mlbam"] = s.get("team",{}).get("id")
            stats["tm_name"] = s.get("team",{}).get("name")
            stats["season"] = s.get("season")
            stats["split_code"] = s.get("split",{}).get("code")
            stats["split"] = s.get("split",{}).get("description")
            data.append(pd.Series(stats))
    
    columns = ['season','split_code','split','tm_mlbam','Team','PA','TB','sB','sF','BABIP','exBH','HBP','GIDP','P','P/PA','BB/PA','SO/PA','HR/PA','BB/SO','ISO']
    df = pd.DataFrame(data).rename(columns=STATDICT)[columns]
    # df = pd.DataFrame(data).rename(columns=STATDICT)
    return df

def player_tracking(mlbam,statGroup):
    url = BASE + f"/people/547989/stats?stats=tracking&season=2021"

def player_stats(mlbam,statGroup,statType,season=None,**kwargs) -> pd.DataFrame:
    """Get various types of player stats, game logs, and pitch logs

    Parameters
    ----------
    mlbam : str or int, required
        player's official MLB ID

    statGroup : str or list, required
        the stat group(s) for which to receive stats. (e.g. "hitting", "pitching", "fielding")

    statType : str or list, required
        the type of stats to search for (e.g. "season", "vsPlayer","yearByYearAdvanced", etc...)

    season : str or int, optional (Default is the current season; or the last completed if in off-season)
        the season to search for results (some cases may allow a comma-delimited list of values)

    gameType : str, optional
        filter results by game type (e.g. "R", "S", "D,L,W", etc.)

    opposingTeamId : str or int, conditionally required
        the opposing team ID to filter results for statTypes "vsTeam", "vsTeamTotal", "vsTeam5Y"

    opposingPlayerId : str or int, conditionally required
        the opposing player ID to filter results for statTypes "vsPlayer", "vsPlayerTotal", "vsPlayer5Y"

    date : str or datetime, optional
        date to search for results (str format -> YYYY-mm-dd)

    startDate : str or datetime, conditionally required
        the starting date boundary to search for results (str format -> YYYY-mm-dd)
        Required for following stat types 'byDateRange', 'byDateRangeAdvanced'
    
    endDate : str or datetime, conditionally required
        the ending date boundary to search for results (str format -> YYYY-mm-dd)
        Required for following stat types 'byDateRange', 'byDateRangeAdvanced'

    eventType : str, optional
        filter results by event type
    
    pitchType : str,optional
        filter results by the type of pitch thrown (for stat types 'pitchLog' and 'playLog')

    oppTeamId : str, optional
        Alias for 'opposingTeamId'
    
    oppPlayerId : str, optional
        Alias for 'opposingPlayerId'

    
    Returns
    -------
    pandas.DataFrame

    See Also
    --------
    mlb.player_hitting()

    mlb.team_stats()

    mlb.statTypes()

    mlb.eventTypes()

    mlb.gameTypes()

    Notes
    -----
    Column labels may differ depending on the stat type; particularly for the 'pitchLog' and 'playLog' stat types
    

    Examples
    --------

    """
    
    params = {}

    statType = statType
    statGroup = statGroup

    # Accounting for possible kwargs and aliases
    if kwargs.get("type") is not None:
        statType = kwargs["type"]

    if kwargs.get("group") is not None:
        statGroup = kwargs["group"]

    if kwargs.get("eventType") is not None:
        eventType = kwargs["eventType"]
    elif kwargs.get("eventTypes") is not None:
        eventType = kwargs["eventTypes"]
    elif kwargs.get("event") is not None:
        eventType = kwargs["event"]
    else:
        eventType = None

    if kwargs.get("gameType") is not None:
        gameType = kwargs["gameType"]
    elif kwargs.get("gameTypes") is not None:
        gameType = kwargs["gameTypes"]
    else:
        gameType = None
    
    if kwargs.get("startDate") is not None:
        startDate = kwargs["startDate"]
        if kwargs.get("endDate") is not None:
            endDate = kwargs["endDate"]
        else:
            print("'startDate' and 'endDate' params must be used together")
            return None
        if type(startDate) is dt.datetime or type(startDate) is dt.date:
            startDate = startDate.strftime(r"%Y-%m-%d")
        if type(endDate) is dt.datetime or type(endDate) is dt.date:
            endDate = endDate.strftime(r"%Y-%m-%d")
        params["startDate"] = startDate
        params["endDate"] = endDate

    if kwargs.get("opposingTeamId") is not None:
        opposingTeamId = kwargs["opposingTeamId"]
    elif kwargs.get("oppTeamId") is not None:
        opposingTeamId = kwargs["oppTeamId"]
    else:
        opposingTeamId = None

    if kwargs.get("opposingPlayerId") is not None:
        opposingPlayerId = kwargs["opposingPlayerId"]
    elif kwargs.get("oppPlayerId") is not None:
        opposingPlayerId = kwargs["oppPlayerId"]
    else:
        opposingPlayerId = None

    if kwargs.get("pitchType") is not None:
        pitchType = kwargs["pitchType"]
        if type(pitchType) is str:
            eventType = eventType
        elif type(pitchType) is list:
            pitchType = ",".join(pitchType).upper()
        params["pitchType"] = pitchType

    if eventType is not None:
        if type(eventType) is str:
            eventType = eventType
        elif type(eventType) is list:
            eventType = ",".join(eventType)
        params["eventType"] = eventType

    if gameType is not None:
        if type(gameType) is str:
            gameType = gameType.upper()
        elif type(gameType) is list:
            gameType = ",".join(gameType).upper()
        params["gameType"] = gameType

    if statType == "vsTeam" or statType == "vsTeamTotal" or statType == "vsTeam5Y":
        if opposingTeamId is None:
            print("Params 'opposingTeamId' is required when using statType 'vsTeam','vsTeamTotal',or 'vsTeam5Y'")
            return None
        else:
            params["opposingTeamId"] = opposingTeamId
        if season is not None:
            params["season"] = season
    elif statType == "vsPlayer" or statType == "vsPlayerTotal" or statType == "vsPlayer5Y":
        if opposingPlayerId is not None:
            params["opposingPlayerId"] = opposingPlayerId
        if opposingTeamId is not None:
            params["opposingTeamId"] = opposingTeamId
        if opposingPlayerId is None and opposingTeamId is None:
            print("Must use either 'opposingTeamId' or 'opposingPlayerId' params for stat types 'vsPlayer','vsPlayerTotal', and 'vsPlayer5Y'")
            return None
        if season is not None:
            params["season"] = season
    else:
        if season is not None:
            params["season"] = season
        else:
            params["season"] = default_season()
    
    if statType == "pitchLog" or statType == "playLog":
        params["hydrate"] = "pitchData,hitData"
    else:
        params["hydrate"] = "team"

    params["group"]     = statGroup
    params["stats"]     = statType

    url = BASE + f"/people/{mlbam}/stats?"

    resp = requests.get(url,params=params)
    resp_data = resp.json()

    if season is not None:
        season = str(season)

    season_col = []
    player_name_col = []
    player_id_col = []
    team_name_col = []
    team_id_col = []
    league_abbrv_col = []
    league_id_col = []
    game_type_col = []

    opp_player_name_col = []
    opp_player_id_col = []
    opp_team_name_col = []
    opp_team_id_col = []
    opp_league_abbrv_col = []
    opp_league_id_col = []

    data = []
    
    # Depending on the statType and statGroup, the dict key for the player in-question may vary. 
    # If you're getting stats for mlbam 547989 (Jose Abreu) and statType='vsPlayer', statGroup='hitting',
        # the player information will be under the "batter" key. While the "opposingPlayerId" player's info is 
        # found under the "pitcher" key. This is likely the case for statTypes such as "vsPlayer","vsTeam","playLog","pitchLog"

    # The 'player_key' variable is meant to account for this

    if statType in ("vsPlayer","vsPlayer5Y","vsTeam","vsTeamTotal","vsTeam5Y","pitchLog","playLog"):
        if statGroup == "hitting":
            player_key      = "batter"
            opp_player_key  = "pitcher"
        elif statGroup == "pitching":
            player_key      = "pitcher"
            opp_player_key  = "batter"

    else:
        player_key          = "player"
        opp_player_key      = ""

    if statType != "pitchLog" and statType != "playLog":
        for i in resp_data["stats"]:
            stat_type = i["type"]["displayName"]
            stat_group = i["group"]["displayName"]
            if statType != stat_type:
                continue
            for s in i["splits"]:
                team        = s.get("team",{})
                player      = s.get(player_key,{})
                opp_player  = s.get(opp_player_key,{})
                opp_team    = s.get("opponent",{})
                league      = team.get("league",{})
                opp_league  = opp_team.get("league",{})
                stats       = s.get("stat")
                
                season_col.append(s.get("season","-"))

                game_type_col.append(s.get("gameType"))

                player_name_col.append(player.get("fullName"))
                player_id_col.append(str(player.get("id",'-')))

                team_name_col.append(team.get("name"))
                team_id_col.append(str(team.get("id",'-')))

                league_id = str(league.get("id"))
                league_id_col.append(league_id)
                league_abbrv_col.append(LEAGUE_IDS.get(league_id))

                opp_player_name_col.append(opp_player.get("fullName"))
                opp_player_id_col.append(str(opp_player.get("id",'-')))

                opp_team_name_col.append(opp_team.get("name"))
                opp_team_id_col.append(str(opp_team.get("id",'-')))

                opp_league_id = str(opp_league.get("id"))
                opp_league_id_col.append(opp_league_id)
                opp_league_abbrv_col.append(LEAGUE_IDS.get(opp_league_id))

                if stat_group == "fielding":
                    stats["position"] = stats["position"]["abbreviation"]

                data.append(pd.Series(stats))
        df = pd.DataFrame(data=data).rename(columns=STATDICT)
    else:
        for i in resp_data["stats"]:
            stat_type = i["type"]["displayName"]
            stat_group = i["group"]["displayName"]

            for s in i["splits"]:
                play        = s.get("stat",{}).get("play",{})
                team        = s.get("team",{})
                batter      = s.get(player_key,{})
                opp_player  = s.get(opp_player_key,{})
                opp_team    = s.get("opponent",{})
                game        = s.get("game")
                date        = s.get("date")
                
                season_col.append(s.get("season","-"))

                game_type_col.append(s.get("gameType"))

                player_name_col.append(batter.get("fullName"))
                player_id_col.append(str(batter.get("id",'-')))

                team_name_col.append(team.get("name"))
                team_id_col.append(str(team.get("id",'-')))

                opp_player_name_col.append(opp_player.get("fullName"))
                opp_player_id_col.append(str(opp_player.get("id",'-')))

                opp_team_name_col.append(opp_team.get("name"))
                opp_team_id_col.append(str(opp_team.get("id",'-')))

                game_pk     = game.get("gamePk")
                game_num    = game.get("gameNumber")

                play_id     = play.get("playId")

                details     = play.get("details",{})
                count       = details.get("count",{})
                event       = details.get("event")
                desc        = details.get("description")
                ptype       = details.get("type",{}).get("description")
                ptype_code  = details.get("type",{}).get("code")

                balls       = count.get("balls")
                strikes     = count.get("strikes")
                outs        = count.get("outs")
                inn         = count.get("inning")
                runner_1b   = count.get("runnerOn1b")
                runner_2b   = count.get("runnerOn2b")
                runner_3b   = count.get("runnerOn3b")

                pdata       = play.get("pitchData",{})
                start_spd   = pdata.get("startSpeed")
                sz_top      = pdata.get("strikeZoneTop")
                sz_bot      = pdata.get("strikeZoneBottom")
                coords      = pdata.get("coordinates",{})
                zone        = pdata.get("zone")
                px          = coords.get("pX")
                py          = coords.get("pY")
                pitch_hand  = details.get("pitchHand",{}).get("code")

                hdata       = play.get("hitData",{})
                launch_spd  = hdata.get("launchSpeed")
                launch_ang  = hdata.get("launchAngle")
                total_dist  = hdata.get("totalDistance")
                trajectory  = hdata.get("trajectory")
                coords      = hdata.get("coordinates",{})
                hx          = coords.get("landingPosX")
                hy          = coords.get("landingPosY")
                bat_side    = details.get("batSide",{}).get("code")

                ab_num      = play.get("atBatNumber")
                pitch_num   = play.get("pitchNumber")

                stats = {
                    "gamePk":       game_pk,
                    "date":         date,
                    "game_num":     game_num,
                    "play_id":      play_id,
                    "ab_num":       ab_num,
                    "event":        event,
                    "desc":         desc,
                    "balls":        balls,
                    "strikes":      strikes,
                    "outs":         outs,
                    "inning":       inn,
                    "r1b":          runner_1b,
                    "r2b":          runner_2b,
                    "r3b":          runner_3b,

                    "pitch_num":    pitch_num,
                    "pitch_hand":   pitch_hand,
                    "pitch_type":   ptype,
                    "pitch_type_id":ptype_code,
                    "start_spd":    start_spd,
                    "sz_top":       sz_top,
                    "sz_bot":       sz_bot,
                    "zone":         zone,
                    "px":           px,
                    "py":           py,

                    "bat_side":     bat_side,
                    "launch_spd":   launch_spd,
                    "launch_ang":   launch_ang,
                    "total_dist":   total_dist,
                    "trajectory":   trajectory,
                    "hx":           hx,
                    "hy":           hy
                }
                
                data.append(pd.Series(stats))

        df = pd.DataFrame(data=data)
        df.insert(0,"season",season_col)
        df.insert(1,"player",player_name_col)
        df.insert(2,"player_mlbam",player_id_col)
        df.insert(3,"team",team_name_col)
        df.insert(4,"team_mlbam",team_id_col)
        df.insert(5,"opp_player",opp_player_name_col)
        df.insert(6,"opp_player_mlbam",opp_player_id_col)
        df.insert(7,"opp_team",opp_team_name_col)
        df.insert(8,"opp_team_mlbam",opp_team_id_col)

        df.dropna(axis="columns",how="all",inplace=True)

        return df

    cols_in_order = []

    if stat_group == "hitting":
        if "advanced" in stat_type.lower():
            col_constants = BAT_FIELDS_ADV
        else:
            col_constants = BAT_FIELDS
        for col in col_constants:
            cols_in_order.append(col)

    elif stat_group == "pitching":
        if "advanced" in stat_type.lower():
            col_constants = PITCH_FIELDS_ADV
        else:
            col_constants = PITCH_FIELDS
        for col in col_constants:
            cols_in_order.append(col)

    elif stat_group == "fielding":
        col_constants = FIELD_FIELDS
        for col in col_constants:
            cols_in_order.append(col)


    df.insert(0,"season",season_col)
    df.insert(1,"player",player_name_col)
    df.insert(2,"player_mlbam",player_id_col)
    df.insert(3,"team",team_name_col)
    df.insert(4,"team_mlbam",team_id_col)
    df.insert(5,"league",league_abbrv_col)
    df.insert(6,"league_mlbam",league_id_col)
    if statType in ("vsPlayer","vsPlayerTotal","vsPlayer5Y","vsTeam","vsTeamTotal","vsTeam5Y","gameLog"):
        if statType != "gameLog":
            df.insert(7,"opp_player",opp_player_name_col)
            df.insert(8,"opp_player_mlbam",opp_player_id_col)
            df.insert(9,"opp_team",opp_team_name_col)
            df.insert(10,"opp_team_mlbam",opp_team_id_col)
            df.insert(11,"opp_league",opp_league_abbrv_col)
            df.insert(12,"opp_league_mlbam",opp_league_id_col)
        else:
            df.insert(7,"opp_team",opp_team_name_col)
            df.insert(8,"opp_team_mlbam",opp_team_id_col)
            df.insert(9,"opp_league",opp_league_abbrv_col)
            df.insert(10,"opp_league_mlbam",opp_league_id_col)

    df.dropna(axis="columns",how="all",inplace=True)

    return df

def player_data(mlbam) -> dict[pd.DataFrame]:
    """Fetch a variety of player information/stats in one API call

    Parameters
    ----------
    mlbam : str or int
        Official team MLB ID

    season : int or str
        season/year ID. If season is not specified, data for the entire franchise will be retrieved by default

    ***

    Keys for returned data
    ---------------------------
    "hitting" 
    "hitting_advanced"
    "pitching"
    "pitching_advanced"
    "fielding"

    
    """

def player_search(name,sportId=1,**kwargs):
    """Search for a person using the API by name
    
    Paramaters
    ----------
    
    name : str
        person's name

        names
        required
            
        string

        Insert name to search for players. Users can search by first name, middle name, last name, and nick name

            Insert name: https://statsapi.mlb.com/api/v1/people/search?names=juansoto
            Insert first name: https://statsapi.mlb.com/api/v1/people/search?names=babe
            Insert last name: https://statsapi.mlb.com/api/v1/people/search?names=salmon
            Insert nickname: https://statsapi.mlb.com/api/v1/people/search?names=bighurt
            Insert letter: https://statsapi.mlb.com/api/v1/people/search?names=j

        personIds
        required
            
        string

        Insert personId(s) to search and return biographical information for a specific player(s). Format '605151,592450'

            One personId: https://statsapi.mlb.com/api/v1/people/search?personIds=605151
            Multiple personIds: https://statsapi.mlb.com/api/v1/people/search?personIds=605151,592450

        List of players by sport can be found in the Sports endpoint under the “view information on a players for a given sportId” subsection.
        sportIds	
        string

        Insert sportId(s) to search and return biographical information for players in a specific sport(s).

            One sportId: https://statsapi.mlb.com/api/v1/people/search?names=juan&sportIds=23
            Multiple sportIds: https://statsapi.mlb.com/api/v1/people/search?names=juan&sportIds=23,1

        List of players by sport can be found in the Sports endpoint under the “view information on a players for a given sportId” subsection.
        leagueIds	
        string

        Insert leagueId(s) to search and return biographical information for players in a specific league(s).

            One leagueId: https://statsapi.mlb.com/api/v1/people/search?names=juan&leagueIds=104
            Multiple leagueIds: https://statsapi.mlb.com/api/v1/people/search?names=juan&leagueIds=104,103

        teamIds	
        string

        Insert teamId(s) to search and return biographical information for players on a specific team(s).

            One sportId: https://statsapi.mlb.com/api/v1/people/search?names=juan&teamIds=133
            Multiple sportIds: https://statsapi.mlb.com/api/v1/people/search?names=juan&teamIds=133,147

        active	
        string

        Insert active to search and return biographical information for players if they are active.

            active: https://statsapi.mlb.com/api/v1/people/search?names=bigUnit&sportId=1&active=true

        rookie	
        string

        Insert rookie to search and return biographical information for players if they are rookies.

            rookie: https://statsapi.mlb.com/api/v1/people/search?names=a&sportId=1&rookie=true

        limit	
        string

        Insert a limit to limit return {Limit 50}.

            One Limit: https://statsapi.mlb.com/api/v1/people/search?names=juan&leagueIds=103&limit=25

        limit	
        string

        Insert a limit to limit return {Limit 50}.

            One Limit: https://statsapi.mlb.com/api/v1/people/search?names=juan&leagueIds=103&limit=25

        hydrate	
        string

        Insert hydration(s) to return statistical or biographical data for a specific player(s).

            One Hydration: https://statsapi.mlb.com/api/v1/people/search?names=trout&hydrate=currentTeam
            Check For Available Hydrations: https://statsapi.mlb.com/api/v1/people/search?names=trout&hydrate=hydrations

            
        any

        Comma delimited list of specific fields to be returned. Format: topLevelNode, childNode, attribute
        fields	
        Array of strings unique

        Comma delimited list of specific fields to be returned. Format: topLevelNode, childNode, attribute

        Example: http://statsapi.mlb.com/api/v1/people/592178/stats/game/current?fields=stats,splits,stat,type,group




    """
    url = f"https://statsapi.mlb.com/api/v1/people/search?names={name}"

    resp = requests.get(url)

    return resp.json()

# ===============================================================
# TEAM Functions
# ===============================================================
def team_hitting(mlbam,season=None) -> pd.DataFrame:
    """Get team hitting stats (roster-level)

    Parameters
    ----------
    mlbam : str or int
        team's official MLB ID

    season : str or int, optional (the default is the current season or the most recently completed if in the off-season)
        the specific season to get stats for
    """

    if season is None:
        season = default_season()

    statTypes = "season"
    statGroups = "hitting"
    roster_hydrations = f"person(stats(type=[{statTypes}],group=[{statGroups}],season={season}))&season={season}"
    url = BASE + f"/teams/{mlbam}/roster?rosterType=fullSeason&hydrate={roster_hydrations}"

    response = requests.get(url)

    response = response.json()
    
    hitting = []
    hittingAdvanced = []

    got_hit = False
    got_hitAdv = False

    hitting_cols = ["status","mlbam","playerName","primaryPosition"]
    hittingAdv_cols = ["status","mlbam","playerName","primaryPosition"]

    for player in response["roster"]:
        status = player["status"]["description"]
        playerName = player["person"]["fullName"]
        primaryPosition = player["person"]["primaryPosition"]["abbreviation"]
        player_mlbam = player["person"]["id"]
        try:
            player_groups_types = player["person"]["stats"]
            for group_type in player_groups_types:
                statType = group_type["type"]["displayName"]
                statGroup = group_type["group"]["displayName"]

                # HITTING
                if statGroup == "hitting" and statType == "season":
                    single_stat_row = [status,player_mlbam,playerName,primaryPosition]
                    player_stats = group_type["splits"][0]["stat"]
                    for stat in BAT_FIELDS:
                        if got_hit is False:hitting_cols.append(stat)
                        try:
                            single_stat_row.append(player_stats[stat])
                        except:
                            single_stat_row.append("--")
                    got_hit = True
                    hitting.append(single_stat_row)
                elif statGroup == "hitting" and statType == "seasonAdvanced":
                    single_stat_row = [status,player_mlbam,playerName,primaryPosition]
                    player_stats = group_type["splits"][0]["stat"]
                    for stat in BAT_FIELDS_ADV:
                        if got_hitAdv is False:hittingAdv_cols.append(stat)
                        try:
                            single_stat_row.append(player_stats[stat])
                        except:
                            single_stat_row.append("--")
                    got_hitAdv = True
                    hittingAdvanced.append(single_stat_row)

        except:
            print(f"Error retrieving stats for --- {playerName}")
            pass
    
    df = pd.DataFrame(hitting,columns=hitting_cols).rename(columns=STATDICT)
    return df

def team_pitching(mlbam,season=None) -> pd.DataFrame:
    """Get team pitching stats (roster-level)

    Parameters
    ----------
    mlbam : str or int
        team's official MLB ID

    season : str or int, optional (the default is the current season or the most recently completed if in the off-season)
        the specific season to get stats for
    """

    if season is None:
        season = default_season()

    statTypes = "season"
    statGroups = "pitching"
    roster_hydrations = f"person(stats(type=[{statTypes}],group=[{statGroups}],season={season}))&season={season}"
    url = BASE + f"/teams/{mlbam}/roster?rosterType=fullSeason&hydrate={roster_hydrations}"

    response = requests.get(url)

    response = response.json()

    pitching = []
    pitchingAdvanced = []

    got_pitch = False
    got_pitchAdv = False

    pitching_cols = ["status","mlbam","playerName","primaryPosition"]
    pitchingAdv_cols = ["status","mlbam","playerName","primaryPosition"]

    for player in response["roster"]:
        status = player["status"]["description"]
        playerName = player["person"]["fullName"]
        primaryPosition = player["person"]["primaryPosition"]["abbreviation"]
        player_mlbam = player["person"]["id"]
        try:
            player_groups_types = player["person"]["stats"]
            for group_type in player_groups_types:
                statType = group_type["type"]["displayName"]
                statGroup = group_type["group"]["displayName"]

                # PITCHING
                if statGroup == "pitching" and statType == "season":
                    single_stat_row = [status,player_mlbam,playerName,primaryPosition]
                    player_stats = group_type["splits"][0]["stat"]
                    for stat in PITCH_FIELDS:
                        if got_pitch is False: pitching_cols.append(stat)
                        try:
                            single_stat_row.append(player_stats[stat])
                        except:
                            single_stat_row.append("--")
                    got_pitch = True
                    pitching.append(single_stat_row)
                elif statGroup == "pitching" and statType == "seasonAdvanced":
                    single_stat_row = [status,player_mlbam,playerName,primaryPosition]
                    player_stats = group_type["splits"][0]["stat"]
                    for stat in PITCH_FIELDS_ADV:
                        if got_pitchAdv is False: pitchingAdv_cols.append(stat)
                        try:
                            single_stat_row.append(player_stats[stat])
                        except:
                            single_stat_row.append("--")
                    got_pitchAdv = True
                    pitchingAdvanced.append(single_stat_row)
        except:
            # print(f"Error retrieving stats for --- {playerName}")
            pass

    df = pd.DataFrame(pitching,columns=pitching_cols).rename(columns=STATDICT)
    # df = pd.DataFrame(pitchingAdvanced,columns=pitchingAdv_cols).rename(columns=STATDICT)

    return df

def team_fielding(mlbam,season=None) -> pd.DataFrame:
    """Get team fielding stats (roster-level)

    Parameters
    ----------
    mlbam : str or int
        team's official MLB ID

    season : str or int, optional (the default is the current season or the most recently completed if in the off-season)
        the specific season to get stats for
    """

    if season is None:
        season = default_season()

    statTypes = "season"
    statGroups = "fielding"
    roster_hydrations = f"person(stats(type=[{statTypes}],group=[{statGroups}],season={season}))&season={season}"
    url = BASE + f"/teams/{mlbam}/roster?rosterType=fullSeason&hydrate={roster_hydrations}"

    response = requests.get(url)

    response = response.json()

    fielding = []

    got_field = False

    fielding_cols = ["status","mlbam","playerName","position"]

    for player in response["roster"]:
        status = player["status"]["description"]
        playerName = player["person"]["fullName"]
        primaryPosition = player["person"]["primaryPosition"]["abbreviation"]
        player_mlbam = player["person"]["id"]
        try:
            player_groups_types = player["person"]["stats"]
            for group_type in player_groups_types:
                statType = group_type["type"]["displayName"]
                statGroup = group_type["group"]["displayName"]

                # FIELDING
                if statGroup == "fielding" and statType == "season":
                    player_positions = group_type["splits"]
                    for position in player_positions:
                        player_stats = position["stat"]
                        pos = player_stats["position"]["abbreviation"]
                        if pos == primaryPosition:
                            pos = "*"+pos
                        single_stat_row = [status,player_mlbam,playerName,pos]
                        for stat in FIELD_FIELDS:
                            if got_field is False: fielding_cols.append(stat)
                            try:
                                single_stat_row.append(player_stats[stat])
                            except:
                                single_stat_row.append("--")
                        got_field = True
                        fielding.append(single_stat_row)
        except:
            print(f"Error retrieving stats for --- {playerName}")
            pass

    df = pd.DataFrame(fielding,columns=fielding_cols).rename(columns=STATDICT)

    # df.sort_values(by=["Season","team_mlbam","PO"],ascending=[True,True,False])
    return df

def team_hitting_advanced(mlbam,season=None) -> pd.DataFrame:
    """Get advanced team hitting stats (roster-level)

    Parameters
    ----------
    mlbam : str or int
        team's official MLB ID

    season : str or int, optional (the default is the current season or the most recently completed if in the off-season)
        the specific season to get stats for
    """

    if season is None:
        season = default_season()

    statTypes = "seasonAdvanced"
    statGroups = "hitting"
    roster_hydrations = f"person(stats(type=[{statTypes}],group=[{statGroups}],season={season}))&season={season}"
    url = BASE + f"/teams/{mlbam}/roster?rosterType=fullSeason&hydrate={roster_hydrations}"

    response = requests.get(url)

    response = response.json()
    
    hitting = []
    hittingAdvanced = []

    got_hit = False
    got_hitAdv = False

    hitting_cols = ["status","mlbam","playerName","primaryPosition"]
    hittingAdv_cols = ["status","mlbam","playerName","primaryPosition"]

    for player in response["roster"]:
        status = player["status"]["description"]
        playerName = player["person"]["fullName"]
        primaryPosition = player["person"]["primaryPosition"]["abbreviation"]
        player_mlbam = player["person"]["id"]
        try:
            player_groups_types = player["person"]["stats"]
            for group_type in player_groups_types:
                statType = group_type["type"]["displayName"]
                statGroup = group_type["group"]["displayName"]

                # HITTING
                if statGroup == "hitting" and statType == "season":
                    single_stat_row = [status,player_mlbam,playerName,primaryPosition]
                    player_stats = group_type["splits"][0]["stat"]
                    for stat in BAT_FIELDS:
                        if got_hit is False:hitting_cols.append(stat)
                        try:
                            single_stat_row.append(player_stats[stat])
                        except:
                            single_stat_row.append("--")
                    got_hit = True
                    hitting.append(single_stat_row)
                elif statGroup == "hitting" and statType == "seasonAdvanced":
                    single_stat_row = [status,player_mlbam,playerName,primaryPosition]
                    player_stats = group_type["splits"][0]["stat"]
                    for stat in BAT_FIELDS_ADV:
                        if got_hitAdv is False:hittingAdv_cols.append(stat)
                        try:
                            single_stat_row.append(player_stats[stat])
                        except:
                            single_stat_row.append("--")
                    got_hitAdv = True
                    hittingAdvanced.append(single_stat_row)

        except:
            print(f"Error retrieving stats for --- {playerName}")
            pass
    
    df = pd.DataFrame(hittingAdvanced,columns=hittingAdv_cols).rename(columns=STATDICT)
    return df

def team_pitching_advanced(mlbam,season=None) -> pd.DataFrame:
    """Get advanced team pitching stats (roster-level)

    Parameters
    ----------
    mlbam : str or int
        team's official MLB ID

    season : str or int, optional (the default is the current season or the most recently completed if in the off-season)
        the specific season to get stats for
    """
    if season is None:
        season = default_season()

    statTypes = "seasonAdvanced"
    statGroups = "pitching"
    roster_hydrations = f"person(stats(type=[{statTypes}],group=[{statGroups}],season={season}))&season={season}"
    url = BASE + f"/teams/{mlbam}/roster?rosterType=fullSeason&hydrate={roster_hydrations}"

    response = requests.get(url)

    response = response.json()

    pitching = []
    pitchingAdvanced = []

    got_pitch = False
    got_pitchAdv = False

    pitching_cols = ["status","mlbam","playerName","primaryPosition"]
    pitchingAdv_cols = ["status","mlbam","playerName","primaryPosition"]

    for player in response["roster"]:
        status = player["status"]["description"]
        playerName = player["person"]["fullName"]
        primaryPosition = player["person"]["primaryPosition"]["abbreviation"]
        player_mlbam = player["person"]["id"]
        try:
            player_groups_types = player["person"]["stats"]
            for group_type in player_groups_types:
                statType = group_type["type"]["displayName"]
                statGroup = group_type["group"]["displayName"]

                # PITCHING
                if statGroup == "pitching" and statType == "season":
                    single_stat_row = [status,player_mlbam,playerName,primaryPosition]
                    player_stats = group_type["splits"][0]["stat"]
                    for stat in PITCH_FIELDS:
                        if got_pitch is False: pitching_cols.append(stat)
                        try:
                            single_stat_row.append(player_stats[stat])
                        except:
                            single_stat_row.append("--")
                    got_pitch = True
                    pitching.append(single_stat_row)
                elif statGroup == "pitching" and statType == "seasonAdvanced":
                    single_stat_row = [status,player_mlbam,playerName,primaryPosition]
                    player_stats = group_type["splits"][0]["stat"]
                    for stat in PITCH_FIELDS_ADV:
                        if got_pitchAdv is False: pitchingAdv_cols.append(stat)
                        try:
                            single_stat_row.append(player_stats[stat])
                        except:
                            single_stat_row.append("--")
                    got_pitchAdv = True
                    pitchingAdvanced.append(single_stat_row)
        except:
            # print(f"Error retrieving stats for --- {playerName}")
            pass

    # df = pd.DataFrame(pitching,columns=pitching_cols).rename(columns=STATDICT)
    df = pd.DataFrame(pitchingAdvanced,columns=pitchingAdv_cols).rename(columns=STATDICT)

    return df

def team_game_logs(mlbam,season=None,statGroup=None,gameType=None,**kwargs) -> pd.DataFrame:
    """Get a team's game log stats for a specific season

    Parameters
    ----------
    mlbam : str or int
        team's official MLB ID
    
    seasons : str or int, optional (Default is the most recent season)
        filter by season

    statGroup : str, optional
        filter results by stat group ('hitting','pitching','fielding')

    gameType : str, optional
        filter results by game type (only one can be specified per call)

    startDate : str, optional
        include games AFTER a specified date
    
    endDate : str, optional
        include games BEFORE a specified date

    """

    params = {
        "stats":"gameLog"
    }
    if kwargs.get("seasons") is not None:
        season = kwargs["seasons"]

    if kwargs.get("group") is not None:
        statGroup = kwargs["group"]
    elif kwargs.get("groups") is not None:
        statGroup = kwargs["groups"]
    elif kwargs.get("statGroups") is not None:
        statGroup = kwargs["statGroups"]
    else:
        statGroup = statGroup

    if kwargs.get("gameTypes") is not None:
        gameType = kwargs["gameTypes"]
    elif kwargs.get("game_type") is not None:
        gameType = kwargs["game_type"]

    if season is not None:
        params["season"] = season
    if statGroup is not None:
        params["group"] = statGroup
    if gameType is not None:
        params["gameType"] = gameType
    if kwargs.get("startDate") is not None:
        params["startdate"] = kwargs["startDate"]
    if kwargs.get("endDate") is not None:
        params["endDate"] = kwargs["endDate"]

    url = BASE + f"/teams/{mlbam}/stats?"
    resp = requests.get(url,params=params)

    data = []
    tms_df = get_teams_df(year=season).set_index("mlbam")

    # "sg" referes to each stat group in the results
    for sg in resp.json()["stats"]:
        if sg["group"]["displayName"] == "hitting":
            # "g" refers to each game
            for g in sg.get("splits"):
                game = g.get("stat",{})
                
                tm = g.get("team",{})
                tm_opp_mlbam = g.get("opponent",{}).get("id")
                
                game["date"] = g.get("date","")
                game["isHome"] = g.get("isHome",False)
                game["isWin"] = g.get("isWin",False)
                game["gamePk"] = g.get("game",{}).get("gamePk")
                game["mlbam"] = tm.get("id")
                game["name"] = tm.get("name")
                game["opp_mlbam"] = tm_opp_mlbam
                game["opp_name"] = tms_df.loc[tm_opp_mlbam]["fullName"]

                data.append(pd.Series(game))

            break
    columns = ['date','isHome','isWin','gamePk','mlbam','name','opp_mlbam','opp_name','G','GO','AO','R','2B','3B','HR','SO','BB','IBB','H','HBP','AVG','AB','OBP','SLG','OPS','CS','SB','SB%','GIDP','P','PA','TB','RBI','LOB','sB','sF','BABIP','GO/AO','AB/HR']
    
    df = pd.DataFrame(data=data).rename(columns=STATDICT)[columns]

    return df

def team_roster(mlbam,season=None,rosterType=None,**kwargs) -> pd.DataFrame:
    """Get team rosters by season

    Parameters
    ----------
    mlbam : str or int, required
        team's official MLB ID
    
    season : str or int, optional (Default is the current season or the most recently completed if in the off-season)
        specify a team's roster by season. This value will be ignored if rosterType="allTime" or rosterType="depthChart".

    rosterType : str, optional
        specify the type of roster to retrieve

    Roster Types
    ------------
    - "active" (Default)
        - Active roster for a team
    - "40Man"
        - 40-man roster for a team
    - "depthChart"
        - Depth chart for a team
    - "fullSeason"
        - Full roster including active and inactive players for a season
    - "fullRoster"
        - Full roster including active and inactive players
    - "allTime"
        - All Time roster for a team
    - "coach"
        - Coach roster for a team
    - "gameday" (NOT WORKING)
        - Roster for day of game
    - "nonRosterInvitees"
        - Non-Roster Invitees

    """

    params = {}
    if season is not None:
        params["season"] = season
    elif season is None:
        params["season"] = default_season()
    if rosterType is not None:
        params["rosterType"] = rosterType
    if len(kwargs) != 0:
        for k,v in kwargs.items():
            params["k"] = v
    if kwargs.get("hydrate") is not None:
        params["hydrate"] = kwargs["hydrate"]

    # hydrate=person(rosterEntries)
    url = BASE + f"/teams/{mlbam}/roster"

    resp = requests.get(url,params=params)
    roster = resp.json()["roster"]
    
    columns = [
        'mlbam',
        'name',
        'name_first',
        'name_last',
        'name_lastfirst',
        'jersey_number',
        'pos',
        'status',
        'status_code'
        ]
    data = []

    for p in roster:
        person          = p.get("person")
        mlbam           = person.get("id")
        name            = person.get("fullName")
        name_first      = person.get("firstName")
        name_last       = person.get("lastName")
        name_lastfirst  = person.get("lastFirstName")
        jersey_number   = p.get("jerseyNumber")
        position        = p.get("position")
        pos             = position.get("abbreviation")
        status          = p.get("status",{}).get("description")
        status_code     = p.get("status",{}).get("code")
        
        data.append([
            mlbam,
            name,
            name_first,
            name_last,
            name_lastfirst,
            jersey_number,
            pos,
            status,
            status_code
        ])
    df = pd.DataFrame(data=data,columns=columns).sort_values(by="name")

    return df

def team_data(mlbam,season=None,statGroup=None,rosterType=None,gameType="S,R,P",**kwargs) -> dict:
    """Fetch various team season data & information for a team in one API call

    Parameters
    ----------
    mlbam : str or int
        Official team MLB ID

    season : int or str
        season/year ID. If season is not specified, data for the entire franchise will be retrieved by default

    rosterType : str
        specify the type of roster to retrieve (Default is "40Man")

    ***

    Keys for Franchise Data (all year-by-year data)
    ---------------------------
    "records"
    "standings"
    "hitting" 
    "hitting_advanced"
    "pitching"
    "pitching_advanced"
    "fielding"

    Keys Team Data for specific year
    -----------------------------------
    "records"
    "standings"
    "roster_hitting"
    "roster_pitching"
    "roster_fielding"

    """
    

    records = get_yby_records()
    records = records[records['tm_mlbam']==int(mlbam)]
    standings = get_standings_df()
    standings = standings[standings['mlbam']==int(mlbam)]

    if season is None:
        single_season = False
    else:
        single_season = True
        _season = season
    if statGroup is None:
        statGroup = 'hitting,pitching,fielding'
    if kwargs.get("group") is not None:
        statGroup = kwargs["group"]
    elif kwargs.get("groups") is not None:
        statGroup = kwargs["groups"]
    elif kwargs.get("statGroups") is not None:
        statGroup = kwargs["statGroups"]
    if rosterType is None:
        rosterType = '40Man'
    if kwargs.get("gameTypes") is not None:
        gameType = kwargs["gameTypes"]

    if single_season is True:
        records = records[records["season"]==(int(_season))]
        standings = standings[standings["Season"]==int(_season)]
        params = {
            "hydrate":f"person(stats(type=season,season={_season},group=[{statGroup}],gameType=[{gameType}]),currentTeam)",
            "rosterType":rosterType
        }
        url = BASE + f"/teams/{mlbam}/roster"
        resp = requests.get(url,params=params)
        # print(f"ROSTER DATA:        {resp.url}")
        roster_data_json = resp.json()

        hit_cols = ['season','game_type','player_mlbam','player_name','team_mlbam','team_name','G','GO','AO','R','2B','3B','HR','SO','BB','IBB','H','HBP','AVG','AB','OBP','SLG','OPS','CS','SB','SB%','GIDP','P','PA','TB','RBI','LOB','sB','sF','BABIP','GO/AO','CI','AB/HR']
        pit_cols = ['season','game_type','player_mlbam','player_name','team_mlbam','team_name','G','GS','GO','AO','R','2B','3B','HR','SO','BB','IBB','H','HBP','AVG','AB','OBP','SLG','OPS','CS','SB','SB%','GIDP','P','ERA','IP','W','L','SV','SVO','HLD','BS','ER','WHIP','BF','O','GP','CG','ShO','K','K%','HB','BK','WP','PK','TB','GO/AO','W%','P/Inn','GF','SO:BB','SO/9','BB/9','H/9','R/9','HR/9','IR','IRS','CI','sB','sF']
        fld_cols = ['season','game_type','player_mlbam','player_name','team_mlbam','team_name','pos','A','PO','E','Ch','FLD%','RF/G','RF/9','INNs','G','GS','DP','TP','thE','CS','PB','SB','cERA','CI','WP','SB%','PK']
        roster_hit_data = []
        roster_pitch_data = []
        roster_field_data = []
        for person in roster_data_json["roster"]:
            p = person.get("person",{})
            # jersey_number = person.get("jerseyNumber")
            for g in p.get("stats",[{}]):
                sg = g.get("group",{}).get("displayName")
                # st = g.get("type",{}).get("displayName")

                if sg == "hitting":
                    for s in g.get("splits",[{}]):
                        team = s.get("team",{})
                        team_mlbam = team.get("id","")
                        if str(team_mlbam) != str(mlbam):
                            continue
                        team_name = team.get("name","")
                        
                        stats = s.get("stat",{})

                        season = s.get("season",'-')
                        game_type = s.get("gameType","")
                        player = s.get("player",{})
                        player_mlbam = player.get("id","")
                        player_name = player.get("fullName","")
                        # league = s.get("league",{})
                        # league_mlbam = league.get("id","")
                        # league_name = league.get("name","")
                        # pos = s.get("position",{}).get("abbreviation",'-')

                        stats["season"] = season
                        stats["game_type"] = game_type
                        stats["player_mlbam"] = player_mlbam
                        stats["player_name"] = player_name
                        stats["team_mlbam"] = team_mlbam
                        stats["team_name"] = team_name
                        # stats["league_mlbam"] = league_mlbam
                        # stats["league_name"] = league_name
                        # stats["pos"] = pos

                        roster_hit_data.append(pd.Series(stats))
                elif sg == "pitching":
                    for s in g.get("splits",[{}]):
                        team = s.get("team",{})
                        team_mlbam = team.get("id","")
                        if str(team_mlbam) != str(mlbam):
                            continue
                        team_name = team.get("name","")
                        
                        stats = s.get("stat",{})

                        season = s.get("season",'-')
                        game_type = s.get("gameType","")
                        player = s.get("player",{})
                        player_mlbam = player.get("id","")
                        player_name = player.get("fullName","")
                        # league = s.get("league",{})
                        # league_mlbam = league.get("id","")
                        # league_name = league.get("name","")
                        # pos = s.get("position",{}).get("abbreviation",'-')

                        stats["season"] = season
                        stats["game_type"] = game_type
                        stats["player_mlbam"] = player_mlbam
                        stats["player_name"] = player_name
                        stats["team_mlbam"] = team_mlbam
                        stats["team_name"] = team_name
                        # stats["league_mlbam"] = league_mlbam
                        # stats["league_name"] = league_name
                        # stats["pos"] = pos

                        roster_pitch_data.append(pd.Series(stats))

                elif sg == "fielding":
                    for s in g.get("splits",[{}]):
                        team = s.get("team",{})
                        team_mlbam = team.get("id","")
                        if str(team_mlbam) != str(mlbam):
                            continue
                        team_name = team.get("name","")
                        
                        stats = s.get("stat",{})

                        season = s.get("season",'-')
                        game_type = s.get("gameType","")
                        player = s.get("player",{})
                        player_mlbam = player.get("id","")
                        player_name = player.get("fullName","")
                        # league = s.get("league",{})
                        # league_mlbam = league.get("id","")
                        # league_name = league.get("name","")
                        pos = s.get("position",{}).get("abbreviation",'-')

                        stats["season"] = season
                        stats["game_type"] = game_type
                        stats["player_mlbam"] = player_mlbam
                        stats["player_name"] = player_name
                        stats["team_mlbam"] = team_mlbam
                        stats["team_name"] = team_name
                        # stats["league_mlbam"] = league_mlbam
                        # stats["league_name"] = league_name
                        stats["pos"] = pos

                        roster_field_data.append(pd.Series(stats))


        roster_hitting_df = pd.DataFrame(data=roster_hit_data).rename(columns=STATDICT)[hit_cols]
        roster_pitching_df = pd.DataFrame(data=roster_pitch_data).rename(columns=STATDICT)[pit_cols]
        roster_fielding_df = pd.DataFrame(data=roster_field_data).rename(columns=STATDICT)[fld_cols]

        fetched_data = {
            "records":records.reset_index(drop=True),
            "standings":standings.reset_index(drop=True),
            "roster_hitting":roster_hitting_df.reset_index(drop=True),
            "roster_pitching":roster_pitching_df.reset_index(drop=True),
            "roster_fielding":roster_fielding_df.reset_index(drop=True)
        }


    else:
        # == ASYNC STARTS HERE ===============================================
        
        team_df = get_teams_df()
        team_df = team_df[team_df['mlbam']==int(mlbam)]
        firstYear = team_df.iloc[0]["firstYear"]

        urls = []
        years = range(firstYear,int(default_season())+1)

        for year in years:
            urls.append(f"https://statsapi.mlb.com/api/v1/teams/{mlbam}?hydrate=standings&season={year}")                   # yby_data
        # for year in years:
        #     query = f"stats=statSplits&season={year}&sitCodes={sitCodes}&group=hitting,pitching,fielding"
        #     urls.append(f"https://statsapi.mlb.com/api/v1/teams/{mlbam}/stats?{query}")                                     # team_splits

        hydrations = f"nextSchedule(limit=5),previousSchedule(limit=1,season={default_season()}),league,division"           # ---- (hydrations for 'team_info') ----
        urls.append(BASE + f"/teams/{mlbam}?hydrate={hydrations}")                                                          # team_info
        urls.append((BASE + f"/teams/{mlbam}/stats?stats=yearByYear,yearByYearAdvanced&group=hitting,pitching,fielding"))   # team_stats
        urls.append(f"https://statsapi.mlb.com/api/v1/teams/{mlbam}/roster/allTime")                                        # all_players
        urls.append(f"https://statsapi.mlb.com/api/v1/awards/MLBHOF/recipients")                                            # hof_players
        urls.append(f"https://statsapi.mlb.com/api/v1/awards/RETIREDUNI_{mlbam}/recipients")                                # retired_numbers
        resps = get_responses(urls)

        yby_data = resps[:-5]
        team_info = resps[-5]
        team_stats = resps[-4]
        all_players = resps[-3]
        hof_players = resps[-2]
        retired_numbers = resps[-1]

        # ---- Parsing 'team_info' --------- team_info_parsed

        #        Includes basic team information and recent/upcoming schedule information

        team_info_parsed = {}
        teams = team_info["teams"][0]
        team_info_parsed["mlbam"]               = teams["id"]
        team_info_parsed["full_name"]           = teams["name"]
        team_info_parsed["location_name"]       = teams["locationName"]
        team_info_parsed["franchise_name"]      = teams["franchiseName"]
        team_info_parsed["team_name"]           = teams["teamName"]
        team_info_parsed["club_name"]           = teams["clubName"]
        team_info_parsed["short_name"]          = teams["shortName"]
        team_info_parsed["venue_mlbam"]         = teams.get("venue",{}).get("id","")
        team_info_parsed["venue_name"]          = teams.get("venue",{}).get("name","")
        team_info_parsed["first_year"]          = teams["firstYearOfPlay"]
        lg = teams.get("league")
        div = teams.get("division")
        team_info_parsed["league_mlbam"]        = lg.get("id","")
        team_info_parsed["league_name"]         = lg.get("name","")
        team_info_parsed["league_short"]        = lg.get("nameShort","")
        team_info_parsed["league_abbrv"]        = lg.get("abbreviation","")
        team_info_parsed["div_mlbam"]           = div.get("id","")
        team_info_parsed["div_name"]            = div.get("name","")
        team_info_parsed["div_short"]           = div.get("nameShort","")
        team_info_parsed["div_abbrv"]           = div.get("abbreviation","")

        sched_prev_data = []
        for d in teams.get("previousGameSchedule",{}).get("dates",[{}]):
            date_obj = dt.datetime.strptime(d["date"],r"%Y-%m-%d")
            for gm in d["games"]:
                away = gm.get("teams",{}).get("away")
                home = gm.get("teams",{}).get("home")
                sched_prev_data.append([
                    gm.get("season"),
                    date_obj,
                    gm.get("gamePk"),
                    gm.get("gameType"),
                    away.get("team",{}).get("id"),
                    away.get("team",{}).get("name"),
                    home.get("team",{}).get("id"),
                    home.get("team",{}).get("name"),
                    False if gm.get("doubleHeader") == "N" else True,
                    gm.get("seriesGameNumber"),
                    gm.get("gamesInSeries")
                ])
        sched_prev_df = pd.DataFrame(data=sched_prev_data,columns=['season','date','gamePk','game_type','away_mlbam','away_name','home_mlbam','home_name','double_header','series_game','series_length'])
        team_info_parsed["sched_prev"] = sched_prev_df

        sched_next_data = []
        for d in teams.get("nextGameSchedule",{}).get("dates",[{}]):
            date_obj = dt.datetime.strptime(d["date"],r"%Y-%m-%d")
            for gm in d["games"]:
                away = gm.get("teams",{}).get("away")
                home = gm.get("teams",{}).get("home")
                sched_next_data.append([
                    gm.get("season"),
                    date_obj,
                    gm.get("gamePk"),
                    gm.get("gameType"),
                    away.get("team",{}).get("id"),
                    away.get("team",{}).get("name"),
                    home.get("team",{}).get("id"),
                    home.get("team",{}).get("name"),
                    False if gm.get("doubleHeader") == "N" else True,
                    gm.get("seriesGameNumber"),
                    gm.get("gamesInSeries")
                ])
        sched_next_df = pd.DataFrame(data=sched_next_data,columns=['season','date','gamePk','game_type','away_mlbam','away_name','home_mlbam','home_name','double_header','series_game','series_length'])
        team_info_parsed["sched_next"] = sched_next_df


        # ---- Parsing 'team_stats' --------
        team_stats_json = team_stats # using alias to for consistency

        hit_cols = ['season','G','R','2B','3B','HR','BB','H','HBP','AVG','AB','OBP','SLG','OPS','SB','PA','TB','RBI','sB','AB/HR','CS','SB%','P','LOB','SO','BABIP','GIDP','sF','IBB','GO','AO','GO/AO','CI']
        pit_cols = ['season','G','GS','R','HR','SO','BB','H','HBP','AVG','AB','OBP','ERA','IP','W','L','SV','ER','WHIP','BF','O','GP','CG','ShO','HB','BK','WP','W%','GF','SO:BB','SO/9','BB/9','H/9','R/9','HR/9','sB','sF','IBB','SVO','BS','GO','AO','2B','3B','SLG','OPS','CS','SB','SB%','GIDP','P','HLD','K','K%','PK','TB','GO/AO','P/Inn','CI']
        fld_cols = ['season','A','PO','E','Ch','FLD%','RF/G','RF/9','INNs','G','GS','DP','TP','thE']
        hit_adv_cols = ['season','PA','TB','sB','exBH','HBP','BB/PA','HR/PA','ISO','P','P/PA','LOB','BABIP','SO/PA','BB/SO','GIDP','sF','GIDPO','ROE','WO','FO','TS','Whiffs','BIP','PO','LO','GO','FH','PH','LH','GH']
        pit_adv_cols = ['season','W%','R/9','BF','BABIP','OBP','SO/9','BB/9','HR/9','H/9','SO:BB','GF','WP','BK','BB/PA','SO/PA','HR/PA','BB/SO','SLG','OPS','SB','CS','QS','2B','3B','GIDP','GIDPO','PK','TS','Whiffs','BIP','RS','K%','P/Inn','P/PA','ISO','FO','PO','LO','GO','FH','PH','LH','GH']

        hit_data = []
        hit_adv_data = []
        pitch_data = []
        pitch_adv_data = []
        field_data = []

        for g in team_stats_json.get("stats",[{}]):
            st = g.get("type",{}).get("displayName")
            sg = g.get("group",{}).get("displayName")
            if sg == "hitting" and st == "yearByYear":
                for s in g.get("splits",[{}]):
                    stats = s.get("stat",{})
                    season = s.get("season")
                    stats["season"] = season
                    hit_data.append(stats)

            elif sg == "hitting" and st == "yearByYearAdvanced":
                for s in g.get("splits",[{}]):
                    stats = s.get("stat",{})
                    season = s.get("season")
                    stats["season"] = season
                    hit_adv_data.append(stats)

            elif sg == "pitching" and st == "yearByYear":
                for s in g.get("splits",[{}]):
                    stats = s.get("stat",{})
                    season = s.get("season")
                    stats["season"] = season
                    pitch_data.append(stats)

            elif sg == "pitching" and st == "yearByYearAdvanced":
                for s in g.get("splits",[{}]):
                    stats = s.get("stat",{})
                    season = s.get("season")
                    stats["season"] = season
                    pitch_adv_data.append(stats)

            elif sg == "fielding" and st == "yearByYear":
                for s in g.get("splits",[{}]):
                    stats = s.get("stat",{})
                    season = s.get("season")
                    stats["season"] = season
                    field_data.append(stats)


        hit_df = pd.DataFrame(data=hit_data).rename(columns=STATDICT)[hit_cols].sort_values(by="season",ascending=False)
        hit_adv_df = pd.DataFrame(data=hit_adv_data).rename(columns=STATDICT)[hit_adv_cols].sort_values(by="season",ascending=False)
        pitch_df = pd.DataFrame(data=pitch_data).rename(columns=STATDICT)[pit_cols].sort_values(by="season",ascending=False)
        pitch_adv_df = pd.DataFrame(data=pitch_adv_data).rename(columns=STATDICT)[pit_adv_cols].sort_values(by="season",ascending=False)
        field_df = pd.DataFrame(data=field_data).rename(columns=STATDICT)[fld_cols].sort_values(by="season",ascending=False)

        # ---- Parsing 'all_players' --------

        rost_cols = [
            'mlbam',
            'name',
            'jersey_number',
            'pos',
            'status',
            'status_code']
        rost_data = []

        for p in all_players["roster"]:
            person          = p.get("person")
            mlbam           = person.get("id")
            name            = person.get("fullName")
            jersey_number   = p.get("jerseyNumber")
            position        = p.get("position")
            pos             = position.get("abbreviation")
            status          = p.get("status",{}).get("description")
            status_code     = p.get("status",{}).get("code")
            
            rost_data.append([
                mlbam,
                name,
                jersey_number,
                pos,
                status,
                status_code
            ])
        all_players_df = pd.DataFrame(data=rost_data,columns=rost_cols).sort_values(by="name")


        # ---- Parsing 'hof_players' --------
        hof_data = []

        for a in hof_players["awards"]:
            if str(a.get("team",{}).get("id")) == str(mlbam):
                p = a.get("player")
                hof_data.append([
                    a.get("season"),
                    a.get("date"),
                    p.get("id"),
                    p.get("nameFirstLast"),
                    p.get("primaryPosition",{}).get("abbreviation"),
                    a.get("votes",""),
                    a.get("notes","")
                ])
        hof_df = pd.DataFrame(data=hof_data,columns=['season','date','mlbam','name','pos','votes','notes'])

        # ---- Parsing 'retired_numbers' ----
        retired_numbers_data = []

        for a in retired_numbers["awards"]:
            player = a.get("player",{})
            retired_numbers_data.append([
                a.get("season"),
                a.get("date"),
                a.get("notes"),     # this is the "retired number" value
                player.get("id"),
                player.get("nameFirstLast"),
                player.get("primaryPosition",{}).get("abbreviation",""),
            ])

        retired_numbers_df = pd.DataFrame(data=retired_numbers_data,columns=['season','date','number','mlbam','name','pos'])

        fetched_data = {
            "record_splits":records.reset_index(drop=True),
            "records":standings.reset_index(drop=True),
            "yby_data":yby_data,
            "team_info":team_info_parsed,
            "hitting":hit_df.reset_index(drop=True),
            "hitting_advanced":hit_adv_df.reset_index(drop=True),
            "pitching":pitch_df.reset_index(drop=True),
            "pitching_advanced":pitch_adv_df.reset_index(drop=True),
            "fielding":field_df.reset_index(drop=True),
            "all_players":all_players_df,
            "hof":hof_df,
            "retired_numbers":retired_numbers_df,
            "temp":hof_players,
        }

    return fetched_data

def team_appearances(mlbam):
    gt_types = {'F':'wild_card_series','D':'division_series','L':'league_series','W':'world_series','P':'playoffs'}
    sort_orders = {'F':1,'D':2,'L':3,'W':4}
    with requests.session() as sesh:
        data = []
        for gt in ('F','D','L','W'):
            url = f"https://statsapi.mlb.com/api/v1/teams/{mlbam}/stats?stats=yearByYearPlayoffs&group=pitching&gameType={gt}&fields=stats,splits,stat,wins,losses,season"
            resp = sesh.get(url)
            game_type = gt_types[gt]
            years = resp.json()["stats"][0]["splits"]
            for y in years:
                season = y.get("season","")
                wins = y.get("stat",{}).get("wins",0)
                losses = y.get("stat",{}).get("losses",0)
                if wins > losses:
                    title_winner = True
                else:
                    title_winner = False
                sort_order = sort_orders[gt]
                
                data.append([
                    season,gt,game_type,wins,losses,title_winner,sort_order
                ])
                

                
        df = pd.DataFrame(data=data,columns=['season','gt','game_type','wins','losses','title_winner','sort_order']).sort_values(by=["season","sort_order"],ascending=[True,True]).reset_index(drop=True)
        
        return df


# ===============================================================
# LEAGUE Functions
# ===============================================================
def league_hitting(league="all",**kwargs):
    """Get league hitting stats (team-level)

    Parameters
    ----------
    league : int or str
        Official League MLB ID or abbreviation - AL, NL (not case-sensitive)

    Default 'season' is the current (or most recently completed if in off-season)

    """

    stats = "season"
    statGroup = "hitting"

    if kwargs.get("season") is not None:
        season = kwargs["season"]
    else:
        season = default_season()

    if league == "all":
        leagueIds = "103,104"
    elif str(league).lower() in ("al","american","103"):
        leagueIds = 103
    elif str(league).lower() in ("nl","national","104"):
        leagueIds = 104


    url = BASE + f"/teams/stats?sportId=1&group={statGroup}&stats={stats}&season={season}&leagueIds={leagueIds}&hydrate=team"

    resp = requests.get(url)

    lg_data = resp.json()["stats"][0]

    data = []

    season_col = []
    team_mlbam_col = []
    team_name_col = []
    league_mlbam_col = []
    league_col = []
    div_mlbam_col = []
    div_col = []

    for tm in lg_data["splits"]:
        team_dict = tm.get("team")
        season = tm.get("season")
        team_mlbam = tm.get("team",{}).get("id")
        team_name = tm.get("team",{}).get("name")
        league_mlbam = str(team_dict.get("league",{}).get("id"))
        league = LEAGUE_IDS[league_mlbam]
        div_mlbam = str(team_dict.get("division",{}).get("id"))
        div = LEAGUE_IDS[div_mlbam]

        season_col.append(season)
        team_mlbam_col.append(team_mlbam)
        team_name_col.append(team_name)
        league_mlbam_col.append(league_mlbam)
        league_col.append(league)
        div_mlbam_col.append(div_mlbam)
        div_col.append(div)

        tm_stats = tm.get("stat")

        data.append(pd.Series(tm_stats))
    
    df = pd.DataFrame(data=data,columns=tm_stats.keys()).rename(columns=STATDICT)
    
    df.insert(0,"Season",season_col)
    df.insert(1,"team_mlbam",team_mlbam_col)
    df.insert(2,"team",team_name_col)
    df.insert(3,"league_mlbam",league_mlbam_col)
    df.insert(4,"league",league_col)
    df.insert(5,"div_mlbam",div_mlbam_col)
    df.insert(6,"div",div_col)

    return df

def league_pitching(league="all",**kwargs):
    """Get league pitching stats (team-level)

    Default 'season' is the current (or most recently completed if in off-season)
    """
    stats = "season"
    statGroup = "pitching"

    if kwargs.get("season") is not None:
        season = kwargs["season"]
    else:
        season = default_season()

    # for k in kwargs.keys():
    #     if k in ("game_type","gametypes","game_types","gameTypes","gameType","gametype"):
    #         gameType = kwargs[k]
    #         break

    if league == "all":
        leagueIds = "103,104"
    elif str(league).lower() in ("al","american","103"):
        leagueIds = 103
    elif str(league).lower() in ("nl","national","104"):
        leagueIds = 104


    url = BASE + f"/teams/stats?sportId=1&group={statGroup}&stats={stats}&season={season}&leagueIds={leagueIds}&hydrate=team"

    resp = requests.get(url)

    lg_data = resp.json()["stats"][0]

    data = []

    season_col = []
    team_mlbam_col = []
    team_name_col = []
    league_mlbam_col = []
    league_col = []
    div_mlbam_col = []
    div_col = []

    for tm in lg_data["splits"]:
        team_dict = tm.get("team")
        season = tm.get("season")
        team_mlbam = tm.get("team",{}).get("id")
        team_name = tm.get("team",{}).get("name")
        league_mlbam = str(team_dict.get("league",{}).get("id"))
        league = LEAGUE_IDS[league_mlbam]
        div_mlbam = str(team_dict.get("division",{}).get("id"))
        div = LEAGUE_IDS[div_mlbam]

        season_col.append(season)
        team_mlbam_col.append(team_mlbam)
        team_name_col.append(team_name)
        league_mlbam_col.append(league_mlbam)
        league_col.append(league)
        div_mlbam_col.append(div_mlbam)
        div_col.append(div)

        tm_stats = tm.get("stat")

        data.append(pd.Series(tm_stats))
    
    df = pd.DataFrame(data=data,columns=tm_stats.keys()).rename(columns=STATDICT)
    
    df.insert(0,"Season",season_col)
    df.insert(1,"team_mlbam",team_mlbam_col)
    df.insert(2,"team",team_name_col)
    df.insert(3,"league_mlbam",league_mlbam_col)
    df.insert(4,"league",league_col)
    df.insert(5,"div_mlbam",div_mlbam_col)
    df.insert(6,"div",div_col)

    return df

def league_fielding(league="all",**kwargs):
    """Get league fielding stats (team-level)

    Default 'season' is the current (or most recently completed if in off-season)
    """
    stats = "season"
    statGroup = "fielding"

    if kwargs.get("season") is not None:
        season = kwargs["season"]
    else:
        season = default_season()

    # for k in kwargs.keys():
    #     if k in ("game_type","gametypes","game_types","gameTypes","gameType","gametype"):
    #         gameType = kwargs[k]
    #         break

    if league == "all":
        leagueIds = "103,104"
    elif str(league).lower() in ("al","american","103"):
        leagueIds = 103
    elif str(league).lower() in ("nl","national","104"):
        leagueIds = 104


    url = BASE + f"/teams/stats?sportId=1&group={statGroup}&stats={stats}&season={season}&leagueIds={leagueIds}&hydrate=team"

    resp = requests.get(url)

    lg_data = resp.json()["stats"][0]

    data = []

    season_col = []
    team_mlbam_col = []
    team_name_col = []
    league_mlbam_col = []
    league_col = []
    div_mlbam_col = []
    div_col = []

    for tm in lg_data["splits"]:
        team_dict = tm.get("team")
        season = tm.get("season")
        team_mlbam = tm.get("team",{}).get("id")
        team_name = tm.get("team",{}).get("name")
        league_mlbam = str(team_dict.get("league",{}).get("id"))
        league = LEAGUE_IDS[league_mlbam]
        div_mlbam = str(team_dict.get("division",{}).get("id"))
        div = LEAGUE_IDS[div_mlbam]

        season_col.append(season)
        team_mlbam_col.append(team_mlbam)
        team_name_col.append(team_name)
        league_mlbam_col.append(league_mlbam)
        league_col.append(league)
        div_mlbam_col.append(div_mlbam)
        div_col.append(div)

        tm_stats = tm.get("stat")

        data.append(pd.Series(tm_stats))
    
    df = pd.DataFrame(data=data,columns=tm_stats.keys()).rename(columns=STATDICT)
    
    df.insert(0,"Season",season_col)
    df.insert(1,"team_mlbam",team_mlbam_col)
    df.insert(2,"team",team_name_col)
    df.insert(3,"league_mlbam",league_mlbam_col)
    df.insert(4,"league",league_col)
    df.insert(5,"div_mlbam",div_mlbam_col)
    df.insert(6,"div",div_col)

    return df

def league_hitting_advanced(league='all',**kwargs):
    """Get advanced league hitting stats (team-level)

    Default 'season' is the current (or most recently completed if in off-season)
    """
    stats = "seasonAdvanced"
    statGroup = "hitting"

    if kwargs.get("season") is not None:
        season = kwargs["season"]
    else:
        season = default_season()

    # for k in kwargs.keys():
    #     if k in ("game_type","gametypes","game_types","gameTypes","gameType","gametype"):
    #         gameType = kwargs[k]
    #         break

    if league == "all":
        leagueIds = "103,104"
    elif str(league).lower() in ("al","american","103"):
        leagueIds = 103
    elif str(league).lower() in ("nl","national","104"):
        leagueIds = 104


    url = BASE + f"/teams/stats?sportId=1&group={statGroup}&stats={stats}&season={season}&leagueIds={leagueIds}&hydrate=team"

    resp = requests.get(url)

    lg_data = resp.json()["stats"][0]

    data = []

    season_col = []
    team_mlbam_col = []
    team_name_col = []
    league_mlbam_col = []
    league_col = []
    div_mlbam_col = []
    div_col = []

    for tm in lg_data["splits"]:
        team_dict = tm.get("team")
        season = tm.get("season")
        team_mlbam = tm.get("team",{}).get("id")
        team_name = tm.get("team",{}).get("name")
        league_mlbam = str(team_dict.get("league",{}).get("id"))
        league = LEAGUE_IDS[league_mlbam]
        div_mlbam = str(team_dict.get("division",{}).get("id"))
        div = LEAGUE_IDS[div_mlbam]

        season_col.append(season)
        team_mlbam_col.append(team_mlbam)
        team_name_col.append(team_name)
        league_mlbam_col.append(league_mlbam)
        league_col.append(league)
        div_mlbam_col.append(div_mlbam)
        div_col.append(div)

        tm_stats = tm.get("stat")

        data.append(pd.Series(tm_stats))
    
    df = pd.DataFrame(data=data,columns=tm_stats.keys()).rename(columns=STATDICT)
    
    df.insert(0,"Season",season_col)
    df.insert(1,"team_mlbam",team_mlbam_col)
    df.insert(2,"team",team_name_col)
    df.insert(3,"league_mlbam",league_mlbam_col)
    df.insert(4,"league",league_col)
    df.insert(5,"div_mlbam",div_mlbam_col)
    df.insert(6,"div",div_col)

    return df

def league_pitching_advanced(league='all',**kwargs):
    """Get advanced league pitching stats (team-level)

    Default 'season' is the current (or most recently completed if in off-season)
    """
    stats = "seasonAdvanced"
    statGroup = "pitching"

    if kwargs.get("season") is not None:
        season = kwargs["season"]
    else:
        season = default_season()

    # for k in kwargs.keys():
    #     if k in ("game_type","gametypes","game_types","gameTypes","gameType","gametype"):
    #         gameType = kwargs[k]
    #         break

    if league == "all":
        leagueIds = "103,104"
    elif str(league).lower() in ("al","american","103"):
        leagueIds = 103
    elif str(league).lower() in ("nl","national","104"):
        leagueIds = 104


    url = BASE + f"/teams/stats?sportId=1&group={statGroup}&stats={stats}&season={season}&leagueIds={leagueIds}&hydrate=team"

    resp = requests.get(url)

    lg_data = resp.json()["stats"][0]

    data = []

    season_col = []
    team_mlbam_col = []
    team_name_col = []
    league_mlbam_col = []
    league_col = []
    div_mlbam_col = []
    div_col = []

    for tm in lg_data["splits"]:
        team_dict = tm.get("team")
        season = tm.get("season")
        team_mlbam = tm.get("team",{}).get("id")
        team_name = tm.get("team",{}).get("name")
        league_mlbam = str(team_dict.get("league",{}).get("id"))
        league = LEAGUE_IDS[league_mlbam]
        div_mlbam = str(team_dict.get("division",{}).get("id"))
        div = LEAGUE_IDS[div_mlbam]

        season_col.append(season)
        team_mlbam_col.append(team_mlbam)
        team_name_col.append(team_name)
        league_mlbam_col.append(league_mlbam)
        league_col.append(league)
        div_mlbam_col.append(div_mlbam)
        div_col.append(div)

        tm_stats = tm.get("stat")

        data.append(pd.Series(tm_stats))
    
    df = pd.DataFrame(data=data,columns=tm_stats.keys()).rename(columns=STATDICT)
    
    df.insert(0,"Season",season_col)
    df.insert(1,"team_mlbam",team_mlbam_col)
    df.insert(2,"team",team_name_col)
    df.insert(3,"league_mlbam",league_mlbam_col)
    df.insert(4,"league",league_col)
    df.insert(5,"div_mlbam",div_mlbam_col)
    df.insert(6,"div",div_col)

    return df

def league_leaders(season=None,statGroup=None,playerPool="Qualified"):
    """Get league leaders for hitting & pitching

    season : int or str
        season/year ID to get stats

    statGroup : str
        stat group(s) to retrieve stats for (hitting, pitching, fielding, etc.)

    playerPool : str
        filter results by pool
            - "All"
            - "Qualified"
            - "Rookies"
            - "Qualified_rookies"
    
    """
    if season is None:
        season = default_season()

    if statGroup is None:
        statGroup = 'hitting,pitching'

    url = BASE + f"/stats?stats=season&season={season}&group={statGroup}&playerPool={playerPool}"

    resp = requests.get(url)

    resp_json = resp.json()

    hit_cols = ['rank','season','position','player_mlbam','player_name','team_mlbam','team_name','league_mlbam','league_name','G','GO','AO','R','2B','3B','HR','SO','BB','IBB','H','HBP','AVG','AB','OBP','SLG','OPS','CS','SB','SB%','GIDP','P','PA','TB','RBI','LOB','sB','sF','BABIP','GO/AO','CI','AB/HR']
    pit_cols = ['rank','season','position','player_mlbam','player_name','team_mlbam','team_name','league_mlbam','league_name','G','GS','GO','AO','R','2B','3B','HR','SO','BB','IBB','H','HBP','AVG','AB','OBP','SLG','OPS','CS','SB','SB%','GIDP','P','ERA','IP','W','L','SV','SVO','HLD','BS','ER','WHIP','BF','O','GP','CG','ShO','K','K%','HB','BK','WP','PK','TB','GO/AO','W%','P/Inn','GF','SO:BB','SO/9','BB/9','H/9','R/9','HR/9','IR','IRS','CI','sB','sF']

    top_hitters = []
    top_pitchers = []

    for g in resp_json['stats']:
        sg = g.get("group",{}).get("displayName")
        # st = g.get("type",{}).get("displayName")
        if sg == "hitting":
            for s in g.get("splits",[{}]):
                stats = s.get("stat")

                rank = s.get('rank')
                season = s.get("season",'-')
                player = s.get("player",{})
                player_mlbam = player.get("id","")
                player_name = player.get("fullName","")
                team = s.get("team",{})
                team_mlbam = team.get("id","")
                team_name = team.get("name","")
                league = s.get("league",{})
                league_mlbam = league.get("id","")
                league_name = league.get("name","")
                position = s.get("position",{}).get("abbreviation",'-')

                stats["rank"] = rank
                stats["season"] = season
                stats["position"] = position
                stats["player_mlbam"] = player_mlbam
                stats["player_name"] = player_name
                stats["team_mlbam"] = team_mlbam
                stats["team_name"] = team_name
                stats["league_mlbam"] = league_mlbam
                stats["league_name"] = league_name
                
                top_hitters.append(pd.Series(stats))

        elif sg == "pitching":
            for s in g.get("splits",[{}]):
                stats = s.get("stat")

                rank = s.get('rank')
                season = s.get("season",'-')
                player = s.get("player",{})
                player_mlbam = player.get("id","")
                player_name = player.get("fullName","")
                team = s.get("team",{})
                team_mlbam = team.get("id","")
                team_name = team.get("name","")
                league = s.get("league",{})
                league_mlbam = league.get("id","")
                league_name = league.get("name","")
                position = s.get("position",{}).get("abbreviation",'-')

                stats["rank"] = rank
                stats["season"] = season
                stats["position"] = position
                stats["player_mlbam"] = player_mlbam
                stats["player_name"] = player_name
                stats["team_mlbam"] = team_mlbam
                stats["team_name"] = team_name
                stats["league_mlbam"] = league_mlbam
                stats["league_name"] = league_name
                
                top_pitchers.append(pd.Series(stats))

    hit_df = pd.DataFrame(data=top_hitters).rename(columns=STATDICT)[hit_cols]
    pitch_df = pd.DataFrame(data=top_pitchers).rename(columns=STATDICT)[pit_cols]

    return {"hitting":hit_df,"pitching":pitch_df}

# ===============================================================
# MISC Functions
# ===============================================================
def find_team(query,season=None):
    """Search for teams by name.

    Paramaters
    ----------
    query : str
        keywords to search for in the 'teams' data (e.g. "white sox" or "Philadelphia")

    season : int or str, optional
        filter results by season

    season = 2005 -> return results from 2005
    season = None -> return results from the season in progress or the last one completed (Default)
    season = "all" -> return results from all seasons (season filter not applied)
    
    """

    df = get_teams_df()

    if season is None:
        season = default_season()
        df = df[df["yearID"]==int(season)]
    elif season == 'all':
        pass
    else:
        df = df[df["yearID"]==int(season)]



    query = query.lower()

    df['fn'] = df['fullName'].str.lower()

    df = df[df['fn'].str.contains(query)]

    return df.drop(columns="fn")

def find_player(query):
    """Search for players by name

    Paramaters
    ----------
    query : str
        keywords to search for in the 'people' data (e.g. "Jose Abre")

    """

    df = get_bios_df()

    query = query.lower()

    df['nml'] = df['name'].str.lower()

    df = df[df['nml'].str.contains(query)]

    return df.drop(columns="nml").sort_values(by='lastGame',ascending=False)

def find_venue(query):
    """Search for venues by name

    Paramaters
    ----------
    query : str
        keywords to search for in the 'venues' data (e.g. "Comiskey Park")

    """

    df = get_venues_df()

    query = query.lower()

    df['vname'] = df['name'].str.lower()

    df = df[df['vname'].str.contains(query)]

    return df.drop(columns="vname")
  
def leaderboards(tm_mlbam=None,league_mlbam=None,season=None,gameTypes=None,sitCodes=None,limit=None,startDate=None,endDate=None,group_by_team=False):
    """Get Leaderboard stats

    tm_mlbam : str or int
        official team MLB ID

    season : str or int, optional (the default is the current season or the most recently completed if in the off-season)
        the specific season to get stats for

    lg_mlbam : str or int
        official league MLB ID
    
    NOTE: statTypes: season, statsSingleSeason, byDateRange
    
    """

    if season is None:
        season = default_season()

    data = get_leaders(tm_mlbam,league_mlbam,season,gameTypes,sitCodes,limit,startDate,endDate,group_by_team)
    return data

def get_matchup_stats(batter,pitcher,count="",outs="",inning="",runners_on=[],tm_hitting="",tm_pitching="",score_tm_hitting="",score_tm_pitching="",batter_stands="",pitcher_throws="",bat_order_pos="",isLeadoff=False,pitch_num="",isFirstIP=False):
    # batter=547989 (Jose Abreu),pitcher=453562 (Jake Arietta)
    # EDIT TERMINAL INPUT HERE: 
    # batter=547989,batter_stands="right",bat_order_pos="2",isLeadoff=True,pitcher=453562,pitcher_throws="left",count="01",outs="1",inning="5",runners_on=[3],tm_hitting=145,tm_pitching=112,score_tm_hitting="3",score_tm_pitching="3",pitch_num=57
    """Retrieve batter's and pitcher's splits for the current matchup situation

    'batter' and 'pitcher' values should be their respective mlbam IDs
    """
   #
    # For BATTER: "statSplits" (for given situation),"careerStatSplits","vsTeam" (totals/current season), "vsPlayer"
    # relevant HITTING sitCodes -> h,a,vl,vr,sah,sbh,sti,b[1-9]/lo,ph,i[01-09/ix],e/ron/r[0,123,1,12,13,2,23,3],ron2,risp,risp2,o[0-2],fp/ac/bc/ec/2s/fc/c[00,01,02,10,11,12,20,21,22,30,31,32],zn[01-14]
    #       pitchTypes & sitCodes -> 
    #       FASTBALL/pfa, FOUR-SEAM_FB/pff, TWO-SEAM_FB/pft, CUTTER/pfc, SINKER/psi, SPLITTER/pfs, FORKBALL/pfo, SLIDER/psl,
    #       CURVEBALL/pcu, KNUCKLE-CURVE/pkc, SCREWBALL/psc, GYROBALL/pgy, CHANGEUP/pch, KNUCKLE/pkn, EEPHUS/pep
    #       (could also maybe add splits for "vsLeft/Right and zone","outs and zone"/"count & zone"/"runnersOn & zone" e.g. - o107 = 1 out Zone 7) 
    #           --  MAYBE SHOW IN "MATCHUP GRAPHIC"! (put "AVG." stat in each respective zone!!!)
    #           -- FORMATS: o107, ec01/ac01/bc/01, r001 (bases empty zone 1), r210 (runner on zone 10), rsp08 (risp zone 08)
    #           -- PRIORITY ORDER: "vl/vr", count&zone, outs&zone, runnersOn&zone
    # 
    # For PITCHER: mostly the same, but might add a few more -> pitch count (grouped), fip (first inning pitched)
    # 
    # **In an effort to not clutter the screen, USER should have option to select on the front-end which stat is being displayed (maybe have a couple defaults?)
   # 
    if score_tm_hitting == "":score_tm_hitting = 0
    else: score_tm_hitting = int(score_tm_hitting)

    if score_tm_pitching == "":score_tm_pitching = 0
    else: score_tm_pitching = int(score_tm_pitching)

    count_balls = int(count[0])
    count_strikes = int(count[-1])
    outs = int(outs)
    inning = int(inning)
    runners_on = runners_on # need to figure out how I'll be retrieving this
    # score_tm_hitting
    # score_tm_pitching
    # batter_stands - hopefully available on game page -- you could ALSO retrieve this by getting ALL the data first & sorting it out after
    # pitcher_throws - hopefully available on game page -- you could ALSO retrieve this by getting ALL the data first & sorting it out after

    zonesList = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14']

   # SETTING UP VALUES FOR 'batCodes' ==========================================
    batCodes = [] # ---- 'batCodes' defined ----

    # ASSIGNING "batCodes" (sitCodes) to retrieve for BATTER
    
    if "l" in pitcher_throws.lower():
        batCodes.append("vl")
    elif "r" in pitcher_throws.lower():
        batCodes.append("vr")


    cCount = f"c{count_balls}{count_strikes}"
    cOuts = f"o{outs}"
    cInns = f"i0{inning}"
    cBatOrder = f"b{bat_order_pos}"

    # Outs Code
    if outs == 3:pass
    else:
        batCodes.append(cOuts)

    # Inning Code
    if inning <= 9:
        batCodes.append(cInns)
    else:batCodes.append("ix")

    # Batting Order Code
    if isLeadoff is True:
        batCodes.append("lo")
    batCodes.append(cBatOrder)

    # Team Ahead/Behind Code
    if score_tm_hitting > score_tm_pitching:
        batCodes.append("sah")
    elif score_tm_hitting < score_tm_pitching:
        batCodes.append("sbh")
    else:
        batCodes.append("sti")

    # Count Codes
    if count_strikes == 2:
        batCodes.append("2s")
    if count_balls == count_strikes and cCount != "c00": # even count (NOT first pitch)
        batCodes.append("ec")
        batCodes.append(cCount)
    elif count_balls > count_strikes and cCount != "c32": # batter ahead (NOT a full count)
        batCodes.append("ac")
        batCodes.append(cCount)
    elif count_balls < count_strikes: # batter behind
        batCodes.append("bc")
        batCodes.append(cCount)
    elif count_balls == 3 & count_strikes == 2: # full count
        batCodes.append("fc")
    elif count_balls == 0 & count_strikes == 0: # first pitch
        batCodes.append("fp")

    
    # Runners-On Codes
    if len(runners_on) == 0:                        # bases empty
        batCodes.append("r0")
    if len(runners_on) > 0:                         # runner(s) on
        batCodes.append("ron")
        if outs == 2:
            batCodes.append("ron2")                 # runner(s) on (2 outs)
    if 2 in runners_on or 3 in runners_on:          # runner(s) in scoring position
        batCodes.append("risp")
        if outs == 2:                               # runner(s) in scoring position (2 outs)
            batCodes.append("risp2")

    if len(runners_on) == 1:    # (ONE base occupied)
        if 1 in runners_on:                         # runner on 1st
            batCodes.append("r1")
        elif 2 in runners_on:                       # runner on 2nd
            batCodes.append("r2")
        elif 3 in runners_on:                       # runner on 3rd
            batCodes.append("r3")
    elif len(runners_on) == 2:  # (TWO bases occupied)
        if 1 in runners_on and 2 in runners_on:     # runners on 1st and 2nd
            batCodes.append("r12")
        elif 1 in runners_on and 3 in runners_on:   # runners on 1st and 3rd
            batCodes.append("r13")
        elif 2 in runners_on and 3 in runners_on:   # runners on 2nd and 3rd
            batCodes.append("r23")
    elif len(runners_on) == 3:                      # runners on 1st, 2nd, and 3rd (BASES LOADED)
        batCodes.append("r123")

    # ---- ZONE Codes ----

    # All Zones
    for znLoc in zonesList:
        batCodes.append(f"zn{znLoc}")

    # Outs
    for out_num in [0,1,2]: # add zone split for each "out count"
        for znLoc in zonesList:
            batCodes.append(f"o{out_num}{znLoc}")

    # Count situation (ahead, behind, and even)
    for znLoc in zonesList:
        batCodes.append(f"ac{znLoc}")
        batCodes.append(f"bc{znLoc}")
        batCodes.append(f"ec{znLoc}")
    
    # vsRHP/LHP
    for znLoc in zonesList:
        batCodes.append(f"vr{znLoc}")
        batCodes.append(f"vl{znLoc}")

    # Current Bases occupied
    if "r0" in batCodes:
        for znLoc in zonesList:
            batCodes.append(f"r0{znLoc}")
    if "ron" in batCodes:
        for znLoc in zonesList:
            batCodes.append(f"ron{znLoc}")
    if "risp" in batCodes:
        for znLoc in zonesList:
            batCodes.append(f"rsp{znLoc}")
    if "r123" in batCodes:
        for znLoc in zonesList:
            batCodes.append(f"rbl{znLoc}")

    batCodes_str = ",".join(batCodes)
   #

   # SETTING UP VALUES FOR 'pitchCodes' ==========================================
    if pitch_num == "":pitch_num = 0
    else: pitch_num = int(pitch_num)
    pitchCodes = [] # ---- 'pitchCodes' defined ----

    # ASSIGNING "pitchCodes" (sitCodes) to retrieve for BATTER
    
    if "l" in batter_stands.lower():
        pitchCodes.append("vl")
    elif "r" in batter_stands.lower():
        pitchCodes.append("vr")

    # ALREADY DEFINED ABOVE (HERE FOR REFERENCE):
    # cCount = f"c{count_balls}{count_strikes}"
    # cOuts = f"o{outs}"
    # cInns = f"i0{inning}"
    # cBatOrder = f"b{bat_order_pos}"

    # Outs Code
    if outs == 3:pass
    else:
        pitchCodes.append(cOuts)

    # Inning Code
    if inning <= 9:
        pitchCodes.append(cInns)
    else:pitchCodes.append("ix")

    if isFirstIP is True: # if this is pitcher's first inning of the game (ONLY FOR PITCHER)
        pitchCodes.append("fip")

    # Batting Order Code
    if isLeadoff is True:
        pitchCodes.append("lo")
    pitchCodes.append(cBatOrder)

    # Team Ahead/Behind Code
    if score_tm_pitching > score_tm_hitting:
        pitchCodes.append("sah")
    elif score_tm_pitching < score_tm_hitting:
        pitchCodes.append("sbh")
    else:
        pitchCodes.append("sti")

    # Count Codes
    if count_strikes == 2:
        pitchCodes.append("2s")
    if count_balls == count_strikes and cCount != "c00": # even count (NOT first pitch)
        pitchCodes.append("ec")
        pitchCodes.append(cCount)
    elif count_balls < count_strikes and cCount != "c32": # pitcher ahead
        pitchCodes.append("ac")
        pitchCodes.append(cCount)
    elif count_balls > count_strikes: # pitcher behind
        pitchCodes.append("bc")
        pitchCodes.append(cCount)
    elif count_balls == 3 & count_strikes == 2: # full count
        pitchCodes.append("fc")
    elif count_balls == 0 & count_strikes == 0: # first pitch
        pitchCodes.append("fp")

    
    # Runners-On Codes
    if len(runners_on) == 0:                        # bases empty
        pitchCodes.append("r0")
    if len(runners_on) > 0:                         # runner(s) on
        pitchCodes.append("ron")
        if outs == 2:
            pitchCodes.append("ron2")                 # runner(s) on (2 outs)
    if 2 in runners_on or 3 in runners_on:          # runner(s) in scoring position
        pitchCodes.append("risp")
        if outs == 2:                               # runner(s) in scoring position (2 outs)
            pitchCodes.append("risp2")

    if len(runners_on) == 1:    # (ONE base occupied)
        if 1 in runners_on:                         # runner on 1st
            pitchCodes.append("r1")
        elif 2 in runners_on:                       # runner on 2nd
            pitchCodes.append("r2")
        elif 3 in runners_on:                       # runner on 3rd
            pitchCodes.append("r3")
    elif len(runners_on) == 2:  # (TWO bases occupied)
        if 1 in runners_on and 2 in runners_on:     # runners on 1st and 2nd
            pitchCodes.append("r12")
        elif 1 in runners_on and 3 in runners_on:   # runners on 1st and 3rd
            pitchCodes.append("r13")
        elif 2 in runners_on and 3 in runners_on:   # runners on 2nd and 3rd
            pitchCodes.append("r23")
    elif len(runners_on) == 3:                      # runners on 1st, 2nd, and 3rd (BASES LOADED)
        pitchCodes.append("r123")

    # Pitch Count Code (ONLY FOR PITCHER)
    if pitch_num <= 15:
        pitchCodes.append("pi001")
    elif 16 <= pitch_num <= 30:
        pitchCodes.append("pi016")
    elif 31 <= pitch_num <= 45:
        pitchCodes.append("pi031")
    elif 46 <= pitch_num <= 60:
        pitchCodes.append("pi046")
    elif 61 <= pitch_num <= 75:
        pitchCodes.append("pi061")
    elif 76 <= pitch_num <= 90:
        pitchCodes.append("pi076")
    elif 91 <= pitch_num <= 105:
        pitchCodes.append("pi091")
    elif 106 <= pitch_num <= 120:
        pitchCodes.append("pi106")
    elif pitch_num > 120:
        pitchCodes.append("pi121")

    # --- ZONE Codes ---

    # All Zones
    for znLoc in zonesList:
        pitchCodes.append(f"zn{znLoc}")

    # Outs
    for out_num in [0,1,2]: # add zone split for each "out count"
        for znLoc in zonesList:
            pitchCodes.append(f"o{out_num}{znLoc}")

    # Count situation (ahead, behind, and even)
    for znLoc in zonesList:
        pitchCodes.append(f"ac{znLoc}")
        pitchCodes.append(f"bc{znLoc}")
        pitchCodes.append(f"ec{znLoc}")
    
    # vsRHP/LHP
    for znLoc in zonesList:
        pitchCodes.append(f"vr{znLoc}")
        pitchCodes.append(f"vl{znLoc}")

    # Current Bases occupied
    if "r0" in pitchCodes:
        for znLoc in zonesList:
            pitchCodes.append(f"r0{znLoc}")
    if "ron" in pitchCodes:
        for znLoc in zonesList:
            pitchCodes.append(f"ron{znLoc}")
    if "risp" in pitchCodes:
        for znLoc in zonesList:
            pitchCodes.append(f"rsp{znLoc}")
    if "r123" in pitchCodes:
        for znLoc in zonesList:
            pitchCodes.append(f"rbl{znLoc}")

    # Pitch Types (ONLY FOR PITCHER) - NEED PITCH ARSENAL


    pitchCodes_str = ",".join(pitchCodes)
   #


    statTypes = "careerStatSplits,statSplits,vsPlayer"

    # statTypes = "careerStatSplits,statSplits,vsTeam,vsPlayer" # "vsTeam" will display both 'vsTeam' AND 'vsTeamTotal'
    # 'vsTeam' taking a back seat for now
    # batter_url = BASE + f"/people?personIds={batter}&hydrate=stats(type=[{statTypes}],group=[hitting],sitCodes=[{batCodes}],opposingTeamId={tm_pitching},opposingPlayerId={pitcher})"
    batter_url = BASE + f"/people?personIds={batter}&hydrate=stats(type=[{statTypes}],group=[hitting],sitCodes=[{batCodes_str}],opposingTeamId={tm_pitching},opposingPlayerId={pitcher})"
    pitcher_url = BASE + f"/people?personIds={pitcher}&hydrate=stats(type=[{statTypes}],group=[pitching],sitCodes=[{pitchCodes_str}],opposingTeamId={tm_hitting},opposingPlayerId={batter})"

    with requests.Session() as sesh:
        print(batter_url)
        print(pitcher_url)
        batter_resp = sesh.get(batter_url).json()
        pitcher_resp = sesh.get(pitcher_url).json()

    batter_info = batter_resp["people"][0]
    pitcher_info = pitcher_resp["people"][0]

    hit_stats = {
        "category":"",
        "groundOuts":"",
        "airOuts":"",
        "runs":"",
        "homeRuns":"",
        "strikeOuts":"",
        "baseOnBalls":"",
        "intentionalWalks":"",
        "hits":"",
        "avg":"",
        "atBats":"",
        "obp":"",
        "plateAppearances":"",
        "totalBases":"",
        "rbi":"",
        "sacBunts":"",
        "sacFlies":""
        }

    h_dict = {
        "name":"",
        "bats":"",
        "careerSplits":{
            "outs":{},
            "inning":{},
            "count":{},
            "runners_on":{},
            "score_sit":{},
            "batting_order":{},
            "zones":{
                "all":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                },
                "outs":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                },
                "count":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                },
                "throws":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                },
                "runners":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                    },
                
                "pitch_types":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                    }
                },
            },
        "seasonSplits":{
            "outs":{},
            "inning":{},
            "count":{},
            "runners_on":{},
            "score_sit":{},
            "batting_order":{},
            "zones":{
                "all":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                },
                "outs":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                },
                "count":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                },
                "throws":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                },
                "runners":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                    },
                
                "pitch_types":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                    }
                },
            },
        "vsPlayerTotal":{
            "outs":{},
            "inning":{},
            "count":{},
            "runners_on":{},
            "score_sit":{},
            "batting_order":{}
            },
        "vsTeam":{
            "outs":{},
            "inning":{},
            "count":{},
            "runners_on":{},
            "score_sit":{},
            "batting_order":{}
            },
        "vsTeamTotal":{
            "outs":{},
            "inning":{},
            "count":{},
            "runners_on":{},
            "score_sit":{},
            "batting_order":{}
            }
        }
    
    out_codes = ["o0","o1","o2"]
    inning_codes = ['i01','i02','i03','i04','i05','i06','i07','i08','i09','ix']
    count_codes = ['fp','ac','bc','ec','2s','fc','c00','c01','c02','c10','c11','c12','c20','c21','c22','c30','c31','c32']
    runnersOn_codes = ['e','r0','r123','r1','r12','r13','r2','r23','r3','ron','ron2','risp','risp2']
    scoreSituation_codes = ['sah','sbh','sti']
    battingOrder_codes = ['b1','b2','b3','b4','b5','b6','b7','b8','b9','lo']
    zone_codes = []
    # Parse BATTER response

    for stat_group in batter_info["stats"]:
        statType = stat_group["type"]["displayName"]
        if statType == "careerStatSplits":

            for split in stat_group["splits"]:
                split_desc = split["split"]["description"]
                split_code = split["split"]["code"]
                stat_data = split["stat"]

                code = split_code

                key_label = f"{split_desc} ({code})"

                if code in out_codes:
                    if key_label not in h_dict["careerSplits"]["outs"].keys():
                        h_dict["careerSplits"]["outs"][key_label] = {}
                    for f in list(hit_stats.keys())[1:]:
                        h_dict["careerSplits"]["outs"][key_label][f] = stat_data.get(f,"--")
                elif code in inning_codes:
                    if key_label not in h_dict["careerSplits"]["inning"].keys():
                        h_dict["careerSplits"]["inning"][key_label] = {}
                    for f in list(hit_stats.keys())[1:]:
                        h_dict["careerSplits"]["inning"][key_label][f] = stat_data.get(f,"--")
                elif code in runnersOn_codes:
                    if key_label not in h_dict["careerSplits"]["runners_on"].keys():
                        h_dict["careerSplits"]["runners_on"][key_label] = {}
                    for f in list(hit_stats.keys())[1:]:
                        h_dict["careerSplits"]["runners_on"][key_label][f] = stat_data.get(f,"--")
                elif code in count_codes:
                    if key_label not in h_dict["careerSplits"]["count"].keys():
                        h_dict["careerSplits"]["count"][key_label] = {}
                    for f in list(hit_stats.keys())[1:]:
                        h_dict["careerSplits"]["count"][key_label][f] = stat_data.get(f,"--")
                elif code in scoreSituation_codes:
                    if key_label not in h_dict["careerSplits"]["score_sit"].keys():
                        h_dict["careerSplits"]["score_sit"][key_label] = {}
                    for f in list(hit_stats.keys())[1:]:
                        h_dict["careerSplits"]["score_sit"][key_label][f] = stat_data.get(f,"--")
                elif code in battingOrder_codes:
                    if key_label not in h_dict["careerSplits"]["batting_order"].keys():
                        h_dict["careerSplits"]["batting_order"][key_label] = {}
                    for f in list(hit_stats.keys())[1:]:
                        h_dict["careerSplits"]["batting_order"][key_label][f] = stat_data.get(f,"--")
                elif code in zone_codes:
                    if key_label not in h_dict["careerSplits"]["zones"].keys():
                        h_dict["careerSplits"]["zones"][key_label] = {}
                    for f in list(hit_stats.keys())[1:]:
                        h_dict["careerSplits"]["zones"][key_label][f] = stat_data.get(f,"--")

        if statType == "statSplits":

            for split in stat_group["splits"]:
                split_desc = split["split"]["description"]
                split_code = split["split"]["code"]
                stat_data = split["stat"]

                code = split_code

                key_label = f"{split_desc} ({code})"

                if code in out_codes:
                    if key_label not in h_dict["seasonSplits"]["outs"].keys():
                        h_dict["seasonSplits"]["outs"][key_label] = {}
                    for f in list(hit_stats.keys())[1:]:
                        h_dict["seasonSplits"]["outs"][key_label][f] = stat_data.get(f,"--")
                elif code in inning_codes:
                    if key_label not in h_dict["seasonSplits"]["inning"].keys():
                        h_dict["seasonSplits"]["inning"][key_label] = {}
                    for f in list(hit_stats.keys())[1:]:
                        h_dict["seasonSplits"]["inning"][key_label][f] = stat_data.get(f,"--")
                elif code in runnersOn_codes:
                    if key_label not in h_dict["seasonSplits"]["runners_on"].keys():
                        h_dict["seasonSplits"]["runners_on"][key_label] = {}
                    for f in list(hit_stats.keys())[1:]:
                        h_dict["seasonSplits"]["runners_on"][key_label][f] = stat_data.get(f,"--")
                elif code in count_codes:
                    if key_label not in h_dict["seasonSplits"]["count"].keys():
                        h_dict["seasonSplits"]["count"][key_label] = {}
                    for f in list(hit_stats.keys())[1:]:
                        h_dict["seasonSplits"]["count"][key_label][f] = stat_data.get(f,"--")
                elif code in scoreSituation_codes:
                    if key_label not in h_dict["seasonSplits"]["score_sit"].keys():
                        h_dict["seasonSplits"]["score_sit"][key_label] = {}
                    for f in list(hit_stats.keys())[1:]:
                        h_dict["seasonSplits"]["score_sit"][key_label][f] = stat_data.get(f,"--")
                elif code in battingOrder_codes:
                    if key_label not in h_dict["seasonSplits"]["batting_order"].keys():
                        h_dict["seasonSplits"]["batting_order"][key_label] = {}
                    for f in list(hit_stats.keys())[1:]:
                        h_dict["seasonSplits"]["batting_order"][key_label][f] = stat_data.get(f,"--")
                elif code in zone_codes:
                    if key_label not in h_dict["seasonSplits"]["zones"].keys():
                        h_dict["seasonSplits"]["zones"][key_label] = {}
                    for f in list(hit_stats.keys())[1:]:
                        h_dict["seasonSplits"]["zones"][key_label][f] = stat_data.get(f,"--")             


    pitch_stats = {
        "category":"",
        "groundOuts":"",
        "airOuts":"",
        "runs":"",
        "homeRuns":"",
        "strikeOuts":"",
        "baseOnBalls":"",
        "intentionalWalks":"",
        "hits":"",
        "atBats":"",
        "battersFaced":"",
        "hitBatsmen":"",
        "totalBases":"",
        "rbi":"",
        "pickoffs":"",
        "wildPitches":"",
        "sacBunts":"",
        "sacFlies":""}
    
    p_dict = {
        "name":"",
        "throws":"",
        "careerSplits":{
            "outs":pitch_stats,
            "inning":pitch_stats,
            "count":pitch_stats,
            "runners_on":pitch_stats,
            "score_sit":pitch_stats,
            "batting_order":pitch_stats,
            "zones":{
                "all":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                },
                "outs":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                },
                "count":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                },
                "throws":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                },
                "runners":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                    },
                
                "pitch_types":{
                    "zn01":{},
                    "zn02":{},
                    "zn03":{},
                    "zn04":{},
                    "zn05":{},
                    "zn06":{},
                    "zn07":{},
                    "zn08":{},
                    "zn09":{},
                    "zn10":{},
                    "zn11":{},
                    "zn12":{},
                    "zn13":{},
                    "zn14":{}
                    }
                },
            },
        "vsPlayerTotal":{
            "outs":pitch_stats,
            "inning":pitch_stats,
            "count":pitch_stats,
            "runners_on":pitch_stats,
            "score_sit":pitch_stats,
            "batting_order":pitch_stats
            },
        "vsTeam":{
            "outs":pitch_stats,
            "inning":pitch_stats,
            "count":pitch_stats,
            "runners_on":pitch_stats,
            "score_sit":pitch_stats,
            "batting_order":pitch_stats
            },
        "vsTeamTotal":{
            "outs":pitch_stats,
            "inning":pitch_stats,
            "count":pitch_stats,
            "runners_on":pitch_stats,
            "score_sit":pitch_stats,
            "batting_order":pitch_stats
            }
        }

    # Parse PITCHER response
    return h_dict

def play_search(mlbam,seasons=None,statGroup=None,opposingTeamId=None,eventTypes=None,pitchTypes=None,gameTypes=None,**kwargs):
    """Search for any individual play 2008 and later

    Parameters
    ----------
    mlbam : str or int
        player's official MLB ID
    
    seasons : str or int, optional
        filter by season

    statGroup : str, optional
        filter results by stat group ('hitting','pitching','fielding')

    eventTypes : str, optional
        type of play events to filter

    pitchTypes : str, optional
        type of play events to filter

    gameType : str, optional
        filter results by game type

    """

    # Allows the user to lookup "playLog" or "pitchLog". URL is flexible since both statTypes are very similar
    if kwargs.get("statType","playLog").lower() == "pitchlog":
        statType = "pitchLog"
    else:
        statType = "playLog"

    if kwargs.get("season") is not None: seasons = kwargs["season"]
    if kwargs.get("group") is not None: statGroup = kwargs["group"]
    elif kwargs.get("groups") is not None: statGroup = kwargs["groups"]
    elif kwargs.get("statGroups") is not None: statGroup = kwargs["statGroups"]
    if kwargs.get("opposingTeamIds") is not None: opposingTeamId = kwargs["opposingTeamIds"]
    if kwargs.get("eventType") is not None: eventTypes = kwargs["eventType"]
    if kwargs.get("pitchType") is not None: pitchTypes = kwargs["pitchType"]
    if kwargs.get("gameType") is not None: gameTypes = kwargs["gameType"]

    if seasons is None:
        seasons = default_season()

    params = {
        "stats":statType,
        "seasons":seasons,
        "hydrate":"hitData,pitchData"
    }

    if opposingTeamId is not None:
        params["opposingTeamId"] = opposingTeamId

    if eventTypes is not None:
        params["eventType"] = eventTypes

    if pitchTypes is not None:
        params["pitchType"] = pitchTypes

    if gameTypes is not None:
        params["gameType"] = gameTypes

    if statGroup is not None:
        params["group"] = statGroup
    

    url = BASE + f"/people/{mlbam}/stats"
    response = requests.get(url,params=params)
    resp = response.json()

    if kwargs.get("_log") is True:
        print("\nREQUEST URL:\n")
        print(response.url)
        print("----------------------------\n")

    # log = resp["stats"][0]
    all_logs = []
    for l in resp["stats"]:
        all_logs.append(l)

    plays = []

    for log in all_logs:
        if statGroup == "hitting":
            # plays = []
            for split in log["splits"]:
                season = split["season"]
                date = split["date"]
                gameType = split["gameType"]
                gamePk = split["game"]["gamePk"]
                batter = split["batter"]
                team = split["team"]
                opponent = split["opponent"]
                pitcher = split["pitcher"]
                play = split.get("stat",{}).get("play",{})
                play_id = play.get("playId")
                details = play.get("details",{})
                eventType = details.get("event","-")
                event = details.get("call",{}).get("description","-")
                description = details.get("description","-")
                isInPlay = details.get("isInPlay","-")
                isStrike = details.get("isStrike","-")
                isBall = details.get("isBall","-")
                isAtBat = details.get("isAtBat","-")
                isPlateAppearance = details.get("isPlateAppearance","-")
                pitchType = details.get("type",{}).get("description","-")
                batterStands = details.get("batSide",{}).get("code")
                pitcherThrows = details.get("pitchHand",{}).get("code")
                balls = play.get("count",{}).get("balls","-")
                strikes = play.get("count",{}).get("strikes","-")
                outs = play.get("count",{}).get("outs","-")
                inning = play.get("count",{}).get("inning","-")
                runnerOnFirst = play.get("count",{}).get("runnerOn1b","-")
                runnerOnSecond = play.get("count",{}).get("runnerOn2b","-")
                runnerOnThird = play.get("count",{}).get("runnerOn3b","-")

                hitData = play.get("hitData",{})
                
                pitchData = play.get("pitchData",{})

                pitch_info = [
                    play_id,
                    batter["fullName"],
                    batter["id"],
                    pitcher["fullName"],
                    pitcher["id"],
                    pitchType,
                    pitchData.get("coordinates",{}).get("x","-"),
                    pitchData.get("coordinates",{}).get("y","-"),
                    pitchData.get("startSpeed","-"),
                    pitchData.get("strikeZoneTop","-"),
                    pitchData.get("strikeZoneBottom","-"),
                    pitchData.get("zone","-"),
                    hitData.get("launchSpeed","-"),
                    hitData.get("launchAngle","-"),
                    hitData.get("totalDistance","-"),
                    hitData.get("trajectory","-"),
                    hitData.get("coordinates",{}).get("landingPosX","-"),
                    hitData.get("coordinates",{}).get("landingPosY","-"),
                    eventType,
                    event,
                    season,
                    date,
                    gameType,
                    gamePk,
                    date,
                    balls,
                    strikes,
                    outs,
                    inning,
                    runnerOnFirst,
                    runnerOnSecond,
                    runnerOnThird,
                    description,
                    isInPlay,
                    isStrike,
                    isBall,
                    isAtBat,
                    isPlateAppearance,
                    batterStands,
                    pitcherThrows,
                    team["name"],
                    team["id"],
                    opponent["name"],
                    opponent["id"]]

                plays.append(pitch_info)

        elif statGroup == "pitching":
            # plays = []
            for split in log["splits"]:
                season = split["season"]
                date = split["date"]
                gameType = split["gameType"]
                gamePk = split["game"]["gamePk"]
                batter = split["batter"]
                team = split["team"]
                opponent = split["opponent"]
                pitcher = split["pitcher"]
                play = split.get("stat",{}).get("play",{})
                play_id = play.get("playId")
                details = play.get("details",{})
                eventType = details.get("event","-")
                event = details.get("call",{}).get("description","-")
                description = details.get("description","-")
                isInPlay = details.get("isInPlay","-")
                isStrike = details.get("isStrike","-")
                isBall = details.get("isBall","-")
                isAtBat = details.get("isAtBat","-")
                isPlateAppearance = details.get("isPlateAppearance","-")
                pitchType = details.get("type",{}).get("description","-")
                batterStands = details.get("batSide",{}).get("code")
                pitcherThrows = details.get("pitchHand",{}).get("code")
                balls = play.get("count",{}).get("balls","-")
                strikes = play.get("count",{}).get("strikes","-")
                outs = play.get("count",{}).get("outs","-")
                inning = play.get("count",{}).get("inning","-")
                runnerOnFirst = play.get("count",{}).get("runnerOn1b","-")
                runnerOnSecond = play.get("count",{}).get("runnerOn2b","-")
                runnerOnThird = play.get("count",{}).get("runnerOn3b","-")

                hitData = play.get("hitData",{})
                
                pitchData = play.get("pitchData",{})

                pitch_info = [
                    play_id,
                    batter["fullName"],
                    batter["id"],
                    pitcher["fullName"],
                    pitcher["id"],
                    pitchType,
                    pitchData.get("coordinates",{}).get("x","-"),
                    pitchData.get("coordinates",{}).get("y","-"),
                    pitchData.get("startSpeed","-"),
                    pitchData.get("strikeZoneTop","-"),
                    pitchData.get("strikeZoneBottom","-"),
                    pitchData.get("zone","-"),
                    hitData.get("launchSpeed","-"),
                    hitData.get("launchAngle","-"),
                    hitData.get("totalDistance","-"),
                    hitData.get("trajectory","-"),
                    hitData.get("coordinates",{}).get("landingPosX","-"),
                    hitData.get("coordinates",{}).get("landingPosY","-"),
                    eventType,
                    event,
                    season,
                    date,
                    gameType,
                    gamePk,
                    date,
                    balls,
                    strikes,
                    outs,
                    inning,
                    runnerOnFirst,
                    runnerOnSecond,
                    runnerOnThird,
                    description,
                    isInPlay,
                    isStrike,
                    isBall,
                    isAtBat,
                    isPlateAppearance,
                    batterStands,
                    pitcherThrows,
                    team["name"],
                    team["id"],
                    opponent["name"],
                    opponent["id"]]

                plays.append(pitch_info)
            
    columns = [
        'play_id',
        'batter_name',
        'batter_mlbam',
        'pitcher_name',
        'pitcher_mlbam',
        'pitch_type',
        'pitchX',
        'pitchY',
        'startSpeed',
        'strikeZoneTop',
        'strikeZoneBottom',
        'zone',
        'launchSpeed',
        'launchAngle',
        'totalDistance',
        'trajectory',
        'hitX',
        'hitY',
        'event_type',
        'event',
        'season',
        'date',
        'gameType',
        'gamePk',
        'date',
        'balls',
        'strikes',
        'outs',
        'inning',
        'runnerOnFirst',
        'runnerOnSecond',
        'runnerOnThird',
        'description',
        'isInPlay',
        'isStrike',
        'isBall',
        'isAtBat',
        'isPlateAppearance',
        'batterStands',
        'pitcherThrows',
        'team_name',
        'team_mlbam',
        'opponent_name',
        'opponent_mlbam']
    
    df = pd.DataFrame(data=plays,columns=columns)
    
    return df

def pitch_search(mlbam,seasons=None,statGroup=None,opposingTeamId=None,eventTypes=None,pitchTypes=None):
    """Search for any individual pitch 2008 and later

    Parameters
    ----------
    mlbam : str or int
        player's official MLB ID
    
    seasons : str or int, optional
        filter by season

    statGroup : str, optional
        filter results by stat group ('hitting','pitching','fielding')

    eventTypes : str, optional
        type of play events to filter

    pitchTypes : str, optional
        type of play events to filter

    gameType : str, optional
        filter results by game type

    """
    if seasons is None:
        seasons = default_season()

    queryString = [f"seasons={seasons}",f"hydrate=hitData,pitchData"]

    if opposingTeamId is None:
        pass
    else:
        queryString.append(f"opposingTeamId={opposingTeamId}")

    if eventTypes is None:
        pass
    else:
        queryString.append(f"eventType={eventTypes}")

    if pitchTypes is None:
        pass
    else:
        queryString.append(f"pitchTypes={pitchTypes}")

    if statGroup is None:
        pass
    else:
        queryString.append(f"group={statGroup}")


    queryString = "&".join(queryString)
    
    url = BASE + f"/people/{mlbam}/stats?stats=pitchLog&{queryString}"


    response = requests.get(url)
    
    log = response.json()["stats"][0]
    
    if statGroup == "hitting":
        pitches = []
        for split in log["splits"]:
            season = split["season"]
            date = split["date"]
            gameType = split["gameType"]
            gamePk = split["game"]["gamePk"]
            batter = split["batter"]
            team = split["team"]
            opponent = split["opponent"]
            pitcher = split["pitcher"]
            play = split.get("stat",{}).get("play",{})
            play_id = play.get("playId")
            details = play.get("details",{})
            eventType = details.get("event","-")
            event = details.get("call",{}).get("description","-")
            description = details.get("description","-")
            isInPlay = details.get("isInPlay","-")
            isStrike = details.get("isStrike","-")
            isBall = details.get("isBall","-")
            isAtBat = details.get("isAtBat","-")
            isPlateAppearance = details.get("isPlateAppearance","-")
            pitchType = details.get("type",{}).get("description","-")
            batterStands = details.get("batSide",{}).get("code")
            pitcherThrows = details.get("pitchHand",{}).get("code")
            balls = play.get("count",{}).get("balls","-")
            strikes = play.get("count",{}).get("strikes","-")
            outs = play.get("count",{}).get("outs","-")
            inning = play.get("count",{}).get("inning","-")
            runnerOnFirst = play.get("count",{}).get("runnerOn1b","-")
            runnerOnSecond = play.get("count",{}).get("runnerOn2b","-")
            runnerOnThird = play.get("count",{}).get("runnerOn3b","-")

            hitData = play.get("hitData",{})
            
            pitchData = play.get("pitchData",{})

            pitch_info = [
                play_id,
                batter["fullName"],
                batter["id"],
                pitcher["fullName"],
                pitcher["id"],
                pitchType,
                pitchData.get("coordinates",{}).get("x","-"),
                pitchData.get("coordinates",{}).get("y","-"),
                pitchData.get("startSpeed","-"),
                pitchData.get("strikeZoneTop","-"),
                pitchData.get("strikeZoneBottom","-"),
                pitchData.get("zone","-"),
                hitData.get("launchSpeed","-"),
                hitData.get("launchAngle","-"),
                hitData.get("totalDistance","-"),
                hitData.get("trajectory","-"),
                hitData.get("coordinates",{}).get("landingPosX","-"),
                hitData.get("coordinates",{}).get("landingPosY","-"),
                eventType,
                event,
                season,
                date,
                gameType,
                gamePk,
                date,
                balls,
                strikes,
                outs,
                inning,
                runnerOnFirst,
                runnerOnSecond,
                runnerOnThird,
                description,
                isInPlay,
                isStrike,
                isBall,
                isAtBat,
                isPlateAppearance,
                batterStands,
                pitcherThrows,
                team["name"],
                team["id"],
                opponent["name"],
                opponent["id"]]

            pitches.append(pitch_info)

    elif statGroup == "pitching":
        pitches = []
        for split in log["splits"]:
            season = split["season"]
            date = split["date"]
            gameType = split["gameType"]
            gamePk = split["game"]["gamePk"]
            batter = split["batter"]
            team = split["team"]
            opponent = split["opponent"]
            pitcher = split["pitcher"]
            play = split.get("stat",{}).get("play",{})
            play_id = play.get("playId")
            details = play.get("details",{})
            eventType = details.get("event","-")
            event = details.get("call",{}).get("description","-")
            description = details.get("description","-")
            isInPlay = details.get("isInPlay","-")
            isStrike = details.get("isStrike","-")
            isBall = details.get("isBall","-")
            isAtBat = details.get("isAtBat","-")
            isPlateAppearance = details.get("isPlateAppearance","-")
            pitchType = details.get("type",{}).get("description","-")
            batterStands = details.get("batSide",{}).get("code")
            pitcherThrows = details.get("pitchHand",{}).get("code")
            balls = play.get("count",{}).get("balls","-")
            strikes = play.get("count",{}).get("strikes","-")
            outs = play.get("count",{}).get("outs","-")
            inning = play.get("count",{}).get("inning","-")
            runnerOnFirst = play.get("count",{}).get("runnerOn1b","-")
            runnerOnSecond = play.get("count",{}).get("runnerOn2b","-")
            runnerOnThird = play.get("count",{}).get("runnerOn3b","-")

            hitData = play.get("hitData",{})
            
            pitchData = play.get("pitchData",{})

            pitch_info = [
                play_id,
                batter["fullName"],
                batter["id"],
                pitcher["fullName"],
                pitcher["id"],
                pitchType,
                pitchData.get("coordinates",{}).get("x","-"),
                pitchData.get("coordinates",{}).get("y","-"),
                pitchData.get("startSpeed","-"),
                pitchData.get("strikeZoneTop","-"),
                pitchData.get("strikeZoneBottom","-"),
                pitchData.get("zone","-"),
                hitData.get("launchSpeed","-"),
                hitData.get("launchAngle","-"),
                hitData.get("totalDistance","-"),
                hitData.get("trajectory","-"),
                hitData.get("coordinates",{}).get("landingPosX","-"),
                hitData.get("coordinates",{}).get("landingPosY","-"),
                eventType,
                event,
                season,
                date,
                gameType,
                gamePk,
                date,
                balls,
                strikes,
                outs,
                inning,
                runnerOnFirst,
                runnerOnSecond,
                runnerOnThird,
                description,
                isInPlay,
                isStrike,
                isBall,
                isAtBat,
                isPlateAppearance,
                batterStands,
                pitcherThrows,
                team["name"],
                team["id"],
                opponent["name"],
                opponent["id"]]

            pitches.append(pitch_info)
           
    columns = [
        'play_id',
        'batter_name',
        'batter_mlbam',
        'pitcher_name',
        'pitcher_mlbam',
        'pitch_type',
        'pitchX',
        'pitchY',
        'startSpeed',
        'strikeZoneTop',
        'strikeZoneBottom',
        'zone',
        'launchSpeed',
        'launchAngle',
        'totalDistance',
        'trajectory',
        'hitX',
        'hitY',
        'event_type',
        'event',
        'season',
        'date',
        'gameType',
        'gamePk',
        'date',
        'balls',
        'strikes',
        'outs',
        'inning',
        'runnerOnFirst',
        'runnerOnSecond',
        'runnerOnThird',
        'description',
        'isInPlay',
        'isStrike',
        'isBall',
        'isAtBat',
        'isPlateAppearance',
        'batterStands',
        'pitcherThrows',
        'team_name',
        'team_mlbam',
        'opponent_name',
        'opponent_mlbam']
    
    df = pd.DataFrame(data=pitches,columns=columns)
    
    return df

def schedule_search(date:str,teamID="",oppID="",season="",startDate="",endDate=""):
    query_str = []
    if teamID != "":
        query_str.append(f"teamId={teamID}")
    if oppID != "":
        query_str.append(f"opponentId={oppID}")
    if season != "":
        query_str.append(f"season={season}")
    if startDate != "":
        query_str.append(f"startDate={startDate}")
    if endDate != "":
        query_str.append(f"endDate={endDate}")
    query_str = "&".join(query_str)
    
    gameType = "S,F,D,L,W,R,E"
    hydrations = "linescore(matchup,person),team,previousPlay,probablePitcher,stats,broadcasts(all),person,lineups,decisions"
    url = BASE + f"/schedule?sportId=1&date={date}&gameType={gameType}&{query_str}&hydrate={hydrations}"
    # date=7/1/2021 &gamePk=633457 &timecode=20210701_172455
    # print(url)
    try:response = requests.get(url).json()["dates"]
    except:return "no games found"
    
    schedule = []
    for date in response:
        game_list = date["games"]
        for game in game_list:
            game_info = {}
            gamePk = game["gamePk"]
            game_info["gamePk"] = gamePk
            game_info["gameType"] = game["gameType"]
            game_info["gameDesc"] = game.get("description","")
            game_info["season"] = game.get("season","")
            game_info["gameDate"] = game.get("officialDate","")
            game_info["gameTime"] = game["gameDate"] # Needs to be converted to UTC (could do with JS)
            game_info["gameState"] = game["status"]["detailedState"]
            game_info["liveState"] = game["status"]["abstractGameState"]
            game_info["reason"] = game["status"].get("reason","")
            game_info["venueName"] = game.get("venue",{}).get("name","")
            game_info["venue_mlbam"] = game.get("venue",{}).get("id","")

            game_info["broadcastAwayTV"] = []
            game_info["broadcastHomeTV"] = []
            game_info["broadcastAwayRadio"] = []
            game_info["broadcastHomeRadio"] = []
            try:
                for bc in game["broadcasts"]:
                    if bc["language"] == "en":
                        if bc["homeAway"] == "away" and bc["type"] == "TV":   # if AWAY TV broadcast
                            broadcast = {
                                "name":bc.get("name",""),
                                "resolution":bc.get("videoResolution",{}).get("videoResolution","")}
                            game_info["broadcastAwayTV"].append(broadcast)

                        elif bc["homeAway"] == "home" and bc["type"] == "TV":   # if HOME TV broadcast
                            broadcast = {
                                "name":bc.get("name",""),
                                "resolution":bc.get("videoResolution",{}).get("videoResolution","")}
                            game_info["broadcastHomeTV"].append(broadcast)

                        elif bc["homeAway"] == "away" and bc["type"] == "AM":   # if AWAY RADIO broadcast
                            broadcast = {"name":bc.get("name","")}
                            game_info["broadcastAwayRadio"].append(broadcast)

                        elif bc["homeAway"] == "home" and bc["type"] == "AM":   # if HOME RADIO broadcast
                            broadcast = {"name":bc.get("name","")}
                            game_info["broadcastHomeRadio"].append(broadcast)

                    else:
                        pass

            except:
                pass

            teams = game["teams"]
            game_info["teamAway"] = teams["away"]["team"]["name"]
            game_info["teamHome"] = teams["home"]["team"]["name"]
            lineups = game.get("lineups",{})
            game_info["lineupAway"] = lineups.get("awayPlayers",{})
            game_info["lineupHome"] = lineups.get("homePlayers",{})
            game_info["teamAway_clubName"] = teams["away"]["team"]["clubName"]
            game_info["teamHome_clubName"] = teams["home"]["team"]["clubName"]
            game_info["teamAway_mlbam"] = teams["away"]["team"]["id"]
            game_info["teamHome_mlbam"] = teams["home"]["team"]["id"]
            game_info["scoreAway"] = teams["away"].get("score","-")
            game_info["scoreHome"] = teams["home"].get("score","-")
            game_info["isWinnerAway"] = teams["away"].get("isWinner",False)
            game_info["isWinnerHome"] = teams["home"].get("isWinner",False)
            awayRecWins = teams["away"].get("leagueRecord",{}).get("wins")
            awayRecLosses = teams["away"].get("leagueRecord",{}).get("losses")
            homeRecWins = teams["home"].get("leagueRecord",{}).get("wins")
            homeRecLosses = teams["home"].get("leagueRecord",{}).get("losses")
            game_info["recordAway"] = f"{awayRecWins}-{awayRecLosses}"
            game_info["recordHome"] = f"{homeRecWins}-{homeRecLosses}"
            probPitcherAway = teams["away"].get("probablePitcher",{})
            probPitcherHome = teams["home"].get("probablePitcher",{})
            game_info["probPitcherAway"] = {"mlbam":probPitcherAway.get("id",""),"name":probPitcherAway.get("fullName",""),"nameShort":probPitcherAway.get("initLastName",""),"era":"","wins":"","losses":""}
            game_info["probPitcherHome"] = {"mlbam":probPitcherHome.get("id",""),"name":probPitcherHome.get("fullName",""),"nameShort":probPitcherHome.get("initLastName",""),"era":"","wins":"","losses":""}
            game_info["pitcherWin"] = game.get("decisions",{}).get("winner",{})
            game_info["pitcherLoss"] = game.get("decisions",{}).get("loser",{})
            game_info["pitcherSave"] = game.get("decisions",{}).get("save",{})

            try:
                for t in probPitcherAway["stats"]:
                    if t["type"]["displayName"] == "statsSingleSeason" and t["group"]["displayName"] == "pitching":
                        game_info["probPitcherAway"]["era"] = t["stats"]["era"]
                        game_info["probPitcherAway"]["wins"] = t["stats"]["wins"]
                        game_info["probPitcherAway"]["losses"] = t["stats"]["losses"]
                        game_info["probPitcherAway"]["saves"] = t["stats"].get("saves","-")
                        break
            except:
                pass
            try:
                for t in probPitcherHome["stats"]:
                    if t["type"]["displayName"] == "statsSingleSeason" and t["group"]["displayName"] == "pitching":
                        game_info["probPitcherHome"]["era"] = t["stats"]["era"]
                        game_info["probPitcherHome"]["wins"] = t["stats"]["wins"]
                        game_info["probPitcherHome"]["losses"] = t["stats"]["losses"]
                        game_info["probPitcherHome"]["saves"] = t["stats"].get("saves","-")
                        break
            except:
                pass
            
            game_info["isDoubleHeader"] = True if game.get("doubleHeader","") != "N" else False
            game_info["gameNumberDay"] = game.get("gameNumber",1)
            game_info["gameNumberSeries"] = game.get("seriesGameNumber",1)
            game_info["gamesInSeries"] = game.get("gamesInSeries","-")

            linescore = game.get("linescore",{})
            innScores = linescore.get("innings",{})
            offense = linescore.get("offense",{})
            defense = linescore.get("defense",{})

            game_info["scoreboard"] = linescore   # might not be necessary but could be another way to access data if it's easier
            game_info["innScores"] = innScores
            currentInning = linescore.get("currentInning","")
            currentInningOrdinal = linescore.get("currentInningOrdinal")
            inningHalf = linescore.get("inningHalf","").lower() # CHANGE TO 'inningState' at some point
            inningState = linescore.get("inningState","").lower()
            game_info["inning"] = currentInning
            game_info["inningOrdinal"] = currentInningOrdinal
            game_info["inningHalf"] = inningHalf
            game_info["inningState"] = inningState
            recentPlay_dict = game.get("previousPlay",{})
            game_info["recentPlay"] = recentPlay_dict.get("result",{}).get("description","")
            game_info["balls"] = linescore.get("balls","")
            game_info["strikes"] = linescore.get("strikes","")
            game_info["outs"] = linescore.get("outs","")
            runnerOnFirst = True if "first" in offense.keys() else False
            runenrOnSecond = True if "second" in offense.keys() else False
            runnerOnThird = True if "third" in offense.keys() else False
            game_info["runnersOn"] = {"first":runnerOnFirst,"second":runenrOnSecond,"third":runnerOnThird}

            # Matchup, pitcher/hitter stats, bat order & situation info

            game_info["teamBatting"] = offense.get("team",{}).get("id","")
            game_info["teamFielding"]= defense.get("team",{}).get("id","")

            # pitcher on mount & upcoming batters for current team batting
            onMound = defense.get("pitcher",{})
            qBatter = defense.get("batter",{})
            qOnDeck = defense.get("onDeck",{})
            qInHole = defense.get("inHole",{})
            
            # current batter/queue for team batting & pitcher for the team batting
            offMound = offense.get("pitcher",{})
            batter = offense.get("batter",{})
            onDeck = offense.get("onDeck",{})
            inHole = offense.get("inHole",{})


            game_info["orderBatting"] = offense.get("battingOrder","")
            game_info["orderFielding"] = defense.get("battingOrder","")

            game_info["onMound"] = {"mlbam":onMound.get("id",""),"name":onMound.get("fullName",""),"nameShort":onMound.get("initLastName",""),"era":"","wins":"","losses":""}
            game_info["offMound"] = {"mlbam":offMound.get("id",""),"name":offMound.get("fullName",""),"nameShort":offMound.get("initLastName",""),"era":"","wins":"","losses":""}

            # get stats for current pitcher on mound
            try:
                for t in onMound["stats"]:
                    if t["type"]["displayName"] == "statsSingleSeason" and t["group"]["displayName"] == "pitching":
                        game_info["onMound"]["era"] = t["stats"]["era"]
                        game_info["onMound"]["wins"] = t["stats"]["wins"]
                        game_info["onMound"]["losses"] = t["stats"]["losses"]
                        game_info["onMound"]["saves"] = t["stats"].get("saves","-")
                        break
            except:
                pass

            # get stats for pitcher on batting team
            try:
                for t in offMound["stats"]:
                    if t["type"]["displayName"] == "statsSingleSeason" and t["group"]["displayName"] == "pitching":
                        game_info["offMound"]["era"] = t["stats"]["era"]
                        game_info["offMound"]["wins"] = t["stats"]["wins"]
                        game_info["offMound"]["losses"] = t["stats"]["losses"]
                        game_info["offMound"]["saves"] = t["stats"].get("saves","-")
                        break
            except:
                pass


            
            game_info["batters"] = {
                "batter":{"mlbam":batter.get("id",""),"name":batter.get("fullName",""),"nameShort":batter.get("initLastName",""),"avg":"","hits":"","ab":"","pa":""},
                "onDeck":{"mlbam":onDeck.get("id",""),"name":onDeck.get("fullName",""),"nameShort":onDeck.get("initLastName",""),"avg":"","hits":"","ab":"","pa":""},
                "inHole":{"mlbam":inHole.get("id",""),"name":inHole.get("fullName",""),"nameShort":inHole.get("initLastName",""),"avg":"","hits":"","ab":"","pa":""}
                }
            game_info["defBatters"] = {
                "batter":{"mlbam":qBatter.get("id",""),"name":qBatter.get("fullName",""),"nameShort":qBatter.get("initLastName",""),"avg":"","hits":"","ab":"","pa":""},
                "onDeck":{"mlbam":qOnDeck.get("id",""),"name":qOnDeck.get("fullName",""),"nameShort":qOnDeck.get("initLastName",""),"avg":"","hits":"","ab":"","pa":""},
                "inHole":{"mlbam":qInHole.get("id",""),"name":qInHole.get("fullName",""),"nameShort":qInHole.get("initLastName",""),"avg":"","hits":"","ab":"","pa":""}
                }
            
            # stats for current batting team order
            for idx,b in enumerate([batter,onDeck,inHole]):
                try:
                    for t in b["stats"]:
                        if t["type"]["displayName"] == "gameLog" and t["group"]["displayName"] == "hitting":
                            if idx == 0:
                                game_info["batters"]["batter"]["hits"] = t["stats"]["hits"]
                                game_info["batters"]["batter"]["ab"] = t["stats"]["atBats"]
                                game_info["batters"]["batter"]["pa"] = t["stats"]["plateAppearances"]
                            elif idx == 1:
                                game_info["batters"]["onDeck"]["hits"] = t["stats"]["hits"]
                                game_info["batters"]["onDeck"]["ab"] = t["stats"]["atBats"]
                                game_info["batters"]["onDeck"]["pa"] = t["stats"]["plateAppearances"]
                            elif idx == 2:
                                game_info["batters"]["inHole"]["hits"] = t["stats"]["hits"]
                                game_info["batters"]["inHole"]["ab"] = t["stats"]["atBats"]
                                game_info["batters"]["inHole"]["pa"] = t["stats"]["plateAppearances"]

                        elif t["type"]["displayName"] == "statsSingleSeason" and t["group"]["displayName"] == "hitting":
                            if idx == 0:
                                game_info["batters"]["batter"]["avg"] = t["stats"]["avg"]
                            elif idx == 1:
                                game_info["batters"]["onDeck"]["avg"] = t["stats"]["avg"]
                            elif idx == 2:
                                game_info["batters"]["inHole"]["avg"] = t["stats"]["avg"]
                except:
                    pass
            #

            # stats for upcoming batters on fielding team
            for idx,b in enumerate([qBatter,qOnDeck,qInHole]):
                try:
                    for t in b["stats"]:
                        if t["type"]["displayName"] == "gameLog" and t["group"]["displayName"] == "hitting":
                            if idx == 0:
                                game_info["defBatters"]["batter"]["hits"] = t["stats"]["hits"]
                                game_info["defBatters"]["batter"]["ab"] = t["stats"]["atBats"]
                                game_info["defBatters"]["batter"]["pa"] = t["stats"]["plateAppearances"]
                            elif idx == 1:
                                game_info["defBatters"]["onDeck"]["hits"] = t["stats"]["hits"]
                                game_info["defBatters"]["onDeck"]["ab"] = t["stats"]["atBats"]
                                game_info["defBatters"]["onDeck"]["pa"] = t["stats"]["plateAppearances"]
                            elif idx == 2:
                                game_info["defBatters"]["inHole"]["hits"] = t["stats"]["hits"]
                                game_info["defBatters"]["inHole"]["ab"] = t["stats"]["atBats"]
                                game_info["defBatters"]["inHole"]["pa"] = t["stats"]["plateAppearances"]
                                
                        elif t["type"]["displayName"] == "statsSingleSeason" and t["group"]["displayName"] == "hitting":
                            if idx == 0:
                                game_info["defBatters"]["batter"]["avg"] = t["stats"]["avg"]
                            elif idx == 1:
                                game_info["defBatters"]["onDeck"]["avg"] = t["stats"]["avg"]
                            elif idx == 2:
                                game_info["defBatters"]["inHole"]["avg"] = t["stats"]["avg"]
                except:
                    pass
            #

            try:game_info["runsAway"] = linescore["teams"]["away"]["runs"]
            except:game_info["runsAway"] = "-"
            try:game_info["hitsAway"] = linescore["teams"]["away"]["hits"]
            except:game_info["hitsAway"] = "-"
            try:game_info["errorsAway"] = linescore["teams"]["away"]["errors"]
            except:game_info["errorsAway"] = "-"
            try:game_info["runsHome"] = linescore["teams"]["home"]["runs"]
            except:game_info["runsHome"] = "-"
            try:game_info["hitsHome"] = linescore["teams"]["home"]["hits"]
            except:game_info["hitsHome"] = "-"
            try:game_info["errorsHome"] = linescore["teams"]["home"]["errors"]
            except:game_info["errorsHome"] = "-"

            schedule.append(game_info)
        
        
    return schedule

def game_search(team=None,date=None,startDate=None,endDate=None,season=None,gameType=None):
    """
    Search for a games by team and/or date (fmt: 'mm/dd/yyyy')

    "team" values can be the team's mlbam or team name

    NOTE: "date" will be ignored if "startDate" and "endDate" are used. If only "startDate" is used, "endDate" will default to today's date
    
    NEED TO ADD PARAMETER FOR 'OPPONENT TEAM'
    """
    tm_strQuery = True
    teamSearch = True
    query_str = []

    if team is None:
        teamSearch = False
    elif type(team) is str:
        if team.isdigit():
            team = f"teamId={team}"
            query_str.append(team)
            tm_strQuery = False
    elif type(team) is int:
        team = f"teamId={team}"
        query_str.append(team)
        tm_strQuery = False

    if date is None:
        pass
    elif startDate is None and endDate is None:
        date = f"date={date}"
        query_str.append(date)
    
    if season is None and date is None:
        if startDate is None and endDate is None:
            season = f"season={curr_year}"
            query_str.append(season)
        elif endDate is None and startDate is not None:
            startDate = f"startDate={startDate}"
            endDate = f"endDate={dt.date.strftime(dt.date.today(),'%m/%d/%Y')}"
            query_str.append(startDate)
            query_str.append(endDate)
        elif startDate is not None and endDate is not None:
            startDate = f"startDate={startDate}"
            endDate = f"endDate={endDate}"
            query_str.append(startDate)
            query_str.append(endDate)
    elif season is not None:
        season = f"season={season}"
        query_str.append(season)
    
    if gameType is None:
        pass
    else:
        gameType = f"gameType={gameType}"
        query_str.append(gameType)
    
    query_str = "&".join(query_str)
    
    url = BASE + f"/schedule?sportId=1&{query_str}"
    response = requests.get(url)
    all_results = []

    if tm_strQuery is False and teamSearch is True:
        for d in response.json()["dates"]:
            for g in d["games"]:
                home = g.get("teams",{}).get("home",{}).get("team",{}).get("name","-")
                home_mlbam = g.get("teams",{}).get("home",{}).get("team",{}).get("id","-")
                away = g.get("teams",{}).get("away",{}).get("team",{}).get("name","-")
                away_mlbam = g.get("teams",{}).get("away",{}).get("team",{}).get("id","-")
                game_date = g.get("officialDate","-")
                game_pk = g.get("gamePk","-")
                game_type = g.get("gameType","-")
                status = g.get("status",{}).get("detailedState","-")
                venue = g.get("venue",{}).get("name","-")
                # start_time = g.get("gameDate","")
                result = [
                    game_pk,
                    f"{away} ({away_mlbam})",
                    f"{home} ({home_mlbam})",
                    game_date,
                    game_type,
                    venue,
                    # start_time,
                    status]
                all_results.append(result)
    
    elif tm_strQuery is True and teamSearch is True:
        for d in response.json()["dates"]:
            for g in d["games"]:
                home = g.get("teams",{}).get("home",{}).get("team",{}).get("name","-")
                home_mlbam = g.get("teams",{}).get("home",{}).get("team",{}).get("id","-")
                away = g.get("teams",{}).get("away",{}).get("team",{}).get("name","-")
                away_mlbam = g.get("teams",{}).get("away",{}).get("team",{}).get("id","-")

                # If team search query is not found in key:value pair, game will not be appended to results
                if team.lower() in home.lower() or team.lower() in away.lower():
                    pass
                else:
                    continue
                
                game_date = g.get("officialDate","-")
                game_pk = g.get("gamePk","-")
                game_type = g.get("gameType","-")
                status = g.get("status",{}).get("detailedState","-")
                venue = g.get("venue",{}).get("name","-")
                # start_time = g.get("gameDate","")
                result = [
                    game_pk,
                    f"{away} ({away_mlbam})",
                    f"{home} ({home_mlbam})",
                    game_date,
                    game_type,
                    venue,
                    # start_time,
                    status]
                all_results.append(result)

    elif tm_strQuery is True and teamSearch is False:
        for d in response.json()["dates"]:
            for g in d["games"]:
                home = g.get("teams",{}).get("home",{}).get("team",{}).get("name","-")
                home_mlbam = g.get("teams",{}).get("home",{}).get("team",{}).get("id","-")
                away = g.get("teams",{}).get("away",{}).get("team",{}).get("name","-")
                away_mlbam = g.get("teams",{}).get("away",{}).get("team",{}).get("id","-")
                game_date = g.get("officialDate","-")
                game_pk = g.get("gamePk","-")
                game_type = g.get("gameType","-")
                status = g.get("status",{}).get("detailedState","-")
                venue = g.get("venue",{}).get("name","-")
                # start_time = g.get("gameDate","")
                result = [
                    game_pk,
                    f"{away} ({away_mlbam})",
                    f"{home} ({home_mlbam})",
                    game_date,
                    game_type,
                    venue,
                    # start_time,
                    status]
                all_results.append(result)
    
    df = pd.DataFrame(data=all_results,columns=("gamePk","Away","Home","Date","Type","Venue","Status"))
    if len(df) == 0:
        return "No games found"
    return df

def last_game(teamID):
    teamID = str(teamID)

    season_info = get_season_info()

    if season_info['in_progress'] is None:
        m = 12
        d = 1
        y = season_info['last_completed']
        season = y
    else:
        m = curr_date.month
        d = curr_date.day
        y = curr_date.year
        season = season_info['in_progress']

    url = BASE + f"/teams/{teamID}?hydrate=previousSchedule(date={m}/{d}/{y},inclusive=True,limit=1,season={season},gameType=[S,R,P])"

    resp = requests.get(url)

    result = resp.json()["teams"][0]["previousGameSchedule"]["dates"][0]["games"][0]
    gamePk = result.get("gamePk","")
    gameType = result.get("gameType","")
    gameDate = result.get("gameDate","")[:10]
    away_mlbam = result.get("teams",{}).get("away",{}).get("team",{}).get("id","-")
    home_mlbam = result.get("teams",{}).get("home",{}).get("team",{}).get("id","-")
    away = result.get("teams",{}).get("away",{}).get("team",{}).get("name","-")
    home = result.get("teams",{}).get("home",{}).get("team",{}).get("name","-")
    if str(away_mlbam) == str(teamID):
        opp = "@ " + home
        opp_mlbam = home_mlbam
    elif str(home_mlbam) == str(teamID):
        opp = "vs " + away
        opp_mlbam = away_mlbam
    df = pd.DataFrame(data=[[gamePk,opp,opp_mlbam,gameDate,gameType]],columns=("gamePk","opponent","opp_mlbam","date","gameType"))

    return df

def next_game(teamID):

    m = curr_date.month
    d = curr_date.day
    y = curr_date.year


    try:
        url = BASE + f"/teams/{teamID}?hydrate=nextSchedule(date={m}/{d}/{y},inclusive=True,limit=1,season={y},gameType=[S,R,P])"
        response = requests.get(url)
        results = response.json()["teams"][0]["nextGameSchedule"]["dates"][0]["games"]
    except:
        url = BASE + f"/teams/{teamID}?hydrate=nextSchedule(date={m}/{d}/{y},inclusive=True,limit=1,season={y+1},gameType=[S,R,P])"
        response = requests.get(url)
        results = response.json()["teams"][0]["nextGameSchedule"]["dates"][0]["games"]

    result = results[0]
    gamePk = result.get("gamePk","")
    gameType = result.get("gameType","")
    gameDate = result.get("gameDate","")[:10]
    away_mlbam = result.get("teams",{}).get("away",{}).get("team",{}).get("id","-")
    home_mlbam = result.get("teams",{}).get("home",{}).get("team",{}).get("id","-")
    away = result.get("teams",{}).get("away",{}).get("team",{}).get("name","-")
    home = result.get("teams",{}).get("home",{}).get("team",{}).get("name","-")
    if str(away_mlbam) == str(teamID):
        opp = "@ " + home
        opp_mlbam = home_mlbam
    elif str(home_mlbam) == str(teamID):
        opp = "vs " + away
        opp_mlbam = away_mlbam
    df = pd.DataFrame(data=[[gamePk,opp,opp_mlbam,gameDate,gameType]],columns=("gamePk","opponent","opp_mlbam","date","gameType"))
    
    return df

def schedule(mlbam=None,season=None,date=None,startDate=None,endDate=None,gameType=None,opponentId=None,**kwargs) -> pd.DataFrame:
    """Get game schedule data.

    Parameters
    ----------
    mlbam : int or str, optional
        team's official MLB ID

    season : int or str, optional
        get schedule information for a specific season

    date : str, optional (format, "YYYY-mm-dd")
        get the schedule for a specific date
    
    startDate : str, optional (format: "YYYY-mm-dd")
        the starting date to filter your search
    
    endDate : str, optional (format: "YYYY-mm-dd")
        the ending date to filter your search
    
    gameType : str, optional
        filter results by specific game type(s)
    
    opponentId : str or int, optional
        specify an opponent's team mlbam/id to get results between two opponents

    Other Parameters
    ----------------
    tz : str, optional (Default is Central time)
        keyword argument to specify which timezone to view game times
    
    hydrate : str, optional
        retrieve additional data

    """

    url = BASE + "/schedule?"

    params = {
        "sportId":1
    }
    if startDate is not None and endDate is not None:
        # params['startDate']   = dt.datetime.strptime(startDate,r"%m/%d/%Y").strftime(r"%Y-%m-%d")
        params['startDate']     = startDate
        # params['endDate']     = dt.datetime.strptime(endDate,r"%m/%d/%Y").strftime(r"%Y-%m-%d")
        params['endDate']       = endDate

    else:
        if date is None and season is None:
            season_info = get_season_info()
            if season_info['in_progress'] is None:
                season = season_info['last_completed']
            else:
                season = season_info['in_progress']

            params['season'] = season

        elif date is not None:
            # date = dt.datetime.strptime(date,r"%m/%d/%Y").strftime(r"%Y-%m-%d")
            params['date'] = date
        
        elif season is not None:
            params['season'] = season
    
    if gameType is not None:
        if type(gameType) is list:
            gtype_str = ",".join(gameType).upper()
        elif type(gameType) is str:
            gtype_str = gameType.upper().replace(" ","")
        params["gameType"] = gtype_str

    if mlbam is not None:
        params['teamId'] = mlbam
    
    tz = kwargs.get("tz",kwargs.get("timezone"))
    if tz is None:
        tz = ct_zone
    else:
        if type(tz) is str:
            tz = tz.lower()
            if tz in ("cst","ct","central"):
                tz = ct_zone
            elif tz in ("est","et","eastern"):
                tz = et_zone
            elif tz in ("mst","mt","mountain"):
                tz = mt_zone
            elif tz in ("pst","pt","pacific"):
                tz = pt_zone
    
    if kwargs.get("oppTeamId") is not None:
        opponentId = kwargs["oppTeamId"]
    elif kwargs.get("opponentTeamId") is not None:
        opponentId = kwargs["opponentTeamId"]
    elif kwargs.get("oppId") is not None:
        opponentId = kwargs["oppId"]
    if opponentId is not None:
        params["opponentId"] = None

    if kwargs.get("hydrate") is not None:
        hydrate = kwargs["hydrate"]
        params["hydrate"] = hydrate
    
    resp = requests.get(url,params=params)

    # print("\n================")
    # print(resp.url)
    # print("================\n")

    dates = resp.json()['dates']

    data = []
    cols = [
        "game_start",
        "date_sched",
        "date_resched",
        "date_official",
        "sched_dt",
        "sched_time",
        "resched_dt",
        "resched_time",
        "gamePk",
        "game_type",
        "venue_mlbam",
        "venue_name",
        "away_mlbam",
        "away_name",
        "away_score",
        "away_record",
        "aw_pp_name",
        "aw_pp_mlbam",
        "home_mlbam",
        "home_name",
        "home_score",
        "home_record",
        "hm_pp_name",
        "hm_pp_mlbam",
        "abstract_state",
        "abstract_code",
        "detailed_state",
        "detailed_code",
        "status_code",
        "reason",
        "bc_tv_aw",
        "bc_tv_aw_res",
        "bc_tv_hm",
        "bc_tv_hm_res",
        "bc_radio_aw", 
        "bc_radio_hm",
        "recap_url",
        "recap_title",
        "recap_desc",
        "recap_avail"
        ]
    
    for d in dates:
        games = d['games']
        date = d['date']
        for g in games:
            teams = g["teams"]
            away = teams["away"]
            home = teams["home"]

            game_date = g.get("gameDate")
            resched_date = g.get("rescheduleDate")
            date_official = g.get("officialDate")
            date_sched = date
            date_resched = g.get("rescheduleGameDate")

            if type(game_date) is str:
                game_dt = dt.datetime.strptime(game_date,r"%Y-%m-%dT%H:%M:%SZ")
                game_dt = pytz.utc.fromutc(game_dt)
                game_dt = game_dt.astimezone(tz)
                sched_time = game_dt.strftime(r"%-I:%M %p")
                game_start = sched_time
            else:
                game_dt = "-"
                sched_time = "-"

            if type(resched_date) is str:
                resched_dt = dt.datetime.strptime(resched_date,r"%Y-%m-%dT%H:%M:%SZ")
                resched_dt = pytz.utc.fromutc(resched_dt)
                resched_dt = resched_dt.astimezone(tz)
                resched_time = resched_dt.strftime(r"%-I:%M %p")
                game_start = resched_time
            else:
                resched_dt = "-"
                resched_time = "-"

            gamePk = str(g.get("gamePk","-"))
            
            away_score = away.get("score")
            away_score = "0" if away_score is None else str(int(away_score))
            home_score = home.get("score")
            home_score = "0" if home_score is None else str(int(home_score))

            sched_dt = str(game_dt)
            resched_dt = str(resched_dt)
            game_type = g.get("gameType")

            venue = g.get("venue",{})
            venue_mlbam = venue.get("id")
            venue_name = venue.get("name")

            away_mlbam = str(int(away.get("team").get("id")))
            away_name = away.get("team").get("name")
            away_rec = f'{away.get("leagueRecord",{}).get("wins")}-{away.get("leagueRecord",{}).get("losses")}'
            aw_pp = away.get("probablePitcher",{})
            aw_pp_mlbam = aw_pp.get("id","")
            aw_pp_name = aw_pp.get("fullName","")

            
            home_mlbam = str(int(home.get("team").get("id")))
            home_name = home.get("team").get("name")
            home_rec = f'{home.get("leagueRecord",{}).get("wins")}-{home.get("leagueRecord",{}).get("losses")}'
            hm_pp = home.get("probablePitcher",{})
            hm_pp_mlbam = hm_pp.get("id","")
            hm_pp_name = hm_pp.get("fullName","")


            status = g.get("status",{})
            abstract_state = status.get("abstractGameState")
            abstract_code = status.get("abstractGameCode")
            detailed_state = status.get("detailedState")
            detailed_code = status.get("codedGameState")
            status_code = status.get("statusCode")
            reason = status.get("reason")

            bc_tv_aw = ""
            bc_tv_aw_res = ""
            bc_tv_hm = ""
            bc_tv_hm_res = ""
            bc_radio_aw = ""
            bc_radio_hm = ""
            broadcasts = g.get("broadcasts",[{}])
            for bc in broadcasts:
                if bc.get("language") == "en":
                    if bc.get("type") == "TV":
                        if bc.get("homeAway") == "away":
                            bc_tv_aw = bc.get("name")
                            bc_tv_aw_res = bc.get("videoResolution",{}).get("resolutionShort","-")
                        else:
                            bc_tv_hm = bc.get("name")
                            bc_tv_hm_res = bc.get("videoResolution",{}).get("resolutionShort","-")
                    else:
                        if bc.get("homeAway") == "away":
                            bc_radio_aw = bc.get("name")
                        else:
                            bc_radio_hm = bc.get("name")

            recap_title = ""
            recap_desc = ""
            recap_url = ""
            recap_avail = False
            media = g.get("content",{}).get("media",{})
            epgAlt = media.get("epgAlternate",[{}])
            for e in epgAlt:
                if e.get("title") == "Daily Recap":
                    epg_items = e.get("items")
                    gotUrl = False
                    for i in epg_items:
                        recap_title = i.get("title","")
                        recap_desc = i.get("description")
                        for p in i.get("playbacks",[{}]):
                            playback_type = p.get("name")
                            if playback_type == "mp4Avc" or playback_type == "highBit":
                                recap_url = p.get("url")
                                gotUrl = True
                                recap_avail = True
                                break
                        if gotUrl is True:
                            break

            data.append([
                game_start,
                date_sched,
                date_resched,
                date_official,
                sched_dt,
                sched_time,
                resched_dt,
                resched_time,
                gamePk,
                game_type,
                venue_mlbam,
                venue_name,
                away_mlbam,
                away_name,
                away_score,
                away_rec,
                aw_pp_name,
                aw_pp_mlbam,
                home_mlbam,
                home_name,
                home_score,
                home_rec,
                hm_pp_name,
                hm_pp_mlbam,
                abstract_state,
                abstract_code,
                detailed_state,
                detailed_code,
                status_code,
                reason,
                bc_tv_aw,
                bc_tv_aw_res,
                bc_tv_hm,
                bc_tv_hm_res,
                bc_radio_aw, 
                bc_radio_hm,
                recap_url,
                recap_title,
                recap_desc,
                recap_avail
            ])
    
    df = pd.DataFrame(data=data,columns=cols)
    official_dt_col = pd.to_datetime(df["date_official"] + " " + df["game_start"],format=r"%Y-%m-%d %I:%M %p")
    df.insert(0,"official_dt",official_dt_col)

    return df

def game_highlights(mlbam=None,date=None,startDate=None,endDate=None,season=None,month=None):
    """
    Get video urls of team highlights for a specific date during the regular season.

    Params:
    -------
    - 'mlbam' (Required)
    - 'date' (conditionally required): format, mm/dd/yyyy
    """

    hydrations = "game(content(media(all),summary,gamenotes,highlights(highlights)))"
    if date is not None:
        date = dt.datetime.strptime(date,r"%m/%d/%Y").strftime(r"%Y-%m-%d")
        url = BASE + f"/schedule?sportId=1&teamId={mlbam}&date={date}&hydrate={hydrations}"
    elif month is not None:
        if season is not None:
            month = str(month)
            if month.isdigit():
                next_month_start = dt.datetime(year=int(season),month=int(month)+1,day=1)
                startDate = dt.datetime(year=int(season),month=int(month),day=1)
                endDate = next_month_start - dt.timedelta(days=1)
                startDate = startDate.strftime(r"%Y-%m-%d")
                endDate = endDate.strftime(r"%Y-%m-%d")
            else:
                if len(month) == 3:
                    month = dt.datetime.strptime(month,r"%b").month
                else:
                    month = dt.datetime.strptime(month,r"%B").month

                next_month_start = dt.datetime(year=int(season),month=int(month)+1,day=1)
                startDate = dt.datetime(year=int(season),month=int(month),day=1)
                endDate = next_month_start - dt.timedelta(days=1)
                startDate = startDate.strftime(r"%Y-%m-%d")
                endDate = endDate.strftime(r"%Y-%m-%d")

            url = BASE + f"/schedule?sportId=1&teamId={mlbam}&startDate={startDate}&endDate={endDate}&hydrate={hydrations}"

        else:
            print("Must specify a 'season' if using 'month' param")
            return None
    elif season is not None:
        url = BASE + f"/schedule?sportId=1&teamId={mlbam}&season={season}&hydrate={hydrations}"

    elif startDate is not None:
        if endDate is not None:
            startDate = dt.datetime.strptime(startDate,r"%m/%d/%Y").strftime(r"%Y-%m-%d")
            endDate = dt.datetime.strptime(endDate,r"%m/%d/%Y").strftime(r"%Y-%m-%d")

            url = BASE + f"/schedule?sportId=1&teamId={mlbam}&startDate={startDate}&endDate={endDate}&hydrate={hydrations}"

        else:
            print("Params 'startDate' & 'endDate' must be used together")
            return None
    else:
        print("One of params, 'date' or 'season' must be utilized")
        return None

    resp = requests.get(url)

    sched = resp.json()

    data = []
    columns = [
        "date",
        "gamePk",
        "game_num",
        "away_mlbam",
        "away_score",
        "home_mlbam",
        "home_score",
        "title",
        "blurb",
        "description",
        "url"
    ]
    for date in sched["dates"]:
        game_date = date["date"]
        games = date["games"]
        for gm in games:
            away = gm["teams"]["away"]
            home = gm["teams"]["home"]
            try:
                highlights = gm["content"]["highlights"]["highlights"]["items"]

                gamePk = gm["gamePk"]
                gameNumber = gm["gameNumber"]
                away_mlbam = away.get("team",{}).get("id")
                away_name = away.get("team",{}).get("name")
                away_score = away.get("score")
                home_mlbam = home.get("team",{}).get("id")
                home_name = home.get("team",{}).get("name")
                home_score = home.get("score")
                venue_mlbam = gm.get("venue",{}).get("id")
                venue_name = gm.get("venue",{}).get("name")

                for h in highlights:
                    h_title = h.get("title")
                    h_blurb = h.get("blurb")
                    h_desc = h.get("description")
                    playbacks = h.get("playbacks",[{}])

                    for p in playbacks:
                        playback_ext = p.get("name")
                        if playback_ext == "mp4Avc" or playback_ext == "highBit":
                            h_video_url = p.get("url")
                            break

                    data.append([
                        game_date,
                        gamePk,
                        gameNumber,
                        away_mlbam,
                        away_score,
                        home_mlbam,
                        home_score,
                        h_title,
                        h_blurb,
                        h_desc,
                        h_video_url
                    ])



            except:
                pass

    df = pd.DataFrame(data=data,columns=columns)

    return df

def get_video_link(playID,broadcast=None) -> str:
    if broadcast is not None:
        broadcast = str(broadcast).upper()
        url = f"https://baseballsavant.mlb.com/sporty-videos?playId={playID}&videoType={broadcast}"
    else:
        url = f"https://baseballsavant.mlb.com/sporty-videos?playId={playID}"
    resp = requests.get(url)
    soup = bs(resp.text,'lxml')
    video_tag = soup.find("video",id="sporty")
    video_source = video_tag.find("source")["src"]
    return video_source

def players_by_year(season):
    """Retrieve player data from the MLB Stats API

    Parameters
    ----------
    season : str or int
        search for players in a given season

    """

    url = BASE + f"/sports/1/players?season={season}&hydrate=person"
    resp = requests.get(url).json()
    return resp

def player_bio(mlbam):
    """Get short biography of player from Baseball-Reference.com's Player Bullpen pages.

    Parameters
    ----------

    mlbam : str or int
        player's official MLB ID
    
    """
    # URL to Player's Baseball-Reference page
    with requests.session() as sesh:
        url = f"https://www.baseball-reference.com/redirect.fcgi?player=1&mlb_ID={mlbam}"

        resp = sesh.get(url)

        soup = bs(resp.text,'lxml')

        # URL to Player's "Bullpen" page
        url = soup.find('a',text='View Player Info')['href']

        resp = sesh.get(url)

        soup = bs(resp.text,'lxml')

        bio_p_tags = soup.find("span",id="Biographical_Information").findParent('h2').find_next_siblings('p')

        return bio_p_tags

