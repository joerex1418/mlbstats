#%%
import json
import requests
import pandas as pd
# from sqlalchemy import create_engine

from .utils import curr_year

from .constants import BASE
from .constants import STATDICT
from .constants import YBY_BAT_FIELDS
from .constants import YBY_PITCH_FIELDS
from .constants import YBY_FIELD_FIELDS
from .constants import YBY_BAT_FIELDS_ADV
from .constants import YBY_PITCH_FIELDS_ADV
from .constants import STAT_GROUPS
from .constants import LEADER_CATEGORIES

from .mlbdata import get_franchise_df
from .mlbdata import get_playoff_history
from .mlbdata import get_standings_df
from .mlbdata import get_yby_records


def franchise_info(teamID):
    """
    Returns a dictionary with the following keys:
    ---------------------------------------------
    firstYear, recentYear, mlbam, franchID, bbrefID, retroID, fileCode, lgDiv, venueCurrent_mlbam, venueList, venueDict
    """

    franchise_df = get_franchise_df(teamID)
    postseason_appearances = get_playoff_history(teamID)
    year = curr_year

    df_ref = franchise_df.iloc[0]
    firstYear = df_ref["firstYear"]
    recentYear = df_ref["yearID"]
    fullName = df_ref["fullName"]
    locationName = df_ref["locationName"]
    clubName = df_ref["clubName"]
    mlbam = df_ref["mlbam"]
    franchID = df_ref["franchID"]
    bbrefID = df_ref["bbrefID"]
    retroID = df_ref["retroID"]
    fileCode = df_ref["fileCode"]
    lgDiv = df_ref["lgAbbrv"]
    curr_venue_mlbam = franchise_df.iloc[0]["venue_mlbam"]
    venues_list = []
    venues = {}

    for venue in list(franchise_df["venueName"]):
        if venue not in venues: venues_list.append(venue)
    for venue in venues_list:
        ven_df = franchise_df[franchise_df["venueName"]==venue]
        ven_min = ven_df[["yearID"]].min().item()
        ven_max = ven_df[["yearID"]].max().item()
        venues[venue] = f"({ven_min}-{ven_max})"

    return {
        "franchise_df":franchise_df,
        "postseason_appearances":postseason_appearances,
        "firstYear":firstYear,
        "recentYear":recentYear,
        "fullName":fullName,
        "locationName":locationName,
        "clubName":clubName,
        "mlbam":mlbam,
        "franchID":franchID,
        "bbrefID":bbrefID,
        "retroID":retroID,
        "fileCode":fileCode,
        "lgDiv":lgDiv,
        "venueCurrent_mlbam":curr_venue_mlbam,
        "venueDict":venues,
        "venueList":venues_list}

def franchise_records(teamID):
    """Get all records data for a specific team
    
    teamID:  'mlbam' (recommended) or 'bbrefID'
    """
    # ybyRecords = get_standings_df()
    ybyRecords = get_yby_records()
    try:
        teamID = int(teamID)
        ybyRecords = ybyRecords[ybyRecords['tm_mlbam']==teamID]
    except:
        ybyRecords = ybyRecords[ybyRecords['tm_bbrefID']==teamID]

    # ybyRecords.rename(columns=STATDICT,inplace=True)
    return ybyRecords

def franchise_stats(mlbam):
    # ============================================== YBY Stats ===================================================
    url = f"http://statsapi.mlb.com/api/v1/teams/{mlbam}/stats?stats=yearByYear,yearByYearAdvanced&group=hitting,pitching,fielding"

    yby_df_dict = {
        "hitting":None,
        "hittingAdv":None,
        "pitching":None,
        "pitchingAdv":None,
        "fielding":None}
    response = json.loads(requests.get(url).text)["stats"]
    for item in response:
        statType = item["type"]["displayName"]
        grp = item["group"]["displayName"]
        yby_stats = item["splits"]
        all_years_rows = []
        if grp == "hitting" and statType == "yearByYear":
            for year in yby_stats:
                season = int(year["season"])
                year_row = [season]
                for stat_item in YBY_BAT_FIELDS:
                    try:
                        year_row.append(year["stat"][stat_item])
                    except:
                        year_row.append("--")
                all_years_rows.append(year_row)
            df = pd.DataFrame(data=all_years_rows,columns=["Season"]+list(YBY_BAT_FIELDS)).sort_values(by="Season",ascending=False) #######
            yby_df_dict["hitting"] = df.rename(columns=STATDICT)
        elif grp == "hitting" and statType == "yearByYearAdvanced":
            for year in yby_stats:
                season = int(year["season"])
                year_row = [season]
                for stat_item in YBY_BAT_FIELDS_ADV: ########
                    try:
                        year_row.append(year["stat"][stat_item])
                    except:
                        year_row.append("--")
                all_years_rows.append(year_row)
            df = pd.DataFrame(data=all_years_rows,columns=["Season"]+list(YBY_BAT_FIELDS_ADV)).sort_values(by="Season",ascending=False) #######
            yby_df_dict["hittingAdv"] = df.rename(columns=STATDICT)
        elif grp == "pitching" and statType == "yearByYear":
            for year in yby_stats:
                season = int(year["season"])
                year_row = [season]
                for stat_item in YBY_PITCH_FIELDS:
                    try:
                        year_row.append(year["stat"][stat_item])
                    except:
                        year_row.append("--")
                all_years_rows.append(year_row)
            df = pd.DataFrame(data=all_years_rows,columns=["Season"]+list(YBY_PITCH_FIELDS)).sort_values(by="Season",ascending=False)
            yby_df_dict["pitching"] = df.rename(columns=STATDICT)
        elif grp == "pitching" and statType == "yearByYearAdvanced": 
            for year in yby_stats:
                season = int(year["season"])
                year_row = [season]
                for stat_item in YBY_PITCH_FIELDS_ADV: 
                    try:
                        year_row.append(year["stat"][stat_item])
                    except:
                        year_row.append("--")
                all_years_rows.append(year_row)
            df = pd.DataFrame(data=all_years_rows,columns=["Season"]+list(YBY_PITCH_FIELDS_ADV)).sort_values(by="Season",ascending=False)
            yby_df_dict["pitchingAdv"] = df.rename(columns=STATDICT)
        elif grp == "fielding" and statType == "yearByYear":
            for year in yby_stats:
                season = int(year["season"])
                year_row = [season]
                for stat_item in YBY_FIELD_FIELDS:
                    try:
                        year_row.append(year["stat"][stat_item])
                    except:
                        year_row.append("--")
                all_years_rows.append(year_row)
            df = pd.DataFrame(data=all_years_rows,columns=["Season"]+list(YBY_FIELD_FIELDS)).sort_values(by="Season",ascending=False)
            yby_df_dict["fielding"] = df.rename(columns=STATDICT)
        else:pass

    return yby_df_dict
    # ============================================================================================================

def franchise_leaders(mlbam,categories=None,STAT_GROUPS=STAT_GROUPS,gameTypes="R",playerPool="qualified",season="2021"):
    if categories is None:
        categories = LEADER_CATEGORIES

def records(mlbam,season):
    url = BASE + f"/teams/{mlbam}?hydrate=standings(season={season})"