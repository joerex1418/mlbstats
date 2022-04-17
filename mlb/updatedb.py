import requests
from typing import Union


import pandas as pd
import numpy as np

from .constants import COLS_SEASON

from .paths import (
    HALL_OF_FAME_CSV,
    PEOPLE_CSV,
    YBY_RECORDS_CSV,
    VENUES_CSV,
    BBREF_BATTING_DATA_CSV,
    BBREF_PITCHING_DATA_CSV,
    BBREF_DATA_CSV,
    LEAGUES_CSV,
    SEASONS_CSV,
    PITCH_CODES_CSV,
    PITCH_TYPES_CSV,
    EVENT_TYPES_CSV,
)
# from .async_mlb import fetch
from .async_mlb import get_updated_records

def update_people(inplace=True) -> Union[pd.DataFrame,None]:
    """Update 'people' in the library's CSV files
    
    Parameters:
    -----------
    inplace : bool default True
        if False, function will simply return the data retrieved from the API 
        without updating the current CSV file
        
    """
    url = "https://raw.githubusercontent.com/chadwickbureau/register/master/data/people.csv"
    df = pd.read_csv(url,low_memory=False)
    
    df = df[["key_mlbam","key_retro","key_bbref","key_bbref_minors","mlb_played_first","mlb_played_last","name_first","name_last","name_given"]]
    df = df.fillna("--")
    df = df[df["key_retro"]!="--"]
    df = df.rename(columns={"key_mlbam":"mlbam","key_retro":"retroID","key_bbref_minors":"bbrefIDminors","key_bbref":"bbrefID","mlb_played_first":"year_debut","mlb_played_last":"year_recent","name_first":"name_first","name_last":"name_last","name_given":"name_given"})
    df = df[df["mlbam"]!="--"]

    columns = ["name_first","name_last","name_given","mlbam","bbrefID","bbrefIDminors","retroID","year_debut","year_recent"]
    for idx, row in df.iterrows():
        try:
            row['mlbam'] = int(row['mlbam'])
        except:
            row['mlbam'] = np.nan
        try:
            row['year_debut'] = int(row['year_debut'])
        except:
            row['year_debut'] = np.nan
        try:
            row['year_recent'] = int(row['year_recent'])
        except:
            row['year_recent'] = np.nan

    df = df[columns]
    df[['mlbam','year_debut','year_recent']] = df[['mlbam','year_debut','year_recent']].fillna(0)
    df = df.astype({'year_debut':'int32','year_recent':'int32','mlbam':'int32'})
    df.reset_index(drop=True,inplace=True)

    if inplace is False:
        return df
    else:
        df.to_csv(PEOPLE_CSV,index=False)
        
def update_yby_records(inplace=True) -> Union[pd.DataFrame,None]:
    """Update yby records in the package's 'baseball.db'
    
    Parameters:
    -----------
    inplace : bool default True
        if False, function will simply return the data retrieved from the API 
        without updating the current CSV file
        
    """
    df = get_updated_records()
    df = df.sort_values(by=["season","W%"],ascending=[False,False])
    
    if inplace is False:
        return df
    else:
        df.to_csv(YBY_RECORDS_CSV,index=False)

def update_hof(inplace=True) -> Union[pd.DataFrame,None]:
    """Update "Hall Of Fame" data in the library's CSV files
    
    Parameters:
    -----------
    inplace : bool default True
        if False, function will simply return the data retrieved from the API 
        without updating the current CSV file
        
    """
    url = "https://statsapi.mlb.com/api/v1/awards/MLBHOF/recipients?sportId=1&hydrate=results,team"

    response = requests.get(url)
    recipients = []
    for r in response.json()["awards"]:
        a_date = r["date"]
        a_id = r["id"]
        a_name = r["name"]
        a_notes = r.get("notes","-")
        p_mlbam = r["player"]["id"]
        p_name = r["player"]["nameFirstLast"]
        p_pos = r["player"]["primaryPosition"]["abbreviation"]
        team_name = r["team"].get("name","-")
        team_id = r["team"]["id"]
        votes = r.get("votes","-")
        entry = (p_mlbam,p_name,p_pos,team_name,team_id,a_date,a_id,a_name,a_notes,votes)
        recipients.append(entry)

    df = pd.DataFrame(data=recipients,columns=('player_mlbam','player','position','team','tm_mlbam','date','award_id','award','ntoes','votes'))

    if inplace is False:
        return df
    else:
        df.to_csv(HALL_OF_FAME_CSV,index=False)

def update_seasons(inplace=True) -> Union[pd.DataFrame,None]:
    """Update 'seasons' data in the library's CSV files
    
    Parameters:
    -----------
    inplace : bool default True
        if False, function will simply return the data retrieved from the API 
        without updating the current CSV file
        
    """
    cols = COLS_SEASON
    date_cols = ['preSeasonStartDate','preSeasonEndDate','seasonStartDate','seasonEndDate','springStartDate','springEndDate','regularSeasonStartDate','regularSeasonEndDate','allStarDate','postSeasonStartDate','postSeasonEndDate','offseasonStartDate','offSeasonEndDate']
    new_date_cols = []
    new_date_cols_map = {'seasonId':'season',
                         'allStarDate':'allstar_game',
                         'preSeasonStartDate':'preseason_start',
                         'preSeasonEndDate':'preseason_end',
                         'seasonStartDate':'season_start',
                         'seasonEndDate':'season_end',
                         'springStartDate':'spring_start',
                         'springEndDate':'spring_end',
                         'regularSeasonStartDate':'regular_start',
                         'regularSeasonEndDate':'regular_end',
                         'postSeasonStartDate':'postseason_start',
                         'postSeasonEndDate':'postseason_end',
                         'offseasonStartDate':'offseason_start',
                         'offSeasonStartDate':'offseason_start',
                         'offSeasonEndDate':'offseason_end',
                         'hasWildCard':'has_wildcard',
                         'qualifierPlateAppearances':'qualifier_pas',
                         'qualifierOutsPitched':'qualifier_outs_pitched'
                         }
    for c in date_cols:
        new_date_cols.append(new_date_cols_map[c])
        
    url = "https://statsapi.mlb.com/api/v1/seasons/all?sportId=1"
    
    resp = requests.get(url)

    data = []
    for s in resp.json()["seasons"]:
        data.append(pd.Series(s))
    
    df = pd.DataFrame(data=data).sort_values(by='seasonId',ascending=False).rename(columns=new_date_cols_map)#[cols]
    df[new_date_cols] = df[new_date_cols].apply(pd.to_datetime,format=r"%Y-%m-%d")
    df.drop(columns=['seasonLevelGamedayType','gameLevelGamedayType','firstDate2ndHalf','lastDate1stHalf'],inplace=True)


    if inplace is False:
        df : pd.DataFrame = df
        return df
    else:
        df.to_csv(SEASONS_CSV,index=False)

def update_venues(inplace=True) -> Union[pd.DataFrame,None]:
    """Update 'venues' data in the library's CSV files
    
    Parameters:
    -----------
    inplace : bool default True
        if False, function will simply return the data retrieved from the API 
        without updating the current CSV file
        
    """
    base = "https://statsapi.mlb.com/api/v1"
    hydrations = "location,social,timezone,fieldInfo,metadata,images,xrefId,video"
    url = base + f"/venues?hydrate={hydrations}"

    resp = requests.get(url)

    venues = resp.json()["venues"]

    data = []

    cols = [
        "mlbam",
        "name",
        "active",
        "address1",
        "address2",
        "postal_code",
        "city",
        "state",
        "country",
        "phone",
        "lat",
        "lon",
        "turf_type",
        "roof_type",
        "left_line",
        "right_line",
        "left",
        "center",
        "right",
        "left_center",
        "right_center",
        "tz_id",
        "tz_offset",
        "tz",
        "retroID"]

    for v in venues:
        location = v.get("location",{})
        coords = location.get("defaultCoordinates",{})
        timeZone = v.get("timeZone",{})
        fi = v.get("fieldInfo",{})
        xrefs = v.get("xrefIds",[])

        mlbam = v.get("id")
        name = v.get("name")
        address1 = location.get("address1")
        address2 = location.get("address2")
        postalCode = location.get("postalCode")
        city = location.get("city")
        state = location.get("state")
        state_abbrv = location.get("stateAbbrev")
        country = location.get("country")
        phone = location.get("phone")
        lat = coords.get("latitude")
        lon = coords.get("longitude")
        tz_id = timeZone.get("id")
        tz_offset = timeZone.get("offset")
        tz = timeZone.get("tz")
        turfType = fi.get("turfType")
        roofType = fi.get("roofType")
        leftLine = fi.get("leftLine")
        rightLine = fi.get("rightLine")
        left = fi.get("left")
        center = fi.get("center")
        right = fi.get("right")
        leftCenter = fi.get("leftCenter")
        rightCenter = fi.get("rightCenter")
        active = v.get("active",False)
        retroID = None
        for x in xrefs:
            if x["xrefType"] == "retrosheet":
                retroID = x["xrefId"]
                break
        
        data.append([
            mlbam,
            name,
            active,
            address1,
            address2,
            postalCode,
            city,
            state_abbrv,
            country,
            phone,
            lat,
            lon,
            turfType,
            roofType,
            leftLine,
            rightLine,
            left,
            center,
            right,
            leftCenter,
            rightCenter,
            tz_id,
            tz_offset,
            tz,
            retroID
        ])

    df = pd.DataFrame(data=data,columns=cols)
    
    df = df[df["country"]=="USA"]
    
    df = df.astype({'mlbam':'int32','tz_offset':'int32'})

    if inplace is False:
        return df
    else:
        df.to_csv(VENUES_CSV,index=False)

def update_bbref_data(inplace=True) -> Union[pd.DataFrame,None]:
    url = "https://www.baseball-reference.com/data/war_daily_bat.txt"
    hit = pd.read_csv(url)
    hit = hit.drop_duplicates(subset='mlb_ID',keep='first')[['name_common','mlb_ID','player_ID']].dropna()
    hit = hit.astype({'mlb_ID':'int32'})
    
    url = "https://www.baseball-reference.com/data/war_daily_pitch.txt"
    pit = pd.read_csv(url)
    pit = pit.drop_duplicates(subset='mlb_ID',keep='first')[['name_common','mlb_ID','player_ID']].dropna()
    pit = pit.astype({'mlb_ID':'int32'})
    
    df = pd.concat([hit,pit]).drop_duplicates(subset='mlb_ID',keep='first').rename(columns={'name_common':'name','mlb_ID':'mlbam','player_ID':'bbrefID'})
    
    if inplace is False:
        return df.reset_index(drop=True)
    else:
        df.to_csv(BBREF_DATA_CSV,index=False)
        
def update_leagues(inplace=True) -> Union[pd.DataFrame,None]:
    """Update 'leagues' data in the library's CSV files
    
    Parameters:
    -----------
    inplace : bool default True
        if False, function will simply return the data retrieved from the API 
        without updating the current CSV file
        
    """
    
    divs_url = "https://statsapi.mlb.com/api/v1/divisions?sportId=1&hydrate=league"
    lgs_url  = "https://statsapi.mlb.com/api/v1/leagues?sportId=1"

    data = [
        [0,'-','-','-','-',0,'-']
    ]

    with requests.session() as sesh:
        divs_resp = sesh.get(divs_url)
        lgs_resp = sesh.get(lgs_url)

    for lg in lgs_resp.json()["leagues"]:
        data.append([
            lg.get("id"),
            lg.get("name"),
            lg.get("nameShort","-"),
            '-',
            lg.get("abbreviation"),
            0,
            "-",
        ])

    for div in divs_resp.json()["divisions"]:
        data.append([
            div.get("id"),
            div.get("name"),
            div.get("nameShort","-"),
            div.get("nameShort","-")[3:],
            div.get("abbreviation"),
            div.get("league",{}).get("id",0),
            div.get("league",{}).get("name","-"),
        ])
    
    df = pd.DataFrame(data=data,columns=['mlbam','name_full','name_short','div_part','abbreviation','parent_mlbam','parent_name'])

    if inplace is False:
        return df
    else:
        df.to_csv(LEAGUES_CSV,index=False)
    
def update_bbref_hitting_war(inplace=True) -> Union[pd.DataFrame,None]:
    url = "https://www.baseball-reference.com/data/war_daily_bat.txt"
    df = pd.read_csv(url)
    if inplace is False:
        return df
    else:
        df.to_csv(BBREF_BATTING_DATA_CSV,index=False)

def update_bbref_pitching_war(inplace=True) -> Union[pd.DataFrame,None]:
    url = "https://www.baseball-reference.com/data/war_daily_pitch.txt"
    df = pd.read_csv(url)
    if inplace is False:
        return df
    else:
        df.to_csv(BBREF_PITCHING_DATA_CSV,index=False)

def update_pitch_types(inplace=True) -> Union[pd.DataFrame,None]:
    """Update 'pitch_types' in the library's CSV files
    
    Parameters:
    -----------
    inplace : bool default True
        if False, function will simply return the data retrieved from the API 
        without updating the current CSV file
        
    """
    url = 'https://statsapi.mlb.com/api/v1/pitchTypes'
    resp = requests.get(url)
    data = []
    for p in resp.json():
        data.append({'code':p['code'],'description':p['description']})

    df = pd.DataFrame.from_dict(data).sort_values(by='code')
    df = df.reset_index(drop=True,inplace=False)

    if inplace is not True:
        return df
    
    df.to_csv(PITCH_TYPES_CSV,index=False)
    
def update_pitch_codes(inplace=True) -> Union[pd.DataFrame,None]:
    """Update 'pitch_codes' in the library's CSV files
    
    Parameters:
    -----------
    inplace : bool default True
        if False, function will simply return the data retrieved from the API 
        without updating the current CSV file
        
    """
    url = 'https://statsapi.mlb.com/api/v1/pitchCodes'
    resp = requests.get(url)
    data = []
    for p in resp.json():
        data.append({'code':p['code'],'description':p['description']})

    df = pd.DataFrame.from_dict(data).sort_values(by='code')
    df = df.reset_index(drop=True,inplace=False)
    
    if inplace is False:
        return df
    
    df.to_csv(PITCH_CODES_CSV,index=False)
    
def update_event_types(inplace=True) -> Union[pd.DataFrame,None]:
    """Update 'event_types' in the library's CSV files
    
    Parameters:
    -----------
    inplace : bool default True
        if False, function will simply return the data retrieved from the API 
        without updating the current CSV file
        
    """
    url = 'https://statsapi.mlb.com/api/v1/eventTypes'
    resp = requests.get(url)
    data = []
    for e in resp.json():
        e_type_data = {'code':e['code'],
                       'description':e['description'],
                       'hit':e['hit'],
                       'plateAppearance':e['plateAppearance'],
                       'baseRunningEvent':e['baseRunningEvent']}
        data.append(e_type_data)

    df = pd.DataFrame.from_dict(data)
    df = df.reset_index(drop=True,inplace=False)
    
    if inplace is False:
        return df
    
    df.to_csv(EVENT_TYPES_CSV,index=False)
    
