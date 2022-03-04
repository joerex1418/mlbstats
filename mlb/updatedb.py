import requests
import pandas as pd

from .paths import (
    BIOS_CSV,
    HALL_OF_FAME_CSV,
    PEOPLE_CSV,
    YBY_RECORDS_CSV,
    VENUES_CSV,
    BBREF_BATTING_DATA_CSV,
    BBREF_PITCHING_DATA_CSV,
    LEAGUES_CSV
)

from .async_mlb import get_updated_records
from .async_mlb import get_bios

def update_bios(return_df=False,replace_existing=True):
    """Update player bios in the package's 'baseball.db'
    
    Arguments:
    -------------------
    return_df: default False
    replace_existing: default TRUE
                        if FALSE, function will simply return the data retrieved from the statsapi
                            without updating the database
    """
    df = get_bios(include_list_data=True)
    if replace_existing is False:
        return df
    # engine = create_engine(f"sqlite:///{os.path.abspath('simplestats/baseball.db')}")
    try:
        # print("opening connection")
        # conn = engine.connect()
        # df.to_sql("bios",con=conn,if_exists="replace")
        df.to_csv(BIOS_CSV,index=False)
        # print("SUCCESS! - closing connection")
        # conn.close()
        # engine.dispose()
        if return_df is True:
            return df
    except Exception as e:
        try:
            print(e)
            # print("'df.to_sql' FAILED - closing connection")
            # conn.close()
            # engine.dispose()
        except:
            pass
        if return_df is True:
            return df

def update_people(return_df=False,replace_existing=True):
    """Update yby records in the package's 'baseball.db'
    
    Arguments:
    -------------------
    return_df: default False
    replace_existing: default TRUE
                        if FALSE, function will simply return the data retrieved from the statsapi without updating the 
                            database
    """
    url = "https://raw.githubusercontent.com/chadwickbureau/register/master/data/people.csv"
    df = pd.read_csv(url)
    df = df.fillna("--")
    
    df = df[["key_mlbam","key_retro","key_bbref","key_bbref_minors","mlb_played_first","mlb_played_last","name_first","name_last","name_given"]]
    df = df[df["key_retro"]!="--"]
    df = df.rename(columns={"key_mlbam":"mlbam","key_retro":"retroID","key_bbref_minors":"bbrefIDminors","key_bbref":"bbrefID","mlb_played_first":"yearDebut","mlb_played_last":"yearRecent","name_first":"first","name_last":"last","name_given":"given"})
    df = df[df["mlbam"]!="--"]
    mlbams = []
    firsts = []
    lasts = []

    labels = ["bbrefID","first","last","given","mlbam","retroID","bbrefIDminors","yearDebut","yearRecent"]

    for i in df.mlbam:
        try:
            mlbams.append((int(i)))
        except:
            mlbams.append(i)
    for i in df.yearDebut:
        try:
            firsts.append((int(i)))
        except:
            firsts.append(i)
    for i in df.yearRecent:
        try:
            lasts.append((int(i)))
        except:
            lasts.append(i)

    df["mlbam"] = mlbams
    df["yearDebut"] = firsts
    df["yearRecent"] = lasts

    df = df[labels]
    df.reset_index(drop=True)    

    if replace_existing is False:
        return df

    # engine = create_engine(f"sqlite:///{os.path.abspath('simplestats/baseball.db')}")
    try:
        # print("opening connection")
        # conn = engine.connect()
        # df.to_sql("people",con=conn,if_exists="replace",dtype={"mlbam":types.Integer()})
        df.to_csv(PEOPLE_CSV,index=False)
        # print("SUCCESS! - closing connection")
        # conn.close()
        # engine.dispose()
        if return_df is True:
            return df
    except Exception as e:
        try:
            print(e)
            # print("'df.to_sql' FAILED - closing connection")
            # conn.close()
            # engine.dispose()
        except:
            pass
        if return_df is True:
            return df

def update_yby_records(return_df=False,replace_existing=True):
    """Update yby records in the package's 'baseball.db'
    
    Arguments:
    -------------------
    return_df: default False
    replace_existing: default TRUE
                        if FALSE, function will simply return the data retrieved from the statsapi
                            without updating the database
    """
    df = get_updated_records()
    df = df.sort_values(by=["season","W%"],ascending=[False,False])
    if replace_existing is False:
        return df
    # engine = create_engine(f"sqlite:///{os.path.abspath('simplestats/baseball.db')}")
    try:
        # print("opening connection")
        # conn = engine.connect()
        # df.to_sql("ybyRecords",con=conn,if_exists="replace")
        df.to_csv(YBY_RECORDS_CSV,index=False)
        # print("SUCCESS! - closing connection")
        # conn.close()
        # engine.dispose()
        if return_df is True:
            return df
    except Exception as e:
        try:
            print(e)
            # print("'df.to_sql' FAILED - closing connection")
            # conn.close()
            # engine.dispose()
        except:
            pass
        if return_df is True:
            return df

def update_hof(return_df=False,replace_existing=True):
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

    df = pd.DataFrame(data=recipients,columns=('player_mlbam','player','position','team','team_mlbam','date','award_id','award','ntoes','votes'))

    if replace_existing is False:
        return df
    # engine = create_engine(f"sqlite:///{os.path.abspath('simplestats/baseball.db')}")
    try:
        # conn = engine.connect()
        # df.to_sql("hof",con=conn,if_exists="replace")
        df.to_csv(HALL_OF_FAME_CSV,index=False)
        # conn.close()
        # engine.dispose()
        if return_df is True:
            return df
    except Exception as e:
        try:
            print(e)
            # print("'df.to_sql' FAILED - closing connection")
            # conn.close()
            # engine.dispose()
        except:
            pass
        if return_df is True:
            return df

def update_venues(return_df=False,replace_existing=True):
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
        "postalCode",
        "city",
        "state",
        "stateAbbrv",
        "country",
        "phone",
        "lat",
        "lon",
        "turfType",
        "roofType",
        "leftLine",
        "rightLine",
        "left",
        "center",
        "right",
        "leftCenter",
        "rightCenter",
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
        stateAbbrv = location.get("stateAbbrev")
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
            state,
            stateAbbrv,
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

    if replace_existing is True:
        df.to_csv(VENUES_CSV,index=False)

    if return_df is True:
        return df

def update_bbref_hitting_war(return_df=False,replace_existing=True):
    url = "https://www.baseball-reference.com/data/war_daily_bat.txt"
    df = pd.read_csv(url)
    if replace_existing is True:
        df.to_csv(BBREF_BATTING_DATA_CSV,index=False)
    if return_df is True:
        return df

def update_bbref_pitching_war(return_df=False,replace_existing=True):
    url = "https://www.baseball-reference.com/data/war_daily_pitch.txt"
    df = pd.read_csv(url)
    if replace_existing is True:
        df.to_csv(BBREF_PITCHING_DATA_CSV,index=False)
    if return_df is True:
        return df

def update_leagues(return_df=False,replace_existing=True):
    divs_url = "https://statsapi.mlb.com/api/v1/divisions?sportId=1&hydrate=league"
    lgs_url  = "https://statsapi.mlb.com/api/v1/leagues?sportId=1"

    data = []

    with requests.session() as sesh:
        divs_resp = sesh.get(divs_url)
        lgs_resp = sesh.get(lgs_url)

    for lg in lgs_resp.json()["leagues"]:
        data.append([
            lg.get("id"),
            lg.get("name"),
            lg.get("nameShort","-"),
            lg.get("abbreviation"),
            0,
            "-",
        ])

    for div in divs_resp.json()["divisions"]:
        data.append([
            div.get("id"),
            div.get("name"),
            div.get("nameShort","-"),
            div.get("abbreviation"),
            div.get("league",{}).get("id",0),
            div.get("league",{}).get("name","-"),
        ])
    
    df = pd.DataFrame(data=data,columns=['mlbam','name_full','name_short','abbreviation','parent_mlbam','parent_name'])

    if replace_existing is True:
        df.to_csv(LEAGUES_CSV,index=False)
    if return_df is True:
        return df