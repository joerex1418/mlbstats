import re
import requests
import pandas as pd

from bs4 import BeautifulSoup as bs
from bs4.element import Comment

from .mlbdata import get_teams_df
from .mlbdata import get_people_df

from .constants import BASE
from .constants import STATDICT
from .constants import BAT_FIELDS
from .constants import BAT_FIELDS_ADV
from .constants import PITCH_FIELDS
from .constants import PITCH_FIELDS_ADV
from .constants import FIELD_FIELDS
from .constants import bbref_hit_log_fields
from .constants import bbref_pitch_log_fields
from .constants import bbref_field_log_fields
from .constants import BBREF_SPLITS
from .constants import sitCodes

from .utils import curr_year

# get player info and stats
def player(playerID) -> dict:
    '''Returns a dictionary of player's basic bio and a number of statistic dataframes in dictionary'''
    people = get_people_df()
    try:
        mlbam = int(playerID)
        player_df = people.set_index("mlbam").loc[mlbam]
        bbrefID = player_df["bbrefID"]
        retroID = player_df["retroID"]
        bbrefIDminors = player_df["bbrefIDminors"]
    except:
        bbrefID = playerID
        player_df = people.set_index("bbrefID").loc[bbrefID]
        mlbam = int(player_df["mlbam"])
        retroID = player_df["retroID"]
        bbrefIDminors = player_df["bbrefIDminors"]
    statTypes = "career,careerAdvanced,yearByYear,yearByYearAdvanced,pitchArsenal"
    sitCodes = "[h,a]"
    statGroups = "hitting,fielding,pitching"
    hydrate = f"stats(type=[{statTypes}],group=[{statGroups}],gameType=[R,P],sitCodes={sitCodes}),rosterEntries,education,xrefId,draft,awards,transactions"
    url = BASE + f"/people?personIds={mlbam}&hydrate={hydrate}"
    playerPage = requests.get(url).json()
    
    player = playerPage["people"][0]
    
    primaryPosition = player["primaryPosition"]["abbreviation"]
    fullName = player["fullFMLName"]
    firstName = player["firstName"]
    lastName = player["lastName"]
    nickName = player.get("nickName","--")
    primaryNumber = player["primaryNumber"]
    birthDate = player["birthDate"]
    currentAge = player["currentAge"]
    birthCity = player.get("birthCity","")
    birthState = player.get("birthStateProvince","")
    birthCountry = player.get("birthCountry","")
    deathDate = player.get("deathDate","")
    deathCity = player.get("deathCity","")
    deathState = player.get("deathStateProvince","")
    deathCountry = player.get("deathCountry","")
    weight = player["weight"]
    height = player["height"]
    bats = player["batSide"]["code"]
    throws = player["pitchHand"]["code"]
    education = player.get("education","")
    mlbDebutDate = player["mlbDebutDate"]
    firstYear = player_df[["yearDebut"]].item()
    lastYear = player_df[["yearRecent"]].item()
    zoneTop = player["strikeZoneTop"]
    zoneBot = player["strikeZoneBottom"]
    isActive = player["active"]
    awards = player.get("awards",None)
    drafts = player.get("drafts",None)
    transactions = player.get("transactions",None)
    stats_list = player["stats"]
    rosterEntries = player["rosterEntries"]

    active_range = list(range(int(mlbDebutDate[:4]),curr_year+1))
    team_df = get_teams_df()
    team_df = team_df[team_df["yearID"].isin(active_range)]

    drafts_df = None
    trx_df = None
    
    try:
        all_rows = []
        for a in awards:
            award_id = a.get("id","-")
            award_name = a.get("name","-")
            award_date = a.get("date","-")
            award_season = a.get("season","-")
            award_team = a.get("team",{}).get("teamName")
            row = [award_id,award_name,award_date,award_season,award_team]
            all_rows.append(row)
        awards_df = pd.DataFrame(data=all_rows,columns=("award_id","award","date","season","team"))
    except:
        awards_df = None

    try:
        draft_columns = ('year','draft_code','draft_type','draft_round','pick_number','round_pickNumber','team','team_mlbam','pos','school')
        draft_rows = []
        for d in drafts:
            pickRound = d.get("pickRound","-")
            pickNumber = d.get("pickNumber","-")
            roundPickNum = d.get("roundPickNumber","-")
            team = d.get("team",{}).get("name","-")
            team_mlbam = d.get("team",{}).get("id","")
            player = d.get("person")
            p_name = player.get("fullName")
            p_mlbam = player.get("id")
            bats = player.get("batSide",{}).get("code","-")
            throws = player.get("pitchHand",{}).get("code","-")
            school = d.get("school",{}).get("name","-")
            draftYear = d.get("year","-")
            pos = player.get("primaryPosition",{}).get("abbreviation","-")
            if pos == "RHP" or pos == "LHP": pos = "P"
            draftTypeCode = d.get("draftType",{}).get("code","-")
            draftType = d.get("draftType",{}).get("description","-")

            row = [draftYear,draftTypeCode,draftType,pickRound,pickNumber,roundPickNum,team,team_mlbam,pos,school]

            draft_rows.append(row)

        drafts_df = pd.DataFrame(data=draft_rows,columns=draft_columns)
    except:
        # print("no draft info found")
        drafts_df = None
    
    try:
        trx_columns = ("name","mlbam","tr_type","tr","description","date","e_date","r_date","fr","fr_mlbam","to","to_mlbam")
        all_rows = []
        for t in transactions:
            person = t.get("person",{})
            p_name = person.get("fullName","")
            p_mlbam = person.get("id","")
            typeCode = t.get("typeCode","")
            typeTr = t.get("typeDesc")
            desc = t.get("description")

            if "fromTeam" in t.keys():
                fr = t.get("fromTeam")
                fromTeam = fr.get("name","")
                fromTeam_mlbam = fr.get("id","")
            else:
                fromTeam = "-"
                fromTeam_mlbam = ""

            if "toTeam" in t.keys():
                to = t.get("toTeam")
                toTeam = to.get("name","")
                toTeam_mlbam = to.get("id","")
            else:
                toTeam = "-"
                toTeam_mlbam = ""

            date = t.get("date","--")
            eDate = t.get("effectiveDate","--")
            rDate = t.get("resolutionDate","--")

            row = [p_name,p_mlbam,typeCode,typeTr,desc,date,eDate,rDate,fromTeam,fromTeam_mlbam,toTeam,toTeam_mlbam]
            
            all_rows.append(row)

        trx_df = pd.DataFrame(data=all_rows,columns=trx_columns)
    except:
        trx_df = None
    
    try:
        rostEntry_cols = ("jerseyNumber","position","status","team","team_mlbam","fromDate","toDate","statusDate","fortyMan","active")
        all_rost_entries = []
        for entry in rosterEntries:
            row = [
                entry.get("jerseyNumber","-"),
                entry.get("position",{}).get("abbreviation",""),
                entry.get("status",{}).get("description",{}),
                entry.get("team",{}).get("name",""),
                entry.get("team",{}).get("id",""),
                entry.get("startDate","-"),
                entry.get("endDate","-"),
                entry.get("statusDate","-"),
                entry.get("isActiveFortyMan",False),
                entry.get("isActive",False)
            ]
            all_rost_entries.append(row)
        rost_entries_df = pd.DataFrame(data=all_rost_entries,columns=rostEntry_cols)

    except Exception as e:
        print("ERROR: ",e)
        rost_entries_df = None



    hitting = {
        "careerReg":False,
        "careerPost":False,
        "careerAdvReg":False,
        "careerAdvPost":False,
        "yby":False,
        "ybyAdv":False,
        "recentSeason":False,
        "pitchArsenal":False}

    pitching = {
        "careerReg":False,
        "careerPost":False,
        "careerAdvReg":False,
        "careerAdvPost":False,
        "yby":False,
        "ybyAdv":False,
        "recentSeason":False,
        "pitchArsenal":False}

    fielding = {
        "careerReg":False,
        "careerPost":False,
        "yby":False,
        "recentSeason":False,
        "pitchArsenal":False}

    
   # === Generate header lists ===========
    bat_headers = []
    bat_advheaders = []

    for f in BAT_FIELDS:
        bat_headers.append(STATDICT[f])
    for f in BAT_FIELDS_ADV:
        bat_advheaders.append(STATDICT[f])

    pitch_headers = []
    pitch_advheaders = []

    for f in PITCH_FIELDS:
        pitch_headers.append(STATDICT[f])
    for f in PITCH_FIELDS_ADV:
        pitch_advheaders.append(STATDICT[f])

    field_headers = []

    for f in FIELD_FIELDS:
        field_headers.append(STATDICT[f])
   # =====================================       

    for stat_item in stats_list:
        if stat_item["type"]["displayName"] == "pitchArsenal":
            pass

    # === CAREER ===================================================================================================================

        # CAREER HITTING ===========================================================================================
        elif stat_item["group"]["displayName"] == "hitting" and stat_item["type"]["displayName"] == "career":
            for split in stat_item["splits"]:
                if split["gameType"] == "R":
                    player_stats = []
                    addingTo = "careerReg"
                    for field in BAT_FIELDS:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=bat_headers)
                    hitting[addingTo] = df
                            
                elif split["gameType"] == "P":
                    player_stats = []
                    addingTo = "careerPost"
                    for field in BAT_FIELDS:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=bat_headers)
                    hitting[addingTo] = df
                else:pass
        elif stat_item["group"]["displayName"] == "hitting" and stat_item["type"]["displayName"] == "careerAdvanced":
            for split in stat_item["splits"]:
                if split["gameType"] == "R":
                    player_stats = []
                    addingTo = "careerAdvReg"
                    for field in BAT_FIELDS_ADV:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=bat_advheaders)
                    hitting[addingTo] = df
                            
                elif split["gameType"] == "P":
                    player_stats = []
                    addingTo = "careerAdvPost"
                    for field in BAT_FIELDS_ADV:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=bat_advheaders)
                    hitting[addingTo] = df
                    
                # elif split["gameType"] == "S":
                #     player_stats = []
                #     addingTo = "careerAdvSpring"
                #     for field in BAT_FIELDS_ADV:
                #         try:player_stats.append(split["stat"][field])
                #         except:player_stats.append("--")

                #     df = pd.DataFrame(data=[player_stats],columns=bat_advheaders)
                #     hitting[addingTo] = df
                else:pass
        # ====================================================================================== CAREER HITTING ====

        # CAREER PITCHING =====================================================================================
        elif stat_item["group"]["displayName"] == "pitching" and stat_item["type"]["displayName"] == "career":
            for split in stat_item["splits"]:
                if split["gameType"] == "R":
                    player_stats = []
                    addingTo = "careerReg"
                    for field in PITCH_FIELDS:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=pitch_headers)
                    pitching[addingTo] = df
                            
                elif split["gameType"] == "P":
                    player_stats = []
                    addingTo = "careerPost"
                    for field in PITCH_FIELDS:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=pitch_headers)
                    pitching[addingTo] = df
                    
                # elif split["gameType"] == "S":
                #     player_stats = []
                #     addingTo = "careerSpring"
                #     for field in PITCH_FIELDS:
                #         try:player_stats.append(split["stat"][field])
                #         except:player_stats.append("--")

                #     df = pd.DataFrame(data=[player_stats],columns=pitch_headers)
                #     pitching[addingTo] = df
                else:pass
        elif stat_item["group"]["displayName"] == "pitching" and stat_item["type"]["displayName"] == "careerAdvanced":
            for split in stat_item["splits"]:
                if split["gameType"] == "R":
                    player_stats = []
                    addingTo = "careerAdvReg"
                    for field in PITCH_FIELDS_ADV:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=pitch_advheaders)
                    pitching[addingTo] = df
                            
                elif split["gameType"] == "P":
                    player_stats = []
                    addingTo = "careerAdvPost"
                    for field in PITCH_FIELDS_ADV:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=pitch_advheaders)
                    pitching[addingTo] = df
                    
                # elif split["gameType"] == "S":
                #     player_stats = []
                #     addingTo = "careerAdvSpring"
                #     for field in PITCH_FIELDS_ADV:
                #         try:player_stats.append(split["stat"][field])
                #         except:player_stats.append("--")

                #     df = pd.DataFrame(data=[player_stats],columns=pitch_advheaders)
                #     pitching[addingTo] = df
                else:pass
        # ===================================================================================== CAREER PITCHING ====

        # CAREER FIELDING =====================================================================================
        elif stat_item["group"]["displayName"] == "fielding" and stat_item["type"]["displayName"] == "career":
            fielding_dfs = []
            fielding_dfs_post = []
            for split in stat_item["splits"]:
                if split["gameType"] == "R":
                    pos = split["position"]["abbreviation"]
                    player_stats = [pos]
                    addingTo = "careerReg"
                    for field in FIELD_FIELDS:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=["Pos"]+field_headers)
                    fielding_dfs.append(df)
                
                            
                elif split["gameType"] == "P":
                    pos = split["position"]["abbreviation"]
                    player_stats = [pos]
                    addingTo = "careerPost"
                    for field in FIELD_FIELDS:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=["Pos"]+field_headers)
                    fielding_dfs_post.append(df)

                    
                else:pass
                
                

            fielding["careerReg"] = pd.concat(fielding_dfs)
            fielding["careerPost"] = pd.concat(fielding_dfs_post)
        # ===================================================================================== CAREER FIELDING ====



    # === YEAR-BY-YEAR =============================================================================================================

        # YEAR-BY-YEAR HITTING ==========================================================================================
        elif stat_item["group"]["displayName"] == "hitting" and stat_item["type"]["displayName"] == "yearByYear":
            years = []
            addingTo = "yby"
            for split in stat_item["splits"]:
                if "numTeams" in split.keys():
                    pass
                else:
                    if split["gameType"] == "R":
                        season = split["season"]
                        teamId = split["team"]["id"]
                        row = team_df[(team_df["yearID"]==int(season)) & (team_df["mlbam"]==teamId)]
                        teamAbbrv = row["franchID"].item()
                        player_stats = [season,teamAbbrv]
                        
                        for field in BAT_FIELDS:
                            try:player_stats.append(split["stat"][field])
                            except:player_stats.append("--")
                        years.append(player_stats)
                    
            df = pd.DataFrame(data=years,columns=["Year","Team"]+bat_headers)
            hitting[addingTo] = df
            
        elif stat_item["group"]["displayName"] == "hitting" and stat_item["type"]["displayName"] == "yearByYearAdvanced":
            years = []
            addingTo = "ybyAdv"
            for split in stat_item["splits"]:
                if "numTeams" in split.keys():
                    pass
                else:
                    if split["gameType"] == "R":
                        season = split["season"]
                        teamId = split["team"]["id"]
                        row = team_df[(team_df["yearID"]==int(season)) & (team_df["mlbam"]==teamId)]
                        teamAbbrv = row["franchID"].item()
                        player_stats = [season,teamAbbrv]
                        
                        for field in BAT_FIELDS_ADV:
                            try:player_stats.append(split["stat"][field])
                            except:player_stats.append("--")
                        years.append(player_stats)
                    

            df = pd.DataFrame(data=years,columns=["Year","Team"]+bat_advheaders)
            hitting[addingTo] = df
        # ====================================================================================== YEAR-BY-YEAR HITTING ===

        # YEAR-BY-YEAR PITCHING =========================================================================================
        elif stat_item["group"]["displayName"] == "pitching" and stat_item["type"]["displayName"] == "yearByYear":
            years = []
            addingTo = "yby"
            for split in stat_item["splits"]:
                if "numTeams" in split.keys():
                    pass
                else:
                    if split["gameType"] == "R":
                        season = split["season"]
                        teamId = split["team"]["id"]
                        row = team_df[(team_df["yearID"]==int(season)) & (team_df["mlbam"]==teamId)]
                        teamAbbrv = row["franchID"].item()
                        player_stats = [season,teamAbbrv]
                        
                        for field in PITCH_FIELDS:
                            try:player_stats.append(split["stat"][field])
                            except:player_stats.append("--")
                        years.append(player_stats)

            df = pd.DataFrame(data=years,columns=["Year","Team"]+pitch_headers)
            pitching[addingTo] = df

        elif stat_item["group"]["displayName"] == "pitching" and stat_item["type"]["displayName"] == "yearByYearAdvanced":
            years = []
            addingTo = "ybyAdv"
            for split in stat_item["splits"]:
                if "numTeams" in split.keys():
                    pass
                else:
                    if split["gameType"] == "R":
                        season = split["season"]
                        teamId = split["team"]["id"]
                        row = team_df[(team_df["yearID"]==int(season)) & (team_df["mlbam"]==teamId)]
                        teamAbbrv = row["franchID"].item()
                        player_stats = [season,teamAbbrv]
                        
                        for field in PITCH_FIELDS_ADV:
                            try:player_stats.append(split["stat"][field])
                            except:player_stats.append("--")
                        years.append(player_stats)

            df = pd.DataFrame(data=years,columns=["Year","Team"]+pitch_advheaders)
            pitching[addingTo] = df
        # ===================================================================================== YEAR-BY-YEAR PITCHING ===

        # YEAR-BY-YEAR FIELDING =========================================================================================
        elif stat_item["group"]["displayName"] == "fielding" and stat_item["type"]["displayName"] == "yearByYear":
            years = []
            addingTo = "yby"
            for split in stat_item["splits"]:
                if "numTeams" in split.keys():
                    pass
                else:
                    if split["gameType"] == "R":
                        season = split["season"]
                        teamId = split["team"]["id"]
                        pos = split["position"]["abbreviation"]
                        row = team_df[(team_df["yearID"]==int(season)) & (team_df["mlbam"]==teamId)]
                        teamAbbrv = row["franchID"].item()
                        player_stats = [season,teamAbbrv,pos]
                        
                        for field in FIELD_FIELDS:
                            try:player_stats.append(split["stat"][field])
                            except:player_stats.append("--")
                        years.append(player_stats)

            df = pd.DataFrame(data=years,columns=["Year","Team","Pos"]+field_headers)
            fielding[addingTo] = df
        # ===================================================================================== YEAR-BY-YEAR FIELDING ===
        else:pass

    # *** need to incorporate a way to include current_season stats from the StatsAPI
    
    return {
        "mlbam":mlbam,
        "bbrefID":bbrefID,
        "retroID":retroID,
        "bbrefIDminors":bbrefIDminors,
        "fullName":fullName,
        "firstName":firstName,
        "lastName":lastName,
        "nickName":nickName,
        "birthDate":birthDate,
        "birthCity":birthCity,
        "birthState":birthState,
        "birthCountry":birthCountry,
        "deathDate":deathDate,
        "deathCity":deathCity,
        "deathState":deathState,
        "deathCountry":deathCountry,
        "currentAge":int(currentAge),
        "weight":weight,
        "height":height,
        "primaryPosition":primaryPosition,
        "primaryNumber":primaryNumber,
        "bats":bats,
        "throws":throws,
        "education":education,
        "mlbDebutDate":mlbDebutDate,
        "firstYear":int(firstYear),
        "lastYear":int(lastYear),
        "zoneTop":zoneTop,
        "zoneBot":zoneBot,
        "isActive":isActive,
        "activeRange":active_range,
        "hitting":hitting,
        "pitching":pitching,
        "fielding":fielding,
        "transactions":trx_df,
        "drafts":drafts_df,
        "awards":awards_df,
        "rosterEntries":rost_entries_df}

# get
def career_splits(playerID) -> dict:
    pass

# get player's hitting log by game from 'baseball-reference.com' (statsapi.mlb.com does not have games in early 20th century)
def hittingLog(bbrefID,year):
    url = f"https://widgets.sports-reference.com/wg.fcgi?site=br&url=%2Fplayers%2Fgl.fcgi%3Fid%3D{bbrefID}%26t%3Db%26year%3D{year}&div=div_batting_gamelogs"
    req = requests.get(url)
    soup = bs(req.text,"lxml").find("table")
    all_rows = []
    for tr in soup.find("tbody").findAll("tr"):
        row = []
        try:
            for data_stat in bbref_hit_log_fields:
                if data_stat == "date_game":
                    row.append(tr.find("td",attrs={"data-stat":data_stat})["csk"][:10])
                elif data_stat == "team_homeORaway":
                    if tr.find("td",attrs={"data-stat":data_stat}).text == "@":
                        row.append("@")
                    else:
                        row.append("vs")
                elif data_stat == "team_game_num":
                    row.append(tr.find("td",attrs={"data-stat":data_stat})["csk"])
                else:
                    row.append(tr.find("td",attrs={"data-stat":data_stat}).text)
            all_rows.append(row)
        except:
            pass
    df = pd.DataFrame(all_rows,columns=bbref_hit_log_fields).rename(columns=STATDICT)
    return df

# get player's pitching log by game from 'baseball-reference.com' (statsapi.mlb.com does not have games in early 20th century)
def pitchingLog(bbrefID,year):
    url = f"https://widgets.sports-reference.com/wg.fcgi?site=br&url=%2Fplayers%2Fgl.fcgi%3Fid%3D{bbrefID}%26t%3Dp%26year%3D{year}&div=div_pitching_gamelogs"
    req = requests.get(url)
    soup = bs(req.text,"lxml").find("table")
    all_rows = []
    for tr in soup.find("tbody").findAll("tr"):
        row = []
        try:
            for data_stat in bbref_pitch_log_fields:
                if data_stat == "date_game":
                    row.append(tr.find("td",attrs={"data-stat":data_stat})["csk"][:10])
                elif data_stat == "team_homeORaway":
                    if tr.find("td",attrs={"data-stat":data_stat}).text == "@":
                        row.append("@")
                    else:
                        row.append("vs")
                elif data_stat == "team_game_num":
                    row.append(tr.find("td",attrs={"data-stat":data_stat})["csk"])
                else:
                    row.append(tr.find("td",attrs={"data-stat":data_stat}).text)
            all_rows.append(row)
        except:
            pass
    df = pd.DataFrame(all_rows,columns=bbref_pitch_log_fields).rename(columns=STATDICT)
    return df

# get player's fielding log by game from 'baseball-reference.com' (statsapi.mlb.com does not have games in early 20th century)
def fieldingLog(bbrefID,year):
    url = f"https://widgets.sports-reference.com/wg.fcgi?site=br&url=%2Fplayers%2Fgl.fcgi%3Fid%3D{bbrefID}%26t%3Df%26year%3D{year}&div=div__0"
    req = requests.get(url)
    soup = bs(req.text,"lxml").find("table")
    all_rows = []
    for tr in soup.find("tbody").findAll("tr"):
        row = []
        try:
            for data_stat in bbref_field_log_fields:
                if data_stat == "date_game":
                    row.append(tr.find("td",attrs={"data-stat":data_stat})["csk"][:10])
                elif data_stat == "team_homeORaway":
                    if tr.find("td",attrs={"data-stat":data_stat}).text == "@":
                        row.append("@")
                    else:
                        row.append("vs")
                elif data_stat == "team_game_num":
                    row.append(tr.find("td",attrs={"data-stat":data_stat})["csk"])
                else:
                    row.append(tr.find("td",attrs={"data-stat":data_stat}).text)
            all_rows.append(row)
        except:
            pass
        
    df = pd.DataFrame(all_rows,columns=bbref_field_log_fields).rename(columns=STATDICT)
    return df

# get player's split stats from 'baseball-reference.com' (statsapi.mlb.com season splits are incomplete and/or broken)
def bbrefSplits(bbrefID,year,s_type):
    req = requests.get(f"https://www.baseball-reference.com/players/split.fcgi?id={bbrefID}&year={year}&t={s_type}")
    soup = bs(req.text,"lxml")
    split_tables = {}
    comments = soup.findAll(text=lambda text:isinstance(text, Comment))
    for c in comments:
        for split_type in BBREF_SPLITS.keys():
            if "div_"+split_type in c:
                split_tables[BBREF_SPLITS[split_type]] = (pd.read_html(c.extract())[0].drop(columns=["tOPS+","sOPS+"],errors='ignore'))
    return split_tables

# get player' pitch arsenal (if they are a pitcher)
# default group is set to "pitching" since the API doesn't differentiate between pitching arsenals & hitting performance AGAINST arsenals
def pitchArsenal(mlbam,season=curr_year):
    statTypes = "pitchArsenal"
    statGroups = "pitching"
    hydrate = f"stats(type=[{statTypes}],group=[{statGroups}],season={season})"
    url = BASE + f"/people?personIds={mlbam}&hydrate={hydrate}"

    response = requests.get(url).json()

    all_rows = []
    for p in response["people"][0]["stats"][0]["splits"]:
        pitch = p.get("stat",{})
        pitchCode = pitch.get("type",{}).get("code")
        pitchName = pitch.get("type",{}).get("description")
        perc = pitch.get("percentage","")
        numPitches = pitch.get("count","")
        totalPitches = pitch.get("totalPitches","")
        avgSpeed = pitch.get("averageSpeed","")
        row = [pitchCode,pitchName,perc,avgSpeed,numPitches,totalPitches]

        all_rows.append(row)
    df = pd.DataFrame(data=all_rows,columns=("pitch_code","pitch_name","percentage","avg_speed","pitch_ct","total_pitches"))
    return df

# player metric search tool - maybe for web app tool (or just to have)
def metricSearch(metrics,specify_group=None,**params):
    """ *CURRENTLY ONLY RETURNS RAW JSON*

    Required parameters
    -----------
    metrics: str | list, specify the metrics to retrieve (Required)

    Accepted parameters
    -----------
    - personIds
    - teamIds
    - seasons
    - date
    - start_date
    - end_date

    NOTE - if 'date' is specified, 'start_date' and 'end_date' values will be ignored

    Accepted values for 'metrics':
    --------
    hitting:
            launchSpeed launchAngle, generatedSpeed, maxHeight, travelTime, hangTime,
            distance, travelDistance, hrDistance, hitTrajectory,launchSpinRate, barreledBall
    
    pitching:
            releaseSpinRate, releaseExtension, releaseSpeed, effectiveSpeed, launchSpeed, 
            launchAngle, generatedSpeed
    
    """
    
    hitting_metrics = ['launchSpeed','launchAngle','generatedSpeed','maxHeight','travelTime','hangTime','distance','travelDistance','hrDistance','hitTrajectory','launchSpinRate','barreledBall']
    pitching_metrics = ['releaseSpinRate','releaseExtension','releaseSpeed','effectiveSpeed','launchSpeed','launchAngle','generatedSpeed']
    
    
    group = []
    query_metrics = []

    hit_metric_present = False
    pitch_metric_present = False

    if type(metrics) is list:
        metrics = ",".join(metrics)
    metrics = metrics.replace(" ","")

    for met in hitting_metrics:
        if re.search(met.lower(),metrics.lower()):
            hit_metric_present = True
            group.append("hitting")
            if met in query_metrics:
                pass
            else:
                query_metrics.append(met)
            
    for met in pitching_metrics:
        if re.search(met.lower(),metrics.lower()):
            pitch_metric_present = True
            group.append("pitching")
            if met in query_metrics:
                pass
            else:
                query_metrics.append(met)
    
    query_metrics = ",".join(query_metrics)
    metrics = f'metrics={query_metrics}'
            
    
    if specify_group is None:
        pass
    else:
        if specify_group == "hitting":
            hit_metric_present = True
            group = f"group={specify_group}"
        elif specify_group == "pitching":
            pitch_metric_present = True
            group = f"group={specify_group}"
        else:
            print(ValueError("accepted values for 'specify_group' param are 'hitting' or 'pitching'"))
            return None

    if hit_metric_present is False and pitch_metric_present is False:
        print("Must enter a valid metric. Or you may specify a specific group value with parameter, 'specify_group'")
        return None
    else:
        if type(group) is list:
            group = f"group={','.join(group)}"
        else:
            pass
            
                

    for key,value in params.items():
        if type(value) is list:
            params[key] = ",".join(value)


    personIds = "personId={}".format(
                params.get("personIds",
                params.get("personIDs",
                params.get("personID",
                params.get("personid",
                params.get("playerid",
                params.get("player",
                params.get("person",
                params.get("people",""))))))))
    )

    teamIds = "teamIds={}".format(
                params.get("teamId",
                params.get("teamIds",
                params.get("teamIDs",
                params.get("teamID",
                params.get("teams",
                params.get("team","")))))))

    seasons = "seasons={}".format(
                params.get("seasons",
                params.get("season",
                params.get("seasonId",
                params.get("year",
                params.get("seasonID","")))))
    )

    startDate = "startDate={}".format(
                params.get("date",
                params.get("start_date",
                params.get("startDate","")))
    )

    endDate = "endDate={}".format(
                params.get("end_date",
                params.get("endDate",""))
    )
    
    if "date" in params.keys():
        endDate = f"endDate={startDate[10:]}"


    url = f"{BASE}/stats?stats=metricLog&{group}&{metrics}&{personIds}&{teamIds}&{seasons}&{startDate}&{endDate}"
    
    response = requests.get(url).json()["stats"]
    return response

# get only player's stats
def getPlayerStats(playerID) -> dict:
    """Returns a dictionary of a player's hitting, pitching, and fielding stats as dataframes"""
    people = get_people_df()
    try:
        mlbam = int(playerID)
        player_df = people.set_index("mlbam").loc[mlbam]
        bbrefID = player_df["bbrefID"]
        # retroID = player_df["retroID"]
        # bbrefIDminors = player_df["bbrefIDminors"]
    except:
        bbrefID = playerID
        player_df = people.set_index("bbrefID").loc[bbrefID]
        mlbam = int(player_df["mlbam"])
        # retroID = player_df["retroID"]
        # bbrefIDminors = player_df["bbrefIDminors"]
    hydrate = "stats(type=[career,careerAdvanced,yearByYear,yearByYearAdvanced],group=[hitting,fielding,pitching],gameType=[R,P]),rosterEntries,education,xrefId"
    url = BASE + f"/people?personIds={mlbam}&hydrate={hydrate}"
    playerPage = requests.get(url).json()
    
    player = playerPage["people"][0]
    
    primaryPosition = player["primaryPosition"]["abbreviation"]
    fullName = player["fullFMLName"]
    firstName = player["firstName"]
    lastName = player["lastName"]
    try:nickName = player["nickName"]
    except:nickName = "--"
    primaryNumber = player["primaryNumber"]
    birthDate = player["birthDate"]
    currentAge = player["currentAge"]
    try:birthCity = player["birthCity"]
    except:birthCity = ""
    try:birthState = player["birthStateProvince"]
    except:birthState = ""
    birthCountry = player["birthCountry"]
    weight = player["weight"]
    height = player["height"]
    bats = player["batSide"]["code"]
    throws = player["pitchHand"]["code"]
    education = player["education"]
    mlbDebutDate = player["mlbDebutDate"]
    firstYear = player_df[["yearDebut"]].item()
    lastYear = player_df[["yearRecent"]].item()
    zoneTop = player["strikeZoneTop"]
    zoneBot = player["strikeZoneBottom"]
    isActive = player["active"]
    stats_list = player["stats"]

    active_range = list(range(int(mlbDebutDate[:4]),curr_year+1))
    team_df = get_teams_df()
    team_df = team_df[team_df["yearID"].isin(active_range)]


    hitting = {
        "careerReg":False,
        "careerPost":False,
        "careerAdvReg":False,
        "careerAdvPost":False,
        "yby":False,
        "ybyAdv":False,
        "recentSeason":False
    }
    pitching = {
        "careerReg":False,
        "careerPost":False,
        "careerAdvReg":False,
        "careerAdvPost":False,
        "yby":False,
        "ybyAdv":False,
        "recentSeason":False
    }
    fielding = {
        "careerReg":False,
        "careerPost":False,
        "yby":False,
        "recentSeason":False
    }
    
    
    # === Generate header lists ===========
    bat_headers = []
    bat_advheaders = []

    for f in BAT_FIELDS:
        bat_headers.append(STATDICT[f])
    for f in BAT_FIELDS_ADV:
        bat_advheaders.append(STATDICT[f])

    pitch_headers = []
    pitch_advheaders = []

    for f in PITCH_FIELDS:
        pitch_headers.append(STATDICT[f])
    for f in PITCH_FIELDS_ADV:
        pitch_advheaders.append(STATDICT[f])

    field_headers = []

    for f in FIELD_FIELDS:
        field_headers.append(STATDICT[f])
    # =====================================       

    # === CAREER ===================================================================================================================
    for stat_item in stats_list:
        # CAREER HITTING ===========================================================================================
        if stat_item["group"]["displayName"] == "hitting" and stat_item["type"]["displayName"] == "career":
            for split in stat_item["splits"]:
                if split["gameType"] == "R":
                    player_stats = []
                    addingTo = "careerReg"
                    for field in BAT_FIELDS:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=bat_headers)
                    hitting[addingTo] = df
                            
                elif split["gameType"] == "P":
                    player_stats = []
                    addingTo = "careerPost"
                    for field in BAT_FIELDS:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=bat_headers)
                    hitting[addingTo] = df
                else:pass
        elif stat_item["group"]["displayName"] == "hitting" and stat_item["type"]["displayName"] == "careerAdvanced":
            for split in stat_item["splits"]:
                if split["gameType"] == "R":
                    player_stats = []
                    addingTo = "careerAdvReg"
                    for field in BAT_FIELDS_ADV:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=bat_advheaders)
                    hitting[addingTo] = df
                            
                elif split["gameType"] == "P":
                    player_stats = []
                    addingTo = "careerAdvPost"
                    for field in BAT_FIELDS_ADV:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=bat_advheaders)
                    hitting[addingTo] = df
                    
                # elif split["gameType"] == "S":
                #     player_stats = []
                #     addingTo = "careerAdvSpring"
                #     for field in BAT_FIELDS_ADV:
                #         try:player_stats.append(split["stat"][field])
                #         except:player_stats.append("--")

                #     df = pd.DataFrame(data=[player_stats],columns=bat_advheaders)
                #     hitting[addingTo] = df
                else:pass
        # ====================================================================================== CAREER HITTING ====

        # CAREER PITCHING =====================================================================================
        elif stat_item["group"]["displayName"] == "pitching" and stat_item["type"]["displayName"] == "career":
            for split in stat_item["splits"]:
                if split["gameType"] == "R":
                    player_stats = []
                    addingTo = "careerReg"
                    for field in PITCH_FIELDS:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=pitch_headers)
                    pitching[addingTo] = df
                            
                elif split["gameType"] == "P":
                    player_stats = []
                    addingTo = "careerPost"
                    for field in PITCH_FIELDS:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=pitch_headers)
                    pitching[addingTo] = df
                    
                # elif split["gameType"] == "S":
                #     player_stats = []
                #     addingTo = "careerSpring"
                #     for field in PITCH_FIELDS:
                #         try:player_stats.append(split["stat"][field])
                #         except:player_stats.append("--")

                #     df = pd.DataFrame(data=[player_stats],columns=pitch_headers)
                #     pitching[addingTo] = df
                else:pass
        elif stat_item["group"]["displayName"] == "pitching" and stat_item["type"]["displayName"] == "careerAdvanced":
            for split in stat_item["splits"]:
                if split["gameType"] == "R":
                    player_stats = []
                    addingTo = "careerAdvReg"
                    for field in PITCH_FIELDS_ADV:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=pitch_advheaders)
                    pitching[addingTo] = df
                            
                elif split["gameType"] == "P":
                    player_stats = []
                    addingTo = "careerAdvPost"
                    for field in PITCH_FIELDS_ADV:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=pitch_advheaders)
                    pitching[addingTo] = df
                    
                # elif split["gameType"] == "S":
                #     player_stats = []
                #     addingTo = "careerAdvSpring"
                #     for field in PITCH_FIELDS_ADV:
                #         try:player_stats.append(split["stat"][field])
                #         except:player_stats.append("--")

                #     df = pd.DataFrame(data=[player_stats],columns=pitch_advheaders)
                #     pitching[addingTo] = df
                else:pass
        # ===================================================================================== CAREER PITCHING ====

        # CAREER FIELDING =====================================================================================
        elif stat_item["group"]["displayName"] == "fielding" and stat_item["type"]["displayName"] == "career":
            fielding_dfs = []
            fielding_dfs_post = []
            for split in stat_item["splits"]:
                if split["gameType"] == "R":
                    pos = split["position"]["abbreviation"]
                    player_stats = [pos]
                    addingTo = "careerReg"
                    for field in FIELD_FIELDS:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=["Pos"]+field_headers)
                    fielding_dfs.append(df)
                
                            
                elif split["gameType"] == "P":
                    player_stats = [pos]
                    addingTo = "careerPost"
                    for field in FIELD_FIELDS:
                        try:player_stats.append(split["stat"][field])
                        except:player_stats.append("--")

                    df = pd.DataFrame(data=[player_stats],columns=["Pos"]+field_headers)
                    fielding_dfs_post.append(df)

                    
                else:pass
                
                

            fielding["careerReg"] = pd.concat(fielding_dfs)
            fielding["careerPost"] = pd.concat(fielding_dfs_post)
        # ===================================================================================== CAREER FIELDING ====

        else:pass
    # ============================================================================================================== CAREER ========


    # === YEAR-BY-YEAR =============================================================================================================
    for stat_item in stats_list:
        # YEAR-BY-YEAR HITTING ==========================================================================================
        if stat_item["group"]["displayName"] == "hitting" and stat_item["type"]["displayName"] == "yearByYear":
            years = []
            addingTo = "yby"
            for split in stat_item["splits"]:
                if "numTeams" in split.keys():
                    pass
                else:
                    if split["gameType"] == "R":
                        season = split["season"]
                        teamId = split["team"]["id"]
                        row = team_df[(team_df["yearID"]==int(season)) & (team_df["mlbam"]==teamId)]
                        teamAbbrv = row["franchID"].item()
                        player_stats = [season,teamAbbrv]
                        
                        for field in BAT_FIELDS:
                            try:player_stats.append(split["stat"][field])
                            except:player_stats.append("--")
                        years.append(player_stats)
                    
            df = pd.DataFrame(data=years,columns=["Year","Team"]+bat_headers)
            hitting[addingTo] = df
            
        elif stat_item["group"]["displayName"] == "hitting" and stat_item["type"]["displayName"] == "yearByYearAdvanced":
            years = []
            addingTo = "ybyAdv"
            for split in stat_item["splits"]:
                if "numTeams" in split.keys():
                    pass
                else:
                    if split["gameType"] == "R":
                        season = split["season"]
                        teamId = split["team"]["id"]
                        row = team_df[(team_df["yearID"]==int(season)) & (team_df["mlbam"]==teamId)]
                        teamAbbrv = row["franchID"].item()
                        player_stats = [season,teamAbbrv]
                        
                        for field in BAT_FIELDS_ADV:
                            try:player_stats.append(split["stat"][field])
                            except:player_stats.append("--")
                        years.append(player_stats)
                    

            df = pd.DataFrame(data=years,columns=["Year","Team"]+bat_advheaders)
            hitting[addingTo] = df
        # ====================================================================================== YEAR-BY-YEAR HITTING ===

        # YEAR-BY-YEAR PITCHING =========================================================================================
        elif stat_item["group"]["displayName"] == "pitching" and stat_item["type"]["displayName"] == "yearByYear":
            years = []
            addingTo = "yby"
            for split in stat_item["splits"]:
                if "numTeams" in split.keys():
                    pass
                else:
                    if split["gameType"] == "R":
                        season = split["season"]
                        teamId = split["team"]["id"]
                        row = team_df[(team_df["yearID"]==int(season)) & (team_df["mlbam"]==teamId)]
                        teamAbbrv = row["franchID"].item()
                        player_stats = [season,teamAbbrv]
                        
                        for field in PITCH_FIELDS:
                            try:player_stats.append(split["stat"][field])
                            except:player_stats.append("--")
                        years.append(player_stats)

            df = pd.DataFrame(data=years,columns=["Year","Team"]+pitch_headers)
            pitching[addingTo] = df

        elif stat_item["group"]["displayName"] == "pitching" and stat_item["type"]["displayName"] == "yearByYearAdvanced":
            years = []
            addingTo = "ybyAdv"
            for split in stat_item["splits"]:
                if "numTeams" in split.keys():
                    pass
                else:
                    if split["gameType"] == "R":
                        season = split["season"]
                        teamId = split["team"]["id"]
                        row = team_df[(team_df["yearID"]==int(season)) & (team_df["mlbam"]==teamId)]
                        teamAbbrv = row["franchID"].item()
                        player_stats = [season,teamAbbrv]
                        
                        for field in PITCH_FIELDS_ADV:
                            try:player_stats.append(split["stat"][field])
                            except:player_stats.append("--")
                        years.append(player_stats)

            df = pd.DataFrame(data=years,columns=["Year","Team"]+pitch_advheaders)
            pitching[addingTo] = df
        # ===================================================================================== YEAR-BY-YEAR PITCHING ===

        # YEAR-BY-YEAR FIELDING =========================================================================================
        elif stat_item["group"]["displayName"] == "fielding" and stat_item["type"]["displayName"] == "yearByYear":
            years = []
            addingTo = "yby"
            for split in stat_item["splits"]:
                if "numTeams" in split.keys():
                    pass
                else:
                    if split["gameType"] == "R":
                        season = split["season"]
                        teamId = split["team"]["id"]
                        pos = split["position"]["abbreviation"]
                        row = team_df[(team_df["yearID"]==int(season)) & (team_df["mlbam"]==teamId)]
                        teamAbbrv = row["franchID"].item()
                        player_stats = [season,teamAbbrv,pos]
                        
                        for field in FIELD_FIELDS:
                            try:player_stats.append(split["stat"][field])
                            except:player_stats.append("--")
                        years.append(player_stats)

            df = pd.DataFrame(data=years,columns=["Year","Team","Pos"]+field_headers)
            fielding[addingTo] = df
        # ===================================================================================== YEAR-BY-YEAR FIELDING ===
        else:pass
    # ======================================================================================================== YEAR-BY-YEAR ========

    # *** need to incorporate a way to include current_season stats from the StatsAPI
    
    return {
        "hitting":hitting,
        "pitching":pitching,
        "fielding":fielding
    }

