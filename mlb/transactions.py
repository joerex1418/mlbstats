import requests
import pandas as pd
# from pprint import pprint

from .constants import BASE
from .utils import curr_year


def draft(year=curr_year,**kwargs):
    """Get dict of draft information for a specified year
    
    Arguments
    ==========
    Required:
    -----
    year: int, year the draft took place

    Accepted:
    -----
    teamId: int, drafting team's mlbam ID
    """

    if len(kwargs) !=0 :
        query_str = []
        for key,val in kwargs.items():
            query_str.append(f"{key}={val}")
        query_str = "&".join(query_str)
    else:
        query_str = ""

    url = BASE + f"/draft/{year}?{query_str}"

    response = requests.get(url).json()

    columns = ('p_name','p_mlbam','round','pick_number','round_pickNumber','team','team_mlbam',"pos",'birth_date','birth_city','birth_state','birth_country','height','weight','bats','throws','school','draft_year')

    all_rows = []
    for r in response["drafts"]["rounds"]:
        draft_round = r.get("round","")
        for p in r["picks"]:
            # pickRound = p.get("pickRound","-")
            pickNumber = p.get("pickNumber","-")
            roundPickNum = p.get("roundPickNumber","-")
            team = p.get("team",{}).get("name","-")
            team_mlbam = p.get("team",{}).get("id","")
            player = p.get("person")
            p_name = player.get("fullName")
            p_mlbam = player.get("id")
            birthDate = player.get("birthDate","")
            birthCity = player.get("birthCity","")
            birthState = player.get("birthStateProvince","")
            birthCountry = player.get("birthCountry","")
            height = player["height"]
            weight = player["weight"]
            bats = player.get("batSide",{}).get("code","")
            throws = player.get("pitchHand",{}).get("code","")
            school = player.get("school",{}).get("name","")
            draftYear = player.get("draftYear","-")
            pos = player.get("primaryPosition",{}).get("abbreviation","")

            row = [p_name,p_mlbam,draft_round,pickNumber,roundPickNum,team,team_mlbam,pos,birthDate,birthCity,birthState,birthCountry,height,weight,bats,throws,school,draftYear]

            all_rows.append(row)
    
    df = pd.DataFrame(data=all_rows,columns=columns)

    return df

def prospects(season=curr_year):
    """Get dataframe of draft prospects for a specified season"""
    url = BASE + f"/draft/prospects/{season}"

    response = requests.get(url).json()
    columns = ('p_name','p_mlbam','round','pick_number','round_pickNumber','team','team_mlbam',"pos",'birth_date','birth_city','birth_state','birth_country','height','weight','bats','throws','school','draft_year')
    all_rows = []
    for p in response["prospects"]:
        pickRound = p.get("pickRound","-")
        pickNumber = p.get("pickNumber","-")
        roundPickNum = p.get("roundPickNumber","-")
        team = p.get("team",{}).get("name","")
        team_mlbam = p.get("team",{}).get("id","")
        player = p.get("person")
        p_name = player.get("fullName")
        p_mlbam = player.get("id")
        birthDate = player.get("birthDate","")
        birthCity = player.get("birthCity","")
        birthState = player.get("birthStateProvince","")
        birthCountry = player.get("birthCountry","")
        height = player["height"]
        weight = player["weight"]
        bats = player.get("batSide",{}).get("code","")
        throws = player.get("pitchHand",{}).get("code","")
        school = player.get("school",{}).get("name","")
        draftYear = player.get("draftYear","-")
        pos = player.get("primaryPosition",{}).get("abbreviation","")

        row = [p_name,p_mlbam,pickRound,pickNumber,roundPickNum,team,team_mlbam,pos,birthDate,birthCity,birthState,birthCountry,height,weight,bats,throws,school,draftYear]

        all_rows.append(row)
    
    df = pd.DataFrame(data=all_rows,columns=columns)

    return df

def free_agents(season=curr_year):
    """Get dataframe of free agents for a specified season
    
    Arguments
    --------
    season: int, requested season to query for free agents
    
    """
    url = BASE + f"/people/freeAgents?season={season}"

    response = requests.get(url).json()
    columns = ('p_name','p_mlbam','pos','posType','og_team','og_mlbam','new_team','new_mlbam','date_signed','date_declared','notes')
    all_rows = []
    for fa in response["freeAgents"]:
        player = fa.get("player",{})
        p_name = player.get("fullName","")
        p_mlbam = player.get("id","")
        
        if "originalTeam" in fa.keys():
            og = fa.get("originalTeam","")
            og_team = og.get("name","")
            og_mlbam = og.get("id","")
        else:
            og_team = ""
            og_mlbam = ""

        if "newTeam" in fa.keys():
            new = fa.get("newTeam","")
            new_team = og.get("name","")
            new_mlbam = og.get("id","")
        else:
            new_team = ""
            new_mlbam = ""
        
        pos_dict = fa.get("position")
        pos = pos_dict["abbreviation"]
        posType = pos_dict["type"]

        dateSigned = fa.get("dateSigned","")
        dateDeclared = fa.get("dateDeclared","")
        notes = fa.get("notes","")

        row = [p_name,p_mlbam,pos,posType,og_team,og_mlbam,new_team,new_mlbam,dateSigned,dateDeclared,notes]
        all_rows.append(row)
    
    df = pd.DataFrame(data=all_rows,columns=columns)
    return df

def transactions(**kwargs):
    """Get dataframe of transactions for a specific player or team given a date (or start date/end date)
    
    Accepted keyword args:
    -------------------
    playerId:   int, player mlbam ID
    teamId:     int, team's mlbam ID
    date:       str, fmt = 'mm/dd/yyyy'
    startDate:  str, fmt = 'mm/dd/yyyy'
    endDate:    str, fmt = 'mm/dd/yyyy'
    NOTE: 'startDate' & 'endDate' must be used together; 
            If just the 'date' param is being used, it's best to exclude 'startDate' and 'endDate'
    """

    query_str = []
    for key,val in kwargs.items():
        query_param = f"{key}={val}"
        query_str.append(query_param)
    query_str = "&".join(query_str)
    
    url = BASE + f"/transactions?{query_str}"
    response = requests.get(url).json()

    columns = ("name","mlbam","tr_type","tr","description","date","e_date","r_date","fr","fr_mlbam","to","to_mlbam")
    all_rows = []
    for t in response["transactions"]:
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
    
    df = pd.DataFrame(data=all_rows,columns=columns)

    return df
