import asyncio
import aiohttp
import pandas as pd
# import time

from ..constants import BASE
from ..constants import BIOS_HEADERS

from ..mlbdata import get_people_df

# STEP 1: Get list of player mlbam IDs
def get_mlbams():
    mlbams = list(get_people_df()["mlbam"])
    return mlbams

# STEP 2: Collect endpoints
def get_tasks(session,mlbams):
    hydrations = "currentTeam,team,rosterEntries,jobs,relatives,transactions,social,education,stats"
    tasks = []
    for mlbam in mlbams:
        tasks.append(session.get(BASE + f"/people/{mlbam}?hydrate={hydrations}", ssl=False))
    return tasks

async def parse_data(response):
    r = response["people"][0]
    mlbam = r.get("id","-")
    name = r.get("fullName","-")
    nameFirst = r.get("firstName","-")
    nameMiddle = r.get("middleName","-")
    nameLast = r.get("lastName","-")
    nameGiven = r.get("fullFMLName","-")
    nameBoxscore = r.get("boxscoreName","-")
    nameMatrilineal = r.get("nameMatrilineal","-")
    nickName = r.get("nickName","-")
    birthDate = r.get("birthDate","-")
    birthCity = r.get("birthCity","-")
    birthStateProvince = r.get("birthStateProvince","-")
    birthCountry = r.get("birthCountry","-")
    age = r.get("currentAge","-")

    hs = r.get("education",{}).get("highschools",[{}])[0]
    highschool = f'{hs.get("name","-")} ({hs.get("city","-")}, {hs.get("state","-")})'
    if highschool == "- (-, -)":highschool = "-"
    c = r.get("education",{}).get("colleges",[{}])[0]
    if c.get("city","-") == "-" or c.get("state","-") == "-":
        college = c.get("name","-")
    else:
        college = f'{c.get("name","-")} ({c.get("city","-")}, {c.get("state","-")})'

    deathDate = r.get("deathDate","-")
    deathCity = r.get("deathCity","-")
    deathStateProvince = r.get("deathStateProvince","-")
    deathCountry = r.get("deathCountry","-")
    height = r.get("height","-")
    weight = r.get("weight","-")
    isPlayer = r.get("isPlayer",False)
    isActive = r.get("active",False)
    draftYear = r.get("draftYear","-")
    mlbDebutDate = r.get("mlbDebutDate","-")
    bats = r.get("batSide",{}).get("code","-")
    throws = r.get("pitchHand",{}).get("code","-")
    primaryNumber = r.get("primaryNumber","-")
    primaryPosition = r.get("primaryPosition",{}).get("abbreviation","-")
    recentTeam = r.get("currentTeam",{})
    recentTeam = f'{recentTeam.get("name","-")} ({recentTeam.get("id","")})'

    allNumbers = []
    allPositions = []
    allTeams = []

    rosterEntries = r.get("rosterEntries",[{}])
    for entry in rosterEntries:
        try:allNumbers.append(entry["jerseyNumber"])
        except:pass

        try:allPositions.append(entry["position"]["abbreviation"])
        except:pass

        try:allTeams.append(str(entry["team"]["id"]))
        except:pass

        try:
            if entry["startDate"] == mlbDebutDate:
                mlbDebutTeam = entry.get("team",{})
                mlbDebutTeam = f'{mlbDebutTeam.get("name","-")} ({mlbDebutTeam.get("id","")})'
            else:
                mlbDebutTeam = "- ()"
        except:
            mlbDebutTeam = "- ()"

    lastGame = r.get("lastPlayedDate","-")
    
    allNumbers = list(set(allNumbers))
    allPositions = list(set(allPositions))
    allTeams = list(set(allTeams))
    
    allNumbers = "|".join(allNumbers)
    allPositions = "|".join(allPositions)
    allTeams = "|".join(allTeams)

    bio_data = [
        mlbam,
        name,
        nameFirst,
        nameMiddle,
        nameLast,
        nameGiven,
        nameBoxscore,
        nameMatrilineal,
        nickName,
        birthDate,
        birthCity,
        birthStateProvince,
        birthCountry,
        age,
        highschool,
        college,
        deathDate,
        deathCity,
        deathStateProvince,
        deathCountry,
        height,
        weight,
        isPlayer,
        isActive,
        draftYear,
        mlbDebutDate,
        # mlbDebutTeam,
        lastGame,
        recentTeam,
        bats,
        throws,
        primaryNumber,
        primaryPosition,
        allNumbers,
        allPositions,
        allTeams
    ]
    return bio_data
    

async def get_bios(mlbams):
    player_bios = []
    async with aiohttp.ClientSession() as session:
        tasks = get_tasks(session,mlbams)
        responses = await asyncio.gather(*tasks)
        
        for response in responses:
            resp = await response.json()
            try:
                parsed = await parse_data(resp)
                player_bios.append(parsed)
            except:
                url = str(response.url)
                mlbamID = int(url[39:url.find("?")])
                empty_data = [mlbamID,'-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-',]
                player_bios.append(empty_data)
    df = pd.DataFrame(data=player_bios,columns=BIOS_HEADERS)

    return df

def runit(mlbams=None,include_list_data=False):
    if mlbams is None: mlbams = get_mlbams()

    # start = time.time()
    retrieved = asyncio.run(get_bios(mlbams))
    # print("--- {} seconds ---".format(time.time()-start))
    if include_list_data is False:
        return retrieved.drop(columns=["allNumbers","allPositions","allTeams"])
    return retrieved