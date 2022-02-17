import asyncio
import aiohttp
# import time
import pandas as pd

from ..constants import BASE
from ..constants import LEAGUE_IDS_SHORT

from ..mlbdata import get_teams_df

from ..utils import curr_year

async def parse_data(response,teams_df):
    all_records = []
    for league in response["records"]:
        lg_mlbam = league.get("league",{}).get("id","")
        year = int(league["league"]["season"])
        lg_abbrv = LEAGUE_IDS_SHORT.get(lg_mlbam)
        for team in league["teamRecords"]:
        # SEASON RECORDS FOR EACH TEAM
            tm_mlbam = team["team"]["id"]
            tm_name = teams_df[(teams_df["mlbam"]==tm_mlbam) & (teams_df["yearID"]==year)].fullName.item()
            tm_bbrefID = teams_df[(teams_df["mlbam"]==tm_mlbam) & (teams_df["yearID"]==year)].bbrefID.item()
            div_mlbam = team.get("team",{}).get("division",{}).get("id","")
            div_short = LEAGUE_IDS_SHORT.get(div_mlbam,"-")
            v_mlbam = team.get("team",{}).get("venue",{}).get("id")
            tm_records = [year,tm_mlbam,tm_name,tm_bbrefID,lg_mlbam,lg_abbrv,div_mlbam,div_short,v_mlbam]
            record_cats = team["records"]
            
            # Total
            tm_records.append(team["gamesPlayed"])
            tm_records.append(team["wins"])
            tm_records.append(team["losses"])
            tm_records.append(team["winningPercentage"])
            tm_records.append(team["runsScored"])
            tm_records.append(team["runsAllowed"])
            tm_records.append(team["runDifferential"])

            # League Records
            lg_records_dict = {}
            for rec in record_cats["leagueRecords"]:
                if rec["league"]["id"] == 103:          # American League
                    lg_records_dict["al_W"] = rec["wins"]
                    lg_records_dict["al_L"] = rec["losses"]
                elif rec["league"]["id"] == 104:        # National League
                    lg_records_dict["nl_W"] = rec["wins"]
                    lg_records_dict["nl_L"] = rec["losses"]

                # elif rec["league"]["id"] == 105:        # Players League (INACTIVE)
                #     tm_records["plWins"] = rec["wins"]
                #     tm_records["plLosses"] = rec["losses"]
                # elif rec["league"]["id"] == 106:        # Federal League (INACTIVE)
                #     tm_records["flWins"] = rec["wins"]
                #     tm_records["flLosses"] = rec["losses"]
                # elif rec["league"]["id"] == 100:        # American Association (INACTIVE)
                #     tm_records["aaWins"] = rec["wins"]
                #     tm_records["aaLosses"] = rec["losses"]
                # elif rec["league"]["id"] == 101:        # Union Association (INACTIVE)
                #     tm_records["uaWins"] = rec["wins"]
                #     tm_records["uaLosses"] = rec["losses"]
                # elif rec["league"]["id"] == 102:        # National Association (INACTIVE)
                #     tm_records["naWins"] = rec["wins"]
                #     tm_records["naLosses"] = rec["losses"] 

            # Division Records
            div_records_dict = {}
            try:
                for rec in record_cats["divisionRecords"]:
                    if rec["division"]["id"] == 200:        # West (AL)
                        div_records_dict['west_W'] = rec["wins"]
                        div_records_dict['west_L'] = rec["losses"]
                    elif rec["division"]["id"] == 201:      # East (AL)
                        div_records_dict['east_W'] = rec["wins"]
                        div_records_dict['east_L'] = rec["losses"]
                    elif rec["division"]["id"] == 202:      # Central (AL)
                        div_records_dict['central_W'] = rec["wins"]
                        div_records_dict['central_L'] = rec["losses"]
                    elif rec["division"]["id"] == 203:      # West (NL)
                        div_records_dict['west_W'] = rec["wins"]
                        div_records_dict['west_L'] = rec["losses"]
                    elif rec["division"]["id"] == 204:      # East (NL)
                        div_records_dict['east_W'] = rec["wins"]
                        div_records_dict['east_L'] = rec["losses"]
                    elif rec["division"]["id"] == 205:      # Central (NL)
                        div_records_dict['central_W'] = rec["wins"]
                        div_records_dict['central_L'] = rec["losses"]
            except:
                # print(f"no division records available for TEAM: {tm_name} ({tm_mlbam}) YEAR: {season}")
                pass

            # Split Records
            split_records_dict = {}
            for rec in record_cats["splitRecords"]:
                if rec["type"] == "home":
                    split_records_dict["hm_W"] = rec["wins"]
                    split_records_dict["hm_L"] = rec["losses"]
                elif rec["type"] == "away":
                    split_records_dict["aw_W"] = rec["wins"]
                    split_records_dict["aw_L"] = rec["losses"]
                elif rec["type"] == "lastTen":
                    split_records_dict["lt_W"] = rec["wins"]
                    split_records_dict["lt_L"] = rec["losses"]
                elif rec["type"] == "extraInning":
                    split_records_dict["xInn_W"] = rec["wins"]
                    split_records_dict["xInn_L"] = rec["losses"]
                elif rec["type"] == "oneRun":
                    split_records_dict["1R_W"] = rec["wins"]
                    split_records_dict["1R_L"] = rec["losses"]
                elif rec["type"] == "winners":
                    split_records_dict["w_W"] = rec["wins"]
                    split_records_dict["w_L"] = rec["losses"]
                elif rec["type"] == "day":
                    split_records_dict["dy_W"] = rec["wins"]
                    split_records_dict["dy_L"] = rec["losses"]
                elif rec["type"] == "night":
                    split_records_dict["nt_W"] = rec["wins"]
                    split_records_dict["nt_L"] = rec["losses"]
                elif rec["type"] == "grass":
                    split_records_dict["g_W"] = rec["wins"]
                    split_records_dict["g_L"] = rec["losses"]
                elif rec["type"] == "turf":
                    split_records_dict["t_W"] = rec["wins"]
                    split_records_dict["t_L"] = rec["losses"]
                elif rec["type"] == "right":
                    split_records_dict["rhp_W"] = rec["wins"]
                    split_records_dict["rhp_L"] = rec["losses"]
                elif rec["type"] == "left":
                    split_records_dict["lhp_W"] = rec["wins"]
                    split_records_dict["lhp_L"] = rec["losses"]

            if year < 1901:
                for key in lg_records_dict.keys():
                    lg_records_dict[key] = "-"
                for key in div_records_dict.keys():
                    div_records_dict[key] = "-"
                for key in split_records_dict.keys():
                    split_records_dict[key] = "-"

            # Appending data via dictionary because there is no way to retrieve league records by key index 
            tm_records.append(lg_records_dict.get("al_W","-"))
            tm_records.append(lg_records_dict.get("al_L","-"))
            tm_records.append(lg_records_dict.get("nl_W","-"))
            tm_records.append(lg_records_dict.get("nl_L","-"))   

            # Appending data via dictionary because there is no way to retrieve division records by key index
            tm_records.append(div_records_dict.get("west_W","-"))
            tm_records.append(div_records_dict.get("west_L","-"))
            tm_records.append(div_records_dict.get("east_W","-"))
            tm_records.append(div_records_dict.get("east_L","-"))
            tm_records.append(div_records_dict.get("central_W","-"))
            tm_records.append(div_records_dict.get("central_L","-"))

            # Appending data via dictionary because there is no way to retrieve split records by key index 
            tm_records.append(split_records_dict.get("hm_W","-"))
            tm_records.append(split_records_dict.get("hm_L","-"))
            tm_records.append(split_records_dict.get("aw_W","-"))
            tm_records.append(split_records_dict.get("aw_L","-"))
            tm_records.append(split_records_dict.get("lt_W","-"))
            tm_records.append(split_records_dict.get("lt_L","-"))
            tm_records.append(split_records_dict.get("xInn_W","-"))
            tm_records.append(split_records_dict.get("xInn_L","-"))
            tm_records.append(split_records_dict.get("1R_W","-"))
            tm_records.append(split_records_dict.get("1R_L","-"))
            tm_records.append(split_records_dict.get("w_W","-"))
            tm_records.append(split_records_dict.get("w_L","-"))
            tm_records.append(split_records_dict.get("dy_W","-"))
            tm_records.append(split_records_dict.get("dy_L","-"))
            tm_records.append(split_records_dict.get("nt_W","-"))
            tm_records.append(split_records_dict.get("nt_L","-"))
            tm_records.append(split_records_dict.get("g_W","-"))
            tm_records.append(split_records_dict.get("g_L","-"))
            tm_records.append(split_records_dict.get("t_W","-"))
            tm_records.append(split_records_dict.get("t_L","-"))
            tm_records.append(split_records_dict.get("rhp_W","-"))
            tm_records.append(split_records_dict.get("rhp_L","-"))
            tm_records.append(split_records_dict.get("lhp_W","-"))
            tm_records.append(split_records_dict.get("lhp_L","-"))

            all_records.append(tm_records)

    return all_records       

async def get_updated_records(year=None,start=None,end=None):
    teams_df = get_teams_df()
    leagueIDs = "103,104"
    standingsTypes = "byLeague"
    if year is not None:
        start = year
        end = year
    if year is None and start is None and end is None:
        start = 1876
        end = curr_year

    parsed_data_by_year = []
    all_records = []
    async with aiohttp.ClientSession() as sesh:
        tasks = []
        for season in range(start,end+1):
            url = BASE + f"/standings?leagueId={leagueIDs}&standingsTypes={standingsTypes}&season={season}&hydrate=league,team(division)"
            tasks.append(sesh.get(url,ssl=False))
        responses = await asyncio.gather(*tasks)
        for response in responses:
            resp = await response.json()
            parsed_data_by_year.append(await parse_data(resp,teams_df))
    for y in parsed_data_by_year:
        for r in y:
            all_records.append(r)
    columns = ["season","tm_mlbam","tm_name","tm_bbrefID","lg_mlbam","lg_abbrv","div_mlbam","div_short","v_mlbam","G","W","L","W%","R","RA","RunDiff","al_W","al_L","nl_W","nl_L","west_W","west_L","east_W","east_L","central_W","central_L","hm_W","hm_L","aw_W","aw_L","lt_W","lt_L","xInn_W","xInn_L","1R_W","1R_L","win_W","win_L","dy_W","dy_L","nt_W","nt_L","g_W","g_L","t_W","t_L","rhp_W","rhp_L","lhp_W","lhp_L"]
    df = pd.DataFrame(data=all_records,columns=columns)
    return df

def runit():
    # start = time.time()
    retrieved = asyncio.run(get_updated_records())
    # print(f"--- {time.time()-start} seconds ---")
    return retrieved
