import asyncio
import aiohttp
import pandas as pd
# import time
# from pprint import pprint

from ..constants import BASE
from ..constants import HITTING_CATEGORIES
from ..constants import PITCHING_CATEGORIES
from ..constants import FIELDING_CATEGORIES
from ..constants import BAT_FIELDS
from ..constants import BAT_FIELDS_ADV
from ..constants import PITCH_FIELDS
from ..constants import PITCH_FIELDS_ADV
from ..constants import FIELD_FIELDS
from ..constants import STATDICT

from ..constants import POSITION_DICT


async def parse_data(response,idx,mlbam):
    # team stats
    try:
        if idx == 0:
            season_stats_dict = {
                "hitting":None,
                "hittingAdvanced":None,
                "pitching":None,
                "pitchingAdvanced":None,
                "fielding":None,
                "records_dict":None
            }
        
        # SEASON STATS

            response = response["stats"]
            for group_type in response:
                statType = group_type["type"]["displayName"]
                statGroup = group_type["group"]["displayName"]

                stat_rows = []

                # HITTING
                if statGroup == "hitting" and statType == "season":
                    team_stats = group_type["splits"][0]["stat"]
                    single_row = []
                    used_headers = []
                    for stat in BAT_FIELDS:
                        try:
                            used_headers.append(stat)
                            single_row.append(team_stats[stat])
                        except:
                            single_row.append("--")
                    stat_rows.append(single_row)
                    season_stats_dict["hitting"] = pd.DataFrame(data=stat_rows,columns=used_headers).rename(columns=STATDICT)        
                elif statGroup == "hitting" and statType == "seasonAdvanced":
                    team_stats = group_type["splits"][0]["stat"]
                    single_row = []
                    used_headers = []
                    for stat in BAT_FIELDS_ADV:
                        try:
                            used_headers.append(stat)
                            single_row.append(team_stats[stat])
                        except:
                            single_row.append("--")
                    stat_rows.append(single_row)
                    season_stats_dict["hittingAdvanced"] = pd.DataFrame(data=stat_rows,columns=used_headers).rename(columns=STATDICT)

                # PITCHING
                elif statGroup == "pitching" and statType == "season":
                    team_stats = group_type["splits"][0]["stat"]
                    single_row = []
                    used_headers = []
                    for stat in PITCH_FIELDS:
                        try:
                            used_headers.append(stat)
                            single_row.append(team_stats[stat])
                        except:
                            single_row.append("--")
                    stat_rows.append(single_row)
                    season_stats_dict["pitching"] = pd.DataFrame(data=stat_rows,columns=used_headers).rename(columns=STATDICT)
                elif statGroup == "pitching" and statType == "seasonAdvanced":
                    team_stats = group_type["splits"][0]["stat"]
                    single_row = []
                    used_headers = []
                    for stat in PITCH_FIELDS_ADV:
                        try:
                            used_headers.append(stat)
                            single_row.append(team_stats[stat])
                        except:
                            single_row.append("--")
                    stat_rows.append(single_row)
                    season_stats_dict["pitchingAdvanced"] = pd.DataFrame(data=stat_rows,columns=used_headers).rename(columns=STATDICT)

                # FIELDING
                elif statGroup == "fielding" and statType == "season":
                    team_stats = group_type["splits"][0]["stat"]
                    single_row = []
                    used_headers = []
                    for stat in FIELD_FIELDS:
                        try:
                            used_headers.append(stat)
                            single_row.append(team_stats[stat])
                        except:
                            single_row.append("--")
                    stat_rows.append(single_row)
                    season_stats_dict["fielding"] = pd.DataFrame(data=stat_rows,columns=used_headers).rename(columns=STATDICT)


            return season_stats_dict
        
        # roster stats
        elif idx == 1:
            roster_stats_dict = {
                "hitting":None,
                "hittingAdvanced":None,
                "pitching":None,
                "pitchingAdvanced":None,
                "fielding":None
            }
            hitting = []
            hittingAdvanced = []
            pitching = []
            pitchingAdvanced = []
            fielding = []

            got_hit = False
            got_hitAdv = False
            got_pitch = False
            got_pitchAdv = False
            got_field = False

            hitting_cols = ["status","mlbam","playerName","primaryPosition"]
            hittingAdv_cols = ["status","mlbam","playerName","primaryPosition"]
            pitching_cols = ["status","mlbam","playerName","primaryPosition"]
            pitchingAdv_cols = ["status","mlbam","playerName","primaryPosition"]
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

                        # PITCHING
                        elif statGroup == "pitching" and statType == "season":
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
                        # FIELDING
                        elif statGroup == "fielding" and statType == "season":
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
                    # # print(f"Error retrieving stats for --- {playerName}")
                    pass
            roster_stats_dict['hitting'] = pd.DataFrame(hitting,columns=hitting_cols).rename(columns=STATDICT)
            roster_stats_dict['hittingAdvanced'] = pd.DataFrame(hittingAdvanced,columns=hittingAdv_cols).rename(columns=STATDICT)
            roster_stats_dict['pitching'] = pd.DataFrame(pitching,columns=pitching_cols).rename(columns=STATDICT)
            roster_stats_dict['pitchingAdvanced'] = pd.DataFrame(pitchingAdvanced,columns=pitchingAdv_cols).rename(columns=STATDICT)
            roster_stats_dict['fielding'] = pd.DataFrame(fielding,columns=fielding_cols).rename(columns=STATDICT)
            
            return roster_stats_dict
        
        # game log
        elif idx == 2:
            all_dates = response["dates"]
            all_games = []
            for date in all_dates:
                playdate = date["date"]

                for game in date["games"]:
                    gamePk = game["gamePk"]
                    if "rescheduledFromDate" in game.keys():
                        rescheduledFrom = game["rescheduledFromDate"]
                    else:rescheduledFrom = ""
                    if "rescheduledGameDate" in game.keys():
                        rescheduledTo = game["rescheduledGameDate"]
                    else:rescheduledTo = ""
                    try:
                        delay_status = f'{game["status"]["detailedState"]} ({game["status"]["reason"]})'
                    except:
                        delay_status = ""
                    venue_mlbam = game["venue"]["id"]
                    venue_name = game["venue"]["name"]
                    away = game["teams"]["away"]["team"]
                    home = game["teams"]["home"]["team"]
                    # away_name = away["clubName"]
                    away_mlbam = away["id"]
                    # home_name = home["clubName"]
                    home_mlbam = home["id"]
                    # away_score = game["teams"]["away"]["score"]
                    # home_score = game["teams"]["home"]["score"]
                    if int(mlbam) == int(away_mlbam):
                        isHome = False
                        vsSymbol = "@"
                        # team_name = away_name
                        # opponent_name = home_name
                        opponent_mlbam = home_mlbam
                    else:
                        isHome = True
                        vsSymbol = "vs"
                        # team_name = home_name
                        # opponent_name = away_name
                        opponent_mlbam = away_mlbam
                    runs = 0
                    hits = 0
                    errors = 0
                    leftOnBase = 0
                    if isHome is True:
                        leagueRecord = game["teams"]["home"]["leagueRecord"]
                        wins = leagueRecord["wins"]
                        losses = leagueRecord["losses"]
                        record = f"{wins}-{losses}"
                        try:
                            if game["teams"]["home"]["isWinner"] is True:
                                result = "W"
                            else:result = "L"
                        except: result = "--"
                        try:
                            for inning in game["linescore"]["innings"]:
                                try:runs += inning["home"]["runs"]
                                except:pass
                                try:hits += inning["home"]["hits"]
                                except:pass
                                try:errors += inning["home"]["errors"]
                                except:pass
                                try:leftOnBase += inning["home"]["leftOnBase"]
                                except:pass
                        except:
                            pass
                    else:
                        leagueRecord = game["teams"]["away"]["leagueRecord"]
                        wins = leagueRecord["wins"]
                        losses = leagueRecord["losses"]
                        record = f"{wins}-{losses}"
                        try:
                            if game["teams"]["away"]["isWinner"] is True:
                                result = "W"
                            else:result = "L"
                        except: result = "--"
                        try:
                            for inning in game["linescore"]["innings"]:
                                try:runs += inning["away"]["runs"]
                                except:pass
                                try:hits += inning["away"]["hits"]
                                except:pass
                                try:errors += inning["away"]["errors"]
                                except:pass
                                try:leftOnBase += inning["away"]["leftOnBase"]
                                except:pass
                        except:
                            pass
                        
                    try:
                        sky = game["weather"]["condition"]
                        if sky == "Unknown": sky = "--"
                    except:
                        sky = "--"
                    try:temp = game["weather"]["temp"]
                    except:temp = "--"
                    try:wind = game["weather"]["wind"]
                    except:wind = "--"
                    try:duration = game["gameInfo"]["gameDurationMinutes"]
                    except:duration = "--"
                    try:attendance = game["gameInfo"]["attendance"]
                    except:attendance = "--"
                    
                    dayNight = game.get("dayNight",'-').capitalize()
                    scheduled_innings = game.get("scheduledInnings","-")
                    games_in_series = game.get("gamesInSeries",'-')
                    series_game_number = game.get("seriesGameNumber",'-')

                    if rescheduledTo == "" and rescheduledFrom == "":
                        notes = ""
                    elif rescheduledFrom == "":
                        notes = f"PP Date {rescheduledTo}"
                    elif rescheduledTo == "":
                        notes = f"Makeup {rescheduledFrom}"

                    # try:
                    #     media_items = game["content"]["media"]["epgAlternate"]
                    #     for media in media_items:
                    #         if media["title"] == "Extended Highlights":
                    #             playbackId = media["items"][0]["mediaPlaybackId"]
                    #             url_extended_highlights = f"https://mlb-cuts-diamond.mlb.com/FORGE/{season}/{season}-{playdate[5:7]}/{playdate[8:]}/{playbackId}_1280x720_59_4000K.mp4"
                    #         elif media["title"] == "Daily Recap":
                    #             playbackId = media["items"][0]["mediaPlaybackId"]
                    #             url_highlights = f"https://mlb-cuts-diamond.mlb.com/FORGE/{season}/{season}-{playdate[5:7]}/{playdate[8:]}/{playbackId}_1280x720_59_4000K.mp4"
                    # except:
                    #     url_extended_highlights = ""
                    #     url_highlights = ""
                    # print(url_extended_highlights)
                    # print(url_highlights)

                    single_game = [
                        gamePk,
                        playdate,
                        mlbam,
                        vsSymbol,
                        opponent_mlbam,
                        venue_mlbam,
                        result,
                        record,
                        f"{series_game_number} of {games_in_series}",
                        runs,
                        hits,
                        errors,
                        leftOnBase,
                        venue_name,
                        attendance,
                        dayNight,
                        sky,
                        temp,
                        wind,
                        scheduled_innings,
                        duration,
                        delay_status,
                        notes]

                    all_games.append(single_game)
            season_games_df = pd.DataFrame(
                all_games,
                columns=[
                    'gamePk','Date','mlbam','-','opp_mlbam','venue_mlbam','Result','Record','SrsGm#','R','H','E','LOB','Venue','Attend.','D/N','Sky','Temp','Wind','#Inns','Elasped','','Notes'])
            return season_games_df
        
        # game stats
        elif idx == 3:
            gameLog_dict = {
                "hitting":None,
                "pitching":None,
                "fielding":None
            }
            for group_type in response["stats"]:
                statGroup = group_type["group"]["displayName"]

                # HITTING
                if statGroup == "hitting":
                    games_hitting = []
                    for game in group_type["splits"]:
                        headers = ["G#","gamePk","Date","home_or_away","opp_mlbam","Result"]
                        gamePk = game["game"]["gamePk"]
                        gameNum = game["game"]["gameNumber"]
                        date = game["date"]
                        opponent_mlbam = game["opponent"]["id"]
                        if game["isHome"] is True:
                            hm_aw = "Home"
                        else: hm_aw = "Away"
                        try:
                            if game["isWin"] is True:
                                result = "W"
                            else:
                                result = "L"
                        except:
                            result = "--"
                        game_row = [gameNum,gamePk,date,hm_aw,opponent_mlbam,result]
                        stats = game["stat"]
                        for game_stat in BAT_FIELDS:
                            headers.append(game_stat)
                            try:
                                game_row.append(stats[game_stat])
                            except:
                                game_row.append("--")
                        games_hitting.append(game_row)
                    gameLog_dict["hitting"] = pd.DataFrame(games_hitting,columns=headers).rename(columns=STATDICT)

                # PITCHING
                elif statGroup == "pitching":
                    games_pitching = []
                    for game in group_type["splits"]:
                        headers = ["G#","gamePk","Date","home_or_away","opp_mlbam","Result"]
                        gamePk = game["game"]["gamePk"]
                        gameNum = game["game"]["gameNumber"]
                        date = game["date"]
                        opponent_mlbam = game["opponent"]["id"]
                        if game["isHome"] is True:
                            hm_aw = "Home"
                        else: hm_aw = "Away"
                        try:
                            if game["isWin"] is True:
                                result = "W"
                            else:
                                result = "L"
                        except:
                            result = "--"

                        game_row = [gameNum,gamePk,date,hm_aw,opponent_mlbam,result]
                        stats = game["stat"]
                        for game_stat in PITCH_FIELDS:
                            headers.append(game_stat)
                            try:
                                game_row.append(stats[game_stat])
                            except:
                                game_row.append("--")
                        games_pitching.append(game_row)
                    gameLog_dict["pitching"] = pd.DataFrame(games_pitching,columns=headers).rename(columns=STATDICT)
                
                # FIELDING
                elif statGroup == "fielding":
                    games_fielding = []
                    for game in group_type["splits"]:
                        headers = ["G#","gamePk","Date","home_or_away","opp_mlbam","Result"]
                        gamePk = game["game"]["gamePk"]
                        gameNum = game["game"]["gameNumber"]
                        date = game["date"]
                        opponent_mlbam = game["opponent"]["id"]
                        if game["isHome"] is True:
                            hm_aw = "Home"
                        else: hm_aw = "Away"
                        try:
                            if game["isWin"] is True:
                                result = "W"
                            else:
                                result = "L"
                        except:
                            result = "--"

                        game_row = [gameNum,gamePk,date,hm_aw,opponent_mlbam,result]
                        stats = game["stat"]
                        for game_stat in FIELD_FIELDS:
                            headers.append(game_stat)
                            try:
                                game_row.append(stats[game_stat])
                            except:
                                game_row.append("--")
                        games_fielding.append(game_row)
                    gameLog_dict["fielding"] = pd.DataFrame(games_fielding,columns=headers).rename(columns=STATDICT)

            return gameLog_dict
        
        # leaders (idx 4: hitting, idx 5: pitching, idx 6:fielding)
        elif 4 <= idx <= 6:
            leaders = {}
            for category in response["leagueLeaders"]:
                try:
                    category_name = category["leaderCategory"]
                    entries_in_category = []
                    for leader in category["leaders"]:
                        entry = {}
                        entry["stat"] = category["leaderCategory"]
                        entry["rank"] = leader["rank"]
                        entry["name"] = leader["person"]["fullName"]
                        entry["mlbam"] = leader["person"]["id"]
                        entry["value"] = leader["value"]
                        entry["season"] = leader["season"]
                        entry["team_name"] = leader["team"]["name"]
                        entry["team_mlbam"] = leader["team"]["id"]
                        entry["league_name"] = leader["league"]["name"]
                        entry["league_mlbam"] = leader["league"]["id"]
                        entry["gameType"] = category["gameType"]["id"]

                        entries_in_category.append(entry)

                    leaders[category_name] = entries_in_category
                except:
                    # print("ERROR with: ")
                    leaders[category_name] = []
                    # print(category["statGroup"])
                    # print(category_name)
            return leaders
        
        
        # transactions
        elif idx == 7:
            transactions = []
            for trx in response["transactions"]:
                transactions.append([
                    trx.get("id"),
                    trx.get("person",{}).get("id","-"),
                    trx.get("person",{}).get("fullName","-"),
                    trx.get("fromTeam",{}).get("name","-"),
                    trx.get("fromTeam",{}).get("id","-"),
                    trx.get("toTeam",{}).get("name","-"),
                    trx.get("toTeam",{}).get("id","-"),
                    trx.get("date","-"),
                    trx.get("effectiveDate","-"),
                    trx.get("typeCode","-"),
                    trx.get("typeDesc","-"),
                    trx.get("description","-")
                ])
            columns = ("trade_id","player_mlbam","player","f_team","f_team_mlbam","t_team","t_team_mlbam","date","eff_date","trx_code","trx_type","description")
            df = pd.DataFrame(data=transactions,columns=columns)
            return df

        # draft picks
        elif idx == 8:
            drafts = response["drafts"]
            picks = []
            for r in drafts["rounds"]:
                for p in r["picks"]:
                    person = p.get("person",{})
                    pick = {
                        "bisPlayerId":p["bisPlayerId"],
                        "pickRound":p["pickRound"],
                        "pickNumber":p["pickNumber"],
                        "roundPickNumber":p["roundPickNumber"],
                        "rank":p.get("rank","-"),
                        "pickValue":p.get("pickValue","-"),
                        "scoutingReport":p.get("scoutingReport","-"),
                        "homeCity":p.get("home",{}).get("city","-"),
                        "homeState":p.get("home",{}).get("state","-"),
                        "homeCountry":p.get("home",{}).get("country","-"),
                        "schoolName":p.get("school",{}).get("name","-"),
                        "schoolClass":p.get("school",{}).get("class","-"),
                        "schoolState":p.get("school",{}).get("state","-"),
                        "schoolCountry":p.get("school",{}).get("country","-"),
                        "blurb":p.get("blurb","-"),
                        "headshotLink":p.get("headshotLink",""),
                        "nameFirst":person.get("firstName","-"),
                        "nameLast":person.get("lastName","-"),
                        "nameFull":person.get("fullName","-"),
                        "nameGiven":person.get("fullFMLName","-"),
                        "birthDate":person.get("birthDate","-"),
                        "primaryPosition":person.get("primaryPosition",{}).get("abbreviation"),
                        "draftYear":person.get("draftYear","-"),
                        "bats":person.get("batSide",{}).get("code","-"),
                        "throws":person.get("pitchHand",{}).get("code","-"),
                        "team":p.get("team",{}).get("name","-"),
                        "team_mlbam":p.get("team",{}).get("id","-")

                    }
                    
                    pick_list = list(pick.values()) # might turn into dictionary later
                    picks.append(pick_list)
            columns = list(pick.keys())
            df = pd.DataFrame(data=picks,columns=columns)
            return df

        # AWARDS...
        else:
            # print('something didn\'t work in awards call')
            return None
        
    except Exception as e:
        # print("ERROR ON index:",idx)
        # print(f"--- {e} ---")
        return None

async def get_team_responses(mlbam,season):
    parsed_data = []
    statTypes = "season,seasonAdvanced"
    statGroups = "hitting,pitching,fielding"
    team_hydrations = "team(standings)"
    roster_hydrations = f"person(stats(type=[{statTypes}],group=[{statGroups}],season={season}))&season={season}"
    log_hydrations = "" # "team,decisions,gameInfo,venue,linescore,weather,series"
    leader_ep = "/stats/leaders?leaderCategories="

    h_cats = ",".join(HITTING_CATEGORIES)
    p_cats = ",".join(PITCHING_CATEGORIES)
    f_cats = ",".join(FIELDING_CATEGORIES)

    async with aiohttp.ClientSession() as session:
        
        endpoints = {
            "team stats":   f"/teams/{mlbam}/stats?stats={statTypes}&group={statGroups}&season={season}",
            "roster stats": f"/teams/{mlbam}/roster?rosterType=fullSeason&hydrate={roster_hydrations}",
            "game log":     f"/schedule?&hydrate={log_hydrations}&season={season}&sportId=1&teamId={mlbam}&gameType=R",
            "game stats":   f"/teams/{mlbam}/stats?stats=gameLog&group={statGroups}&season={season}",
            "hit_leaders": leader_ep + f"{h_cats}&statType=season&teamId={mlbam}&gameType=R&season={season}&statGroup=hitting&limit=1000&playerPool=all",
            "pitch_leaders": leader_ep + f"{p_cats}&statType=season&teamId={mlbam}&gameType=R&season={season}&statGroup=pitching&limit=1000&playerPool=all",
            "field_leaders": leader_ep + f"{f_cats}&statType=season&teamId={mlbam}&gameType=R&season={season}&statGroup=fielding&limit=1000&playerPool=all",
            "transactions": f"/transactions?teamId={mlbam}&startDate=1/1/{season}&endDate=12/1/{season}",
            "draft":        f"/draft/{season}?teamId={mlbam}"
            # "retired nums": f"/awards/RETIREDUNI_{mlbam}/recipients?sportId=1&hydrate=results",
            
            }
        
        tasks = []
        for ep in endpoints.values():
            tasks.append(session.get(BASE + ep, ssl=False))

        responses = await asyncio.gather(*tasks)
        
        for idx, response in enumerate(responses):
            resp = await response.json()
            parsed = await parse_data(resp,idx,mlbam)
            parsed_data.append(parsed)

    parsed_data_dict = {
        "team_stats":parsed_data[0],
        "roster_stats":parsed_data[1],
        "game_log":parsed_data[2],
        "game_stats":parsed_data[3],
        "leaders_hitting":parsed_data[4],
        "leaders_pitching":parsed_data[5],
        "leaders_fielding":parsed_data[6],
        "transactions":parsed_data[7],
        "draft":parsed_data[8]}
    
    return parsed_data_dict

def runit(mlbam,season):
    # start = time.time()
    # loop = asyncio.get_event_loop()
    # retrieved = asyncio.run_coroutine_threadsafe(get_team_responses(mlbam,season), loop)
    # retrieved = loop.create_task(get_team_responses(mlbam,season))

    # retrieved = asyncio.run(get_team_responses(mlbam,season))
    # print("--- {} seconds ---".format(time.time()-start))
    
    # return retrieved.result()
    # r =  asyncio.run_coroutine_threadsafe(get_team_responses(mlbam,season),loop=loop)
    retrieved = asyncio.run(get_team_responses(mlbam,season))
    return retrieved
