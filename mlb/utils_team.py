import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from bs4.element import Comment
from bs4.element import SoupStrainer

from .constants import BASE
from .constants import HITTING_CATEGORIES
from .constants import PITCHING_CATEGORIES
from .constants import FIELDING_CATEGORIES
from .constants import BBREF_SPLITS
from .constants import BAT_FIELDS
from .constants import STATDICT
from .constants import BAT_FIELDS_ADV
from .constants import PITCH_FIELDS
from .constants import PITCH_FIELDS_ADV
from .constants import FIELD_FIELDS
# from .constants import STAT_GROUPS


# from .constants import bbref_hit_log_fields
# from .constants import bbref_pitch_log_fields
# from .constants import bbref_field_log_fields

from .mlbdata import get_franchise_df
# from .mlbdata import get_playoff_history
# from .mlbdata import get_yby_records

from .utils import curr_year

def team(teamID,year):
    try:
        mlbam = int(teamID)
        franchise_df = get_franchise_df(teamID=mlbam)
        bbrefID = franchise_df[franchise_df["yearID"]==year]["bbrefID"].item()
    except:
        bbrefID = teamID
        franchise_df = get_franchise_df(teamID=bbrefID)
        mlbam = franchise_df[franchise_df["yearID"]==year]["mlbam"].item()
    team_df = franchise_df.set_index("yearID").loc[year]
    franchID = team_df.franchID
    retroID = team_df.retroID
    mlbID = team_df.mlbID
    fullName = team_df.fullName
    lgAbbrv = team_df.lgAbbrv
    locationName =  team_df.locationName
    clubName = team_df.clubName
    venueName = team_df.venueName
    venue_mlbam = team_df.venue_mlbam
    firstYear = team_df.firstYear

    return {
        "franchise_df":franchise_df,
        "team_df":team_df,
        "mlbam":mlbam,
        "bbrefID":bbrefID,
        "franchID":franchID,
        "retroID":retroID,
        "mlbID":mlbID,
        "fullName":fullName,
        "lgAbbrv":lgAbbrv,
        "locationName":locationName,
        "clubName":clubName,
        "venueName":venueName,
        "venue_mlbam":venue_mlbam,
        "firstYear":firstYear}

def stats(mlbam,season=curr_year):
    statTypes = "season,seasonAdvanced"
    STAT_GROUPS = "hitting,pitching,fielding"
    hydrations = "team(standings)"
    url = BASE + f"/teams/{mlbam}/stats?stats={statTypes}&group={STAT_GROUPS}&season={season}&hydrate={hydrations}"
    response = requests.get(url).json()["stats"]
    season_stats_dict = {
        "hitting":None,
        "hittingAdvanced":None,
        "pitching":None,
        "pitchingAdvanced":None,
        "fielding":None,
        "records_dict":None
    }
  #
   # SEASON STATS
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

   # SEASON RECORDS
    records_dict = {}
    records = response[0]["splits"][0]["team"]["record"]
    record_cats = records["records"]
    # Total
    records_dict["wins"] = records["wins"]
    records_dict["losses"] = records["losses"]
    records_dict["runs"] = records["runsScored"]
    records_dict["runsAllowed"] = records["runsAllowed"]
    records_dict["runDiff"] = records["runDifferential"]

    # League Records
    for rec in record_cats["leagueRecords"]:
        if rec["league"]["id"] == 100:      # American Association (INACTIVE)
            records_dict["nlWins"] = rec["wins"]
            records_dict["nlLosses"] = rec["losses"]
        elif rec["league"]["id"] == 101:    # Union Association (INACTIVE)
            records_dict["nlWins"] = rec["wins"]
            records_dict["nlLosses"] = rec["losses"]
        elif rec["league"]["id"] == 102:    # National Association (INACTIVE)
            records_dict["nlWins"] = rec["wins"]
            records_dict["nlLosses"] = rec["losses"]
        elif rec["league"]["id"] == 103:    # American League
            records_dict["alWins"] = rec["wins"]
            records_dict["alLosses"] = rec["losses"]
        elif rec["league"]["id"] == 104:    # National League
            records_dict["nlWins"] = rec["wins"]
            records_dict["nlLosses"] = rec["losses"]
        elif rec["league"]["id"] == 105:    # Players League (INACTIVE)
            records_dict["nlWins"] = rec["wins"]
            records_dict["nlLosses"] = rec["losses"]
        elif rec["league"]["id"] == 106:    # Federal League (INACTIVE)
            records_dict["nlWins"] = rec["wins"]
            records_dict["nlLosses"] = rec["losses"]

    # Division Records
    for rec in record_cats["divisionRecords"]:
        if rec["division"]["id"] == 200:
            records_dict["westWins"] = rec["wins"]
            records_dict["westLosses"] = rec["losses"]
        elif rec["division"]["id"] == 201:
            records_dict["eastWins"] = rec["wins"]
            records_dict["eastLosses"] = rec["losses"]
        elif rec["division"]["id"] == 202:
            records_dict["centralWins"] = rec["wins"]
            records_dict["centralLosses"] = rec["losses"]
        elif rec["division"]["id"] == 203:
            records_dict["westWins"] = rec["wins"]
            records_dict["westLosses"] = rec["losses"]
        elif rec["division"]["id"] == 204:
            records_dict["eastWins"] = rec["wins"]
            records_dict["eastLosses"] = rec["losses"]
        elif rec["division"]["id"] == 205:
            records_dict["centralWins"] = rec["wins"]
            records_dict["centralLosses"] = rec["losses"]

    # Split Records
    for rec in record_cats["splitRecords"]:
        if rec["type"] == "home":
            records_dict["homeWins"] = rec["wins"]
            records_dict["homeLosses"] = rec["losses"]
        elif rec["type"] == "away":
            records_dict["awayWins"] = rec["wins"]
            records_dict["awayLosses"] = rec["losses"]
        elif rec["type"] == "lastTen":
            records_dict["last10Wins"] = rec["wins"]
            records_dict["last10Losses"] = rec["losses"]
        elif rec["type"] == "extraInning":
            records_dict["exInnWins"] = rec["wins"]
            records_dict["exInnLosses"] = rec["losses"]
        elif rec["type"] == "oneRun":
            records_dict["oneRunWins"] = rec["wins"]
            records_dict["oneRunLosses"] = rec["losses"]
        elif rec["type"] == "winners":
            records_dict["winnersWins"] = rec["wins"]
            records_dict["winnersLosses"] = rec["losses"]
        elif rec["type"] == "day":
            records_dict["dayWins"] = rec["wins"]
            records_dict["dayLosses"] = rec["losses"]
        elif rec["type"] == "night":
            records_dict["nightWins"] = rec["wins"]
            records_dict["nightLosses"] = rec["losses"]
        elif rec["type"] == "grass":
            records_dict["grassWins"] = rec["wins"]
            records_dict["grassLosses"] = rec["losses"]
        elif rec["type"] == "turf":
            records_dict["turfWins"] = rec["wins"]
            records_dict["turfLosses"] = rec["losses"]
   
   #
    season_stats_dict["records_dict"] = records_dict
    return season_stats_dict

def roster_stats(mlbam,season=curr_year):
    statTypes = "season,seasonAdvanced"
    STAT_GROUPS = "hitting,pitching,fielding"
    hydrations = f"person(stats(type=[{statTypes}],group=[{STAT_GROUPS}],season={season}))&season={season}"
    url = BASE + f"/teams/{mlbam}/roster?rosterType=fullSeason&hydrate={hydrations}"
    # print(url)
    roster_response = requests.get(url).json()["roster"]
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

    for player in roster_response:
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
            pass
    roster_stats_dict['hitting'] = pd.DataFrame(hitting,columns=hitting_cols).rename(columns=STATDICT)
    roster_stats_dict['hittingAdvanced'] = pd.DataFrame(hittingAdvanced,columns=hittingAdv_cols).rename(columns=STATDICT)
    roster_stats_dict['pitching'] = pd.DataFrame(pitching,columns=pitching_cols).rename(columns=STATDICT)
    roster_stats_dict['pitchingAdvanced'] = pd.DataFrame(pitchingAdvanced,columns=pitchingAdv_cols).rename(columns=STATDICT)
    roster_stats_dict['fielding'] = pd.DataFrame(fielding,columns=fielding_cols).rename(columns=STATDICT)
    
    return roster_stats_dict

def game_log(mlbam,season=curr_year):
    gameTypes = "R,D,W,F,L,E"
    url = BASE + f"/schedule?&startDate=1/1/{season}&endDate=12/1/{season}&sportId=1&teamId={mlbam}&gameType={gameTypes}"
    # print(url)
    # print('retrieving team schedule...')
    with requests.Session() as sesh:
        all_dates = sesh.get(url).json()["dates"]
    # print('success')
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

def game_stats(mlbam,season=curr_year):
    statTypes = "gameLog"
    STAT_GROUPS = "hitting,pitching,fielding"
    url = BASE + f"/teams/{mlbam}/stats?stats={statTypes}&group={STAT_GROUPS}&season={season}"
    # print(url)

    gameLog_dict = {
        "hitting":None,
        "pitching":None,
        "fielding":None
    }
    gameLog = requests.get(url).json()["stats"]
    for group_type in gameLog:
        statGroup = group_type["group"]["displayName"]

        # HITTING
        if statGroup == "hitting":
            games_hitting = []
            for game in group_type["splits"]:
                headers = ["G#","gamePk","Date","","opp_mlbam","Result"]
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
                headers = ["G#","gamePk","Date","","opp_mlbam","Result"]
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
                headers = ["G#","gamePk","Date","","opp_mlbam","Result"]
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

def team_leaders(mlbam,statGroup="",season=None,gameTypes=[],sitCodes=None):
    if type(gameTypes) is list and len(gameTypes) == 0:
        gameTypes = "R"
    elif type(gameTypes) is list:
        gameTypes = ",".join(gameTypes).upper()
    else:
        gameTypes = gameTypes.upper()
        
    leader_ep = BASE + "/stats/leaders?leaderCategories="

    h_cats = ",".join(HITTING_CATEGORIES)
    p_cats = ",".join(PITCHING_CATEGORIES)
    f_cats = ",".join(FIELDING_CATEGORIES)

    if sitCodes is None:
        sitCodes = ""
    else:
        if type(sitCodes) is list:
            sitCodes = f"&sitCodes={','.join(sitCodes)}"
        elif type(sitCodes) is str:
            sitCodes = f"&sitCodes={sitCodes.replace(' ','')}"
        else:
            print("sitCodes must be comma delimited string or list type")
            return None

    if season is not None:
        statType = "statType=season"
        url_hitting = leader_ep + f"{h_cats}&{statType}&teamId={mlbam}&gameType=R&season={season}&statGroup=hitting&limit=1000{sitCodes}",
        url_pitching = leader_ep + f"{p_cats}&{statType}&teamId={mlbam}&gameType=R&season={season}&statGroup=pitching&limit=1000{sitCodes}",
        url_fielding = leader_ep + f"{f_cats}&{statType}&teamId={mlbam}&gameType=R&season={season}&statGroup=fielding&limit=1000{sitCodes}"
    else:
        statType = "statType=statsSingleSeason"
        url_hitting = leader_ep + f"{h_cats}&{statType}&teamId={mlbam}&gameType=R&statGroup=hitting&limit=1000{sitCodes}",
        url_pitching = leader_ep + f"{p_cats}&{statType}&teamId={mlbam}&gameType=R&statGroup=pitching&limit=1000{sitCodes}",
        url_fielding = leader_ep + f"{f_cats}&{statType}&teamId={mlbam}&gameType=R&statGroup=fielding&limit=1000{sitCodes}"
    responses = {"hitting":None,"pitching":None,"fielding":None}
    with requests.session() as sesh:
        responses["hitting"] = sesh.get(url_hitting)
        responses["pitching"] = sesh.get(url_pitching)
        responses["fielding"] = sesh.get(url_fielding)
    entries = []
    # for entry in response.json()["leagueLeaders"]:
    #     print(entry["leaderCategory"],"|",entry["statGroup"])

    return responses["hitting"].json()

def bbrefSplits(bbrefID,season=curr_year,s_type="b"):
    req = requests.get(f"https://www.baseball-reference.com/teams/split.cgi?t={s_type}&team={bbrefID}&year={season}")
    strainer = SoupStrainer("div")
    soup = bs(req.content,"lxml",parse_only=strainer)
    split_tables = {}
    split_tables["total"] = pd.read_html(str(soup.find("table",id="total")))[0].drop(columns=["tOPS+","sOPS+"],errors='ignore')
    split_tables["platoon"] = pd.read_html(str(soup.find("table",id="plato")))[0].drop(columns=["tOPS+","sOPS+"],errors='ignore')
    comments = soup.findAll(text=lambda text:isinstance(text, Comment))
    for c in comments:
        for split_type in BBREF_SPLITS.keys():
            if "div_"+split_type in c:
                split_tables[BBREF_SPLITS[split_type]] = pd.read_html(c.extract())[0].drop(columns=["tOPS+","sOPS+"],errors='ignore')
    return split_tables

