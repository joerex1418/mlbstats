import datetime as dt
from typing import Union, List, Dict

import pytz
import pandas as pd

from . import utils
from . import constants as c
from . import mlbdata

TEAMS = mlbdata.get_teams_df()
LEAGUES = mlbdata.get_leagues_df()
_JSON = Union[Dict,List]

def _parse_player_stats(splits:list[dict],**kwargs) -> pd.DataFrame:
    data = []
    for s in splits:
        season_data = {}
        season_data['season'] = s.get('season')
        season_data['player_mlbam'] = s.get('player',{}).get('id')
        season_data['player_name'] = s.get('player',{}).get('fullName')
        
        season_data['team_mlbam'] = s.get('team',{}).get('id')
        
        stat = s.get('stat',{})
        if stat.get('position',{}).get('abbreviation') is not None:
            stat['position'] = stat['position']['abbreviation']
        season_data.update(stat)
        
        data.append(season_data)
    
    df = pd.DataFrame(data).sort_values(by='season',ascending=False)
    # if kwargs.get('include_team_name') is not False:
    #     df.insert(4,'team_name',df.apply(lambda row: add_team_attr(row,'name_full'),axis=1))

    if kwargs.get('keep_original_keys'):
        return df
        
    return df.rename(columns=c.STATDICT)
    
def _parse_team_stats(splits:list[dict],include_lg_short:bool=False,**kwargs) -> pd.DataFrame:
    data = []
    
    for s in splits:
        season_data = {}
        season_data['season'] = s.get('season')
        season_data['team_mlbam'] = s.get('team',{}).get('id')
        season_data.update(s.get('stat',{}))
        data.append(season_data)
    
    df = pd.DataFrame(data).sort_values(by='season',ascending=False)
    # if include_lg_short:
        # df.insert(2,'lg_abbrv',df.apply(lambda row: add_league_short(row),axis=1))
    # df.insert(2,'lg_mlbam',df.apply(lambda row: add_league_attr(row,'mlbam'),axis=1))
    # df.insert(2,'team_name',df.apply(lambda row: add_team_attr(row,'name_full'),axis=1))
    
    if kwargs.get('keep_original_keys'):
        return df.reset_index(drop=True)
        
    return df.rename(columns=c.STATDICT).reset_index(drop=True)

def _parse_league_stats(splits:list[dict]) -> pd.DataFrame:
    data = []

    season_col = []
    team_mlbam_col = []
    team_name_col = []
    team_abbrv_col = []
    league_mlbam_col = []
    div_mlbam_col = []

    for tm in splits:
        season = tm.get("season")
        team_mlbam = tm.get("team",{}).get("id")
        team_name = tm.get("team",{}).get("name")
        
        season_col.append(season)
        team_mlbam_col.append(team_mlbam)
        team_name_col.append(team_name)
        team_abbrv_col.append(TEAMS[TEAMS['mlbam']==team_mlbam].iloc[0]['mlbID'])
        league_mlbam_col.append(TEAMS[TEAMS['mlbam']==team_mlbam].iloc[0]['lg_mlbam'])
        div_mlbam_col.append(TEAMS[TEAMS['mlbam']==team_mlbam].iloc[0]['div_mlbam'])

        tm_stats = tm.get("stat")

        data.append(pd.Series(tm_stats))
    
    df = pd.DataFrame(data=data,columns=tm_stats.keys()).rename(columns=c.STATDICT)
    
    df.insert(0,"Season",season_col)
    df.insert(1,"team_mlbam",team_mlbam_col)
    df.insert(2,"team",team_name_col)
    df.insert(3,"team_abbrv",team_abbrv_col)
    df.insert(4,"league_mlbam",league_mlbam_col)
    df.insert(5,"div_mlbam",div_mlbam_col)
    
    return df

def _parse_schedule_data(json_response:_JSON,selected_timezone:str=None) -> list[dict]:
    dates = json_response['dates']

    data = []
    for d in dates:
        games:list[dict] = d['games']
        date = d['date']
        for g in games:
            teams = g["teams"]
            away = teams["away"]
            home = teams["home"]
            
            linescore = g.get('linescore',{})
            inn = linescore.get('currentInning','')
            inn_ord = linescore.get('currentInningOrdinal','')
            inn_state = linescore.get('inningState','')
            inn_half = linescore.get('inningHalf','')
            
            offense = linescore.get('offense',{})
            defense = linescore.get('defense',{})
            at_bat_tm = offense.get('team',{}).get('id',0)
            if at_bat_tm == str(int(away.get("team").get("id"))):
                at_bat_tm = 'away'
            else:
                at_bat_tm = 'home'
            at_bat = offense.get('batter',{})
            at_bat_mlbam  = at_bat.get('id',0)
            at_bat_name = at_bat.get('lastInitName','')
            on_mound = defense.get('pitcher',{})
            on_mound_mlbam  = on_mound.get('id',0)
            on_mound_name = on_mound.get('lastInitName','')
            
            game_date = g.get("gameDate")
            resched_date = g.get("rescheduleDate")
            date_official = g.get("officialDate")
            date_sched = date
            date_resched = g.get("rescheduleGameDate")

            if type(game_date) is str:
                game_dt = dt.datetime.strptime(game_date,r"%Y-%m-%dT%H:%M:%SZ")
                game_dt = pytz.utc.fromutc(game_dt)
                game_dt = game_dt.astimezone(selected_timezone)
                sched_time = game_dt.strftime(utils.standard_time_format)
                game_start = sched_time
            else:
                game_dt = "-"
                sched_time = "-"

            if type(resched_date) is str:
                resched_dt = dt.datetime.strptime(resched_date,r"%Y-%m-%dT%H:%M:%SZ")
                resched_dt = pytz.utc.fromutc(resched_dt)
                resched_dt = resched_dt.astimezone(selected_timezone)
                resched_time = resched_dt.strftime(utils.standard_time_format)
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
            away_record = f'{away.get("leagueRecord",{}).get("wins")}-{away.get("leagueRecord",{}).get("losses")}'
            aw_pp = away.get("probablePitcher",{})
            aw_pp_mlbam = aw_pp.get("id","")
            # aw_pp_name = aw_pp.get("fullName","")
            aw_pp_name = aw_pp.get("lastInitName","")

            home_mlbam = str(int(home.get("team").get("id")))
            home_name = home.get("team").get("name")
            home_record = f'{home.get("leagueRecord",{}).get("wins")}-{home.get("leagueRecord",{}).get("losses")}'
            hm_pp = home.get("probablePitcher",{})
            hm_pp_mlbam = hm_pp.get("id","")
            # hm_pp_name = hm_pp.get("fullName","")
            hm_pp_name = hm_pp.get("lastInitName","")

            status = g.get("status",{})
            abstract_state = status.get("abstractGameState")
            abstract_code = status.get("abstractGameCode")
            detailed_state = status.get("detailedState")
            detailed_code = status.get("codedGameState")
            status_code = status.get("statusCode")
            reason = status.get("reason")
            
            balls = linescore.get('balls',0)
            strikes = linescore.get('strikes',0)
            outs = linescore.get('outs',0)
            
            up_next_name = offense.get('batter',{}).get('lastInitName','')
            up_next_mlbam = offense.get('batter',{}).get('id',0)
            on_deck_name = offense.get('onDeck',{}).get('lastInitName','')
            on_deck_mlbam = offense.get('onDeck',{}).get('id',0)
            in_hole_name = offense.get('inHole',{}).get('lastInitName','')
            in_hole_mlbam = offense.get('inHole',{}).get('id',0)
            
            if detailed_state == "In Progress" and (inn_state == "Middle" or inn_state == "End"):
                lineup_key = "awayPlayers" if inn_state == "Middle" else "homePlayers"
                
                up_next_mlbam = defense.get('batter',{}).get('id',0)
                on_deck_mlbam = defense.get('onDeck',{}).get('id',0)
                in_hole_mlbam = defense.get('inHole',{}).get('id',0)
                
                got_upnext, got_ondeck, got_inhole = False, False, False
                for p in g.get('lineups',{}).get(lineup_key,[{}]):
                    if p.get('id') == up_next_mlbam:
                        up_next_name = p.get('lastInitName','')
                    elif p.get('id') == on_deck_mlbam:
                        on_deck_name = p.get('lastInitName','')
                    elif p.get('id') == in_hole_mlbam:
                        in_hole_name = p.get('lastInitName','')
                    
                    if got_upnext and got_ondeck and got_inhole:
                        break
            
            decisions: dict = g.get('decisions',{})
            winner: dict = decisions.get('winner',{})
            loser: dict = decisions.get('loser',{})
            save: dict = decisions.get('save',{})
            win_name: str = winner.get('lastInitName','')
            win_mlbam: int = winner.get('id',0)
            loss_name: str = loser.get('lastInitName','')
            loss_mlbam: int = loser.get('id',0)
            save_name: str = save.get('lastInitName','')
            save_mlbam: int = save.get('id',0)
            
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
            
            data.append({
                'game_start': game_start,
                'date_sched': date_sched,
                'date_resched': date_resched,
                'date_official': date_official,
                'sched_dt': sched_dt,
                'sched_time': sched_time,
                'resched_dt': resched_dt,
                'resched_time': resched_time,
                'gamePk': gamePk,
                'game_type': game_type,
                'inn': inn,
                'inn_ord': inn_ord,
                'inn_state': inn_state,
                'inn_half': inn_half,
                'venue_mlbam': venue_mlbam,
                'venue_name': venue_name,
                'away_mlbam': away_mlbam,
                'away_name': away_name,
                'away_score': away_score,
                'away_record': away_record,
                'aw_pp_name': aw_pp_name,
                'aw_pp_mlbam': aw_pp_mlbam,
                'home_mlbam': home_mlbam,
                'home_name': home_name,
                'home_score': home_score,
                'home_record': home_record,
                'hm_pp_name': hm_pp_name,
                'hm_pp_mlbam': hm_pp_mlbam,
                'at_bat_tm': at_bat_tm,
                'at_bat_mlbam': at_bat_mlbam,
                'at_bat_name': at_bat_name,
                'on_mound_mlbam': on_mound_mlbam,
                'on_mound_name': on_mound_name,
                'up_next_name': up_next_name,
                'up_next_mlbam': up_next_mlbam,
                'on_deck_name': on_deck_name,
                'on_deck_mlbam': on_deck_mlbam,
                'in_hole_name': in_hole_name,
                'in_hole_mlbam': in_hole_mlbam,
                'abstract_state': abstract_state,
                'abstract_code': abstract_code,
                'detailed_state': detailed_state,
                'detailed_code': detailed_code,
                'status_code': status_code,
                'reason': reason,
                'balls': balls,
                'strikes': strikes,
                'outs': outs,
                'win_name': win_name,
                'win_mlbam': win_mlbam,
                'loss_name': loss_name,
                'loss_mlbam': loss_mlbam,
                'save_name': save_name,
                'save_mlbam': save_mlbam,
                'bc_tv_aw': bc_tv_aw,
                'bc_tv_aw_res': bc_tv_aw_res,
                'bc_tv_hm': bc_tv_hm,
                'bc_tv_hm_res': bc_tv_hm_res,
                'bc_radio_aw': bc_radio_aw,
                'bc_radio_hm': bc_radio_hm,
                'recap_url': recap_url,
                'recap_title': recap_title,
                'recap_desc': recap_desc,
                'recap_avail': recap_avail,
                }
            )
    
    return data

def _parse_season_standings_data(json_response:_JSON) -> list[dict]:
    records = json_response.get('records',[{}])
    data = []
    for rec in records:
        lg_mlbam = rec.get('league',{}).get('id',0)
        div_mlbam = rec.get('division',{}).get('id',0)
        
        for tr in rec.get("teamRecords",[{}]):
            team = tr.get('team',{})
            data.append({
                'team_mlbam':team.get('id'),
                'team_abbrv':team.get('abbreviation'),
                'league':lg_mlbam,
                'division':div_mlbam,
                'games_played':tr.get('gamesPlayed',0),
                'wc_games_back':tr.get('wildCardGamesBack'),
                'lg_games_back':tr.get('leagueGamesBack','-'),
                'div_games_back':tr.get('divisionGamesBack','-'),
                'sp_games_back':tr.get('sportGamesBack','-'),
                'lg_rank':tr.get('leagueRank'),
                'div_rank':tr.get('divisionRank'),
                'sp_rank':tr.get('sportRank'),
                'wins':tr.get('wins',0),
                'losses':tr.get('losses',0),
                'runs_scored':tr.get('runsScored',0),
                'runs_allowed':tr.get('runsAllowed',0),
                'run_diff':tr.get('runDifferential','0'),
                'win_perc':tr.get('winningPercentage','.000')
                })

    return data

def _parse_roster(json_response:_JSON):
    data = []
    for entry in json_response.get('roster',[{}]):
        person = entry.get('person',{})
        data.append({'player_mlbam':person.get('id',0),
                     'player_name':person.get('fullName',''),
                     'pos':entry.get('position',{}).get('abbreviation'),
                     'jersey':entry.get('jerseyNumber',''),
                     'status_code':entry.get('status',{}).get('code'),
                     'status_desc':entry.get('status',{}).get('description')
                     })
    
    return pd.DataFrame(data)

def _parse_transaction_data(json_response:_JSON):
    transactions = json_response.get('transactions',[{}])
    data = []
    for trx in transactions:
        trx_id = trx.get('id',0)
        person_mlbam = trx.get('person',{}).get('id',0)
        person_name = trx.get('person',{}).get('fullName','')
        
        to_team = trx.get('toTeam',{})
        to_team_mlbam = to_team.get('id',0)
        to_team_name = to_team.get('name','')
        from_team = trx.get('fromTeam',{})
        from_team_mlbam = from_team.get('id',0)
        from_team_name = from_team.get('name','')
        
        date = trx['date']
        date_obj = dt.datetime.strptime(date,r'%Y-%m-%d').date()
        effective_date = trx.get('effectiveDate','')
        resolution_date = trx.get('resolutionDate','')
        type_code = trx.get('typeCode','')
        type_desc = trx.get('typeDesc','')
        description = trx.get('description','')
        
        data.append({
            'transaction_id':trx_id,
            'person_mlbam':person_mlbam,
            'person_name':person_name,
            'to_team_mlbam':to_team_mlbam,
            'to_team_name':to_team_name,
            'from_team_mlbam':from_team_mlbam,
            'from_team_name':from_team_name,
            'date':date_obj,
            'effective_date':effective_date,
            'type_code':type_code,
            'type_desc':type_desc,
            'description':description,
        })
        
    return data


# ====================================================
def _parse_person(_obj: dict):
    d = _obj
    pos = d.get("primaryPosition", {})
    bats = d.get("batSide", {})
    throws = d.get("pitchHand", {})

    zoneTop = d.get("strikeZoneTop", None)
    zoneBottom = d.get("strikeZoneBottom", None)
    primaryNumber = d.get("primaryNumber", 0)
    data = {
        "mlbam": d.get("id", 0),
        "name_full": d.get("fullName", "-"),
        "name_first": d.get("firstName", "-"),
        "name_middle": d.get("middleName", "-"),
        "name_last": d.get("lastName", "-"),
        "name_use": d.get("useName", "-"),
        "name_boxscore": d.get("boxscoreName", "-"),
        "name_slug": d.get("nameSlug", "-"),
        "name_first_last": d.get("firstLastName", "-"),
        "name_last_first": d.get("lastFirstName", "-"),
        "name_last_init": d.get("lastInitName", "-"),
        "name_init_last": d.get("initLastName", "-"),
        "name_fml": d.get("fullFMLName", "-"),
        "name_lfm": d.get("fullLFMName", "-"),
        "name_pronunciation": d.get("pronunciation", "-"),
        "pronunciation": d.get("pronunciation", "-"),
        "current_age": d.get("currentAge", 0),
        "birth_date": d.get("birthDate", "-"),
        "birth_city": d.get("birthCity", "-"),
        "birth_state": d.get("birthStateProvince", "-"),
        "birth_country": d.get("birthCountry", "-"),
        "death_date": d.get("deathDate", "-"),
        "death_city": d.get("deathCity", "-"),
        "death_state": d.get("deathStateProvince", "-"),
        "death_country": d.get("deathCountry", "-"),
        "active": d.get("active", False),
        "height": d.get("height", "-"),
        "weight": d.get("weight", "-"),
        "bats_code": bats.get("code", "-"),
        "bats_desc": bats.get("description", "-"),
        "throws_code": throws.get("code", "-"),
        "throws_desc": throws.get("description", "-"),
        "primary_number": primaryNumber,
        "jersey": primaryNumber,
        "gender": d.get("gender", "-"),
        "is_player": d.get("isPlayer", False),
        "is_verified": d.get("isVerified"),
        "mlb_debut": d.get("mlbDebutDate", "-"),
        "last_played": d.get("lastPlayedDate", "-"),
        "strikezone_top": zoneTop,
        "strikezone_bot": zoneBottom,
        "zone_top": zoneTop,
        "zone_bottom": zoneBottom,
        "draft_year": d.get("draftYear"),
        "pos_code": pos.get("code", "-"),
        "pos_name": pos.get("name", "-"),
        "pos_type": pos.get("type", "-"),
        "pos_abbreviation": pos.get("abbreviation", "-"),
    }
    return data

def _parse_team(_obj: dict):
    d = _obj
    spring_league = d.get("springLeague", {})
    spring_venue = d.get("springVenue", {})
    lg = d.get("league", {})
    div = d.get("division", {})
    venue = d.get("venue", {})

    data = {
        "mlbam": d.get("id", 0),
        "name_full": d.get("name", "-"),
        "team_code": d.get("teamCode", 0),
        "file_code": d.get("fileCode", 0),
        "abbreviation": d.get("abbreviation", "-"),
        "name_team": d.get("teamName", "-"),
        "name_location": d.get("locationName", "-"),
        "name_short": d.get("shortName", "-"),
        "name_franchise": d.get("franchiseName", "-"),
        "name_club": d.get("clubName", "-"),
        "season": d.get("season", 0),
        "first_year": d.get("firstYearOfPlay", "0"),
        "active": d.get("active", None),
        "all_start_status": d.get("allStarStatus"),
        "lg_mlbam": lg.get("id", 0),
        "lg_full": lg.get("name", "-"),
        "lg_abbrv": lg.get("abbreviation", "-"),
        "lg_abbreviation": lg.get("abbreviation", "-"),
        "lg_short": lg.get("nameShort", "-"),
        "div_mlbam": div.get("id", 0),
        "div_full": div.get("name", "-"),
        "div_abbrv": div.get("abbreviation", "-"),
        "div_abbreviation": div.get("abbreviation", "-"),
        "div_short": div.get("nameShort", "-"),
        "ven_mlbam": venue.get("id", 0),
        "ven_name": venue.get("name", "-"),
        "sp_ven_mlbam": spring_venue.get("id", 0),
        "sp_ven_name": spring_venue.get("name", "-"),
        "sp_lg_mlbam": spring_league.get("id", 0),
        "sp_lg_name": spring_league.get("name", "-"),
        "sp_lg_abbrv": spring_league.get("abbreviation", "-"),
    }

    return data

# def add_league_attr(row:pd.Series,attr:str):
#     teams = TEAMS[TEAMS['season']==int(row['season'])]
#     tmrow = teams[teams['mlbam']==row['team_mlbam']].iloc[0]
#     lgrow = LEAGUES[LEAGUES['mlbam']==tmrow['lg_mlbam']].iloc[0]
#     return lgrow[attr]

# def add_league_short(row:pd.Series):
#     teams = TEAMS[TEAMS['season']==int(row['season'])]
#     tmrow = teams[teams['mlbam']==row['team_mlbam']].iloc[0]
#     return tmrow.lg_abbrv

# def add_division_short(row:pd.Series):
#     teams = TEAMS[TEAMS['season']==int(row['season'])]
#     tmrow = teams[teams['mlbam']==row['team_mlbam']].iloc[0]
#     div_mlbam: Union[str,int] = tmrow['div_mlbam']
#     return league_ref[div_mlbam].short

# def add_division_mlbam(row:pd.Series):
#     teams = TEAMS[TEAMS['season']==int(row['season'])]
#     tmrow = teams[teams['mlbam']==row['team_mlbam']].iloc[0]
#     div_mlbam: Union[str,int] = tmrow['div_mlbam']
#     return div_mlbam

# def add_team_attr(row:pd.DataFrame,attr:str,season=None):
#     teams = TEAMS[TEAMS['season']==int(row['season'])]
#     tmrow = teams[teams['mlbam']==row['team_mlbam']].iloc[0]
#     return tmrow[attr]
