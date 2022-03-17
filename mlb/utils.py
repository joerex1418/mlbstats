import requests
import pandas as pd
import datetime as dt
from dateutil import tz
# from tabulate import tabulate

from .constants import ZONE_TOP_STANDARD
from .constants import ZONE_BOTTOM_STANDARD

from .constants import (
    BAT_FIELDS,
    # BAT_FIELDS_ADV,
    # PITCH_FIELDS,
    # PITCH_FIELDS_ADV,
    # FIELD_FIELDS,
    STATDICT,
    # LEAGUE_IDS,
    COLS_HIT,
    COLS_HIT_ADV,
    COLS_PIT,
    COLS_PIT_ADV,
    COLS_FLD,
    W_SEASON,
    WO_SEASON
)

from .mlbdata import get_season_info

today_date = dt.datetime.today()

utc_zone = tz.tzutc()
et_zone = tz.gettz("America/New_York")
ct_zone = tz.gettz("America/Chicago")
mt_zone = tz.gettz("America/Denver")
pt_zone = tz.gettz("America/Los_Angeles")

standard_format = r"%-I:%M %p"
military_format = r"%H:%M"
iso_format = r"%Y-%m-%dT%H:%M:%SZ"
iso_format_ms = r"%Y-%m-%dT%H:%M:%S.%fZ"

curr_year = today_date.year
curr_date = today_date

def make_dt_obj(_dt_str:str,_date_only=False):
    if _date_only is False:
        try:
            return dt.datetime.strptime(_dt_str,r"%Y-%m-%d")
        except:
            return dt.datetime.strptime(_dt_str,r"%m/%d/%Y")
    else:
        try:
            return dt.datetime.strptime(_dt_str,r"%Y-%m-%d").date()
        except:
            return dt.datetime.strptime(_dt_str,r"%m/%d/%Y").date()

def default_season() -> int:
    season_info = get_season_info()
    if season_info['in_progress'] is None:
        season = season_info['last_completed']
    else:
        season = season_info['in_progress']
    return season

def compile_codes(*code_lists,output_list=False) -> str:
    all_codes = []
    for code_list in code_lists:
        all_codes += code_list
    if output_list is False:
        return ",".join(all_codes)
    return all_codes

def draw_strikezone(matchup):
    player_zoneTop = matchup[0]["zoneTop"]
    player_zoneBottom = matchup[0]["zoneBottom"]
    bat_side = matchup[0]["bat_side"]
    batter = matchup[0]["batter"]
    batter_mlbam = matchup[0]["batter_mlbam"]

    pitches = []
    pitch_info = []
    for i in matchup:
        pitch_info.append((i["pX"],i["pZ"],i["pitch_code"]))
        pitches.append({
            "count":i["count"],
            "call":i["call"],
            "pitchType":i["pitch_type"],
            "releaseVelocity":i["release_velocity"],
            "endVelocity":i["end_velocity"],
            "spinRate":i["spin_rate"],
            "zone":i["zone"],
            "zoneTop":i["batter_zoneTop"],
            "zoneBottom":i["batter_zoneBottom"],
            "distance":i["distance"]
        })

    zoneTop = ZONE_TOP_STANDARD * 12
    zoneBot = ZONE_BOTTOM_STANDARD * 12

    if bat_side == "R":
        batter_img = "righty"
        img_pos = ""
    elif bat_side == "L":
        batter_img = "lefty"
        img_pos = "345"
    
    sZone_h = zoneTop - zoneBot
    zBox_h = sZone_h/3
    zBox_h_x2 = zBox_h*2

    circle_radius = 1.2
    
    drawn_circles = []
    for idx,tup in enumerate(pitch_info):
        pX = tup[0]
        pZ = tup[1]
        try: # TRY/EXCEPT because sometimes pX and/or pZ are not identified
            x = (pX*12) + 8.5
            y = zoneTop - (pZ*12)
            x = round(x,2)
            y = round(y,2)
            pitchCode = tup[2].lower()
            circle_html = f"""
                    <circle id="p{idx+1}" class="{pitchCode}" cx="{x}" cy="{y}" r="{circle_radius}" />
                    <text x={x} y={y} text-anchor="middle" style="fill:white;font-size:1.8px;" transform="translate(0 0.65)">{idx+1}</text>
                    """
            drawn_circles.append(circle_html)
        except:
            pass
    
    circles = "\n".join(drawn_circles)
    
    # <div style="border: 2px solid rgb(0, 0, 0);width:max-content; height:fit-content;">

    batters_box_html = f"""
            <div class="zone_wrapper" style="transform:scale(1.000)">
                <svg class="at_bat" width="455" height="500" viewBox="" style="transform: scale(0.9);">
                <image xlink:href="/static/misc/{batter_img}.svg" x="{img_pos}" y="10" height="100%" width="160" style="transform:scale(0.9);"/>
                <g transform="scale(6.5) translate(26.5 25)">
                    <!-- ZONES 1,2,3 -->
                    <rect x="" y="" width="5.667" height="{zBox_h}" style="fill:#f8f4d2"/>
                    <rect x="5.667" y="" width="5.667" height="{zBox_h}" style="fill:#f8f4d2"/>
                    <rect x="11.333" y="" width="5.667" height="{zBox_h}" style="fill:#f8f4d2"/>
                
                    <!-- ZONES 4,5,6 -->
                    <rect x="" y="{zBox_h}" width="5.667" height="{zBox_h}" style="fill:#f8f4d2"/>
                    <rect x="5.667" y="{zBox_h}" width="5.667" height="{zBox_h}" style="fill:#f8f4d2"/>
                    <rect x="11.333" y="{zBox_h}" width="5.667" height="{zBox_h}" style="fill:#f8f4d2"/>
                
                    <!-- ZONES 7,8,9 -->
                    <rect x="" y="{zBox_h_x2}" width="5.667" height="{zBox_h}" style="fill:#f8f4d2"/>
                    <rect x="5.667" y="{zBox_h_x2}" width="5.667" height="{zBox_h}" style="fill:#f8f4d2"/>
                    <rect x="11.333" y="{zBox_h_x2}" width="5.667" height="{zBox_h}" style="fill:#f8f4d2"/>
                
                    <!-- PARENT ZONE -->
                    <rect x="" y="" width="17" height="{sZone_h}" style="fill:none;stroke:black;stroke-width:0.1"/>
                        <line x1="5.667" y1="0"
                            x2="5.667" y2="{sZone_h}"
                            stroke="black"
                            stroke-width="0.1"/>
                        <line x1="11.333" y1="0"
                            x2="11.333" y2="{sZone_h}"
                            stroke="black"
                            stroke-width="0.1"/>
                
                        <line x1="0" y1="{zBox_h}"
                            x2="17" y2="{zBox_h}"
                            stroke="black"
                            stroke-width="0.1"/>
                        <line x1="0" y1="{zBox_h_x2}"
                            x2="17" y2="{zBox_h_x2}"
                            stroke="black"
                            stroke-width="0.1"/>
                        {circles}
                </g>
                </svg>
            </div>
        """
    return batters_box_html

def draw_pitches(pCoordinates:list,zoneTop,pitchCodes=[],ab=""):
    # still need to determine correct 'zoneTop' and 'zoneBottom' values since they may be different for each pitch
    
    c_radius = 1.2
    
    drawn_circles = []
    # pitchTypes = ""
    for idx,tup in enumerate(pCoordinates):
        pX = tup[0]
        pZ = tup[1]
        x = (pX*12) + 8.5
        y = (zoneTop*12) - (pZ*12)
        x = round(x,2)
        y = round(y,2)
        try:
            pitchCode = pitchCodes[idx]
        except:
            pitchCode = "unknown"
        circle_html = f"""
                <circle id="p{idx+1}" class="{pitchCode}" cx="{x}" cy="{y}" r="{c_radius}" />
                <text x={x} y={y} text-anchor="middle" style="fill:white;font-size:1.8px;" transform="translate(0 0.65)">{idx+1}</text>
                """

        drawn_circles.append(circle_html)
    
    return "\n".join(drawn_circles)

def simplify_time(iso_time) -> str:
    """Converts ISO formatted datetime string to a timestamp
    
    `2021-11-03T00:34:53Z` -> `20211003_003453`
    """
    # 20211003_003453
    t = iso_time
    date = t[0:10].replace("-","")
    time = t[11:19].replace(":","")
    return f"{date}_{time}"

def prettify_time(t=dt.datetime.strftime(dt.datetime.utcnow(),iso_format),fmt="twelve",tz="ct",show_output=False) -> str:
    """Converts Timestamp or ISO Format to standard 12-Hour or 24-Hour format

    (Timestamp: `20211003_083453`)
    (ISO Format `2021-11-03T08:34:53Z`)

    - fmt: "twelve" or "military"
    -------
    Accepted 'tz' values:
    - Central Time -> 'ct'
    - Easterb Time -> 'et'
    - UTC Time     -> 'utc'
    """
    if type(t) is dt.timedelta:
        t = str(t)
        # start = t.find(":")+1
        start = 0
        end = t.rfind(".")
        pt = t[start:end]
        return pt
    elif type(t) is dt.datetime:
        t = dt.datetime.strftime(t,iso_format)

    raw = t
    if "_" in t:
        if show_output is True:
            print("Timestamp detected")
        stamp = t
        datepart = t[0:8]
        timepart = t[-6:]
        date_utc_part = f"{datepart[0:4]}-{datepart[4:6]}-{datepart[6:8]}"
        time_utc_part = f"T{timepart[0:2]}:{timepart[2:4]}:{timepart[-2:]}Z"
        t = date_utc_part + time_utc_part
        if show_output is True:
            print(f"Converting to UTC format -> {t}")
    else:
        if show_output is True:
            print("UTC format detected")
            stamp = simplify_time(t)
            print(f"Timestamp format: {stamp}")
    try:
        dt_obj = dt.datetime.strptime(t,iso_format)
    except:
        try:
            dt_obj = dt.datetime.strptime(t,iso_format_ms)
        except:
            # dt_obj = dt.datetime.strftime(dt.datetime.utcnow(),iso_format)
            dt_obj = dt.datetime.utcnow()

    utc_time_obj = dt_obj.replace(tzinfo=utc_zone) # tells the date object that it should be UTC

    utc_time_obj = utc_time_obj
    utc_time_iso = dt.datetime.strftime(utc_time_obj,iso_format)
    utc_time_stmp = simplify_time(utc_time_iso)
    utc_time_12 = dt.datetime.strftime(utc_time_obj,standard_format)
    utc_time_24 = dt.datetime.strftime(utc_time_obj,military_format)

    ct_time_obj = utc_time_obj.astimezone(ct_zone)
    ct_time_iso = dt.datetime.strftime(ct_time_obj,iso_format)
    ct_time_stmp = simplify_time(ct_time_iso)
    ct_time_12 = dt.datetime.strftime(ct_time_obj,standard_format)
    ct_time_24 = dt.datetime.strftime(ct_time_obj,military_format)

    et_time_obj = utc_time_obj.astimezone(et_zone)
    et_time_iso = dt.datetime.strftime(et_time_obj,iso_format)
    et_time_stmp = simplify_time(et_time_iso)
    et_time_12 = dt.datetime.strftime(et_time_obj,standard_format)
    et_time_24 = dt.datetime.strftime(et_time_obj,military_format)


    shitty_table = """
    \tUSER INPUT: {}


    ZONE     ISO Format\t\t    Stamp\t\t12-Hour\t\t24-Hour
    ------------------------------------------------------------------------
        UTC: {:<21}  {:<14}\t{:<8}\t{:<8}
    Central: {:<21}  {:<14}\t{:<8}\t{:<8}
    Eastern: {:<21}  {:<14}\t{:<8}\t{:<8}
    
    """
    if show_output is True:
        print(shitty_table.format(
            raw,
            utc_time_iso,
            utc_time_stmp,
            utc_time_12,
            utc_time_24,

            ct_time_iso,
            ct_time_stmp,
            ct_time_12,
            ct_time_24,
            
            et_time_iso,
            et_time_stmp,
            et_time_12,
            et_time_24))
    
    if tz == "ct":
        if fmt == "twelve":
            return ct_time_12
        elif fmt == "military":
            return ct_time_24
    elif tz == "et":
        if fmt == "twelve":
            return et_time_12
        elif fmt == "military":
            return et_time_24
    elif tz == "utc":
        if fmt == "twelve":
            return utc_time_12
        elif fmt == "military":
            return utc_time_24

def prepare_game_data(gm,**params):
    matchup_df = gm.matchup_event_log()
    pas_df = gm.pa_results_log()
    # scoring_pas_df = gm.scoring_pa_log()
    events_df = gm.game_event_log()
    scoring_events_df = gm.scoring_event_log()
    linescore = gm.linescore() 
    boxscore = gm.boxscore()

    players = gm.players_

    away_batting = gm.away_batting_stats()
    away_pitching = gm.away_pitching_stats()
    away_fielding = gm.away_fielding_stats()
    home_batting = gm.home_batting_stats()
    home_pitching = gm.home_pitching_stats()
    home_fielding = gm.home_fielding_stats()

    data_dict = {
        "linescore":linescore,
        "boxscore":boxscore,
        "currentMatchupLog":matchup_df.to_dict("records"),          # Events for the current matchup
        "atBats":pas_df.to_dict("records"),                         # Information for each plate appearance
        # "atBatsScoring":scoring_pas_df.to_dict("records"),          # Information for each scoring plate appearance 
        "events":events_df.to_dict("records"),                      # All game events (pitches, actions, player change, etc)
        "eventsByAb":[],                                            # All game events grouped by plate appearance
        "eventsScoring":scoring_events_df.to_dict("records"),       # All events where PA resulted in run(s) scored
        "ref":gm.all_plays,                                         # Raw data from Game instance's "all_plays" attribute
        "venue":gm.venue(),
        "players":players,
        "strikezones":[]
    }
    # 2021-10-03T00:34:53.888Z  - example timestamp
    # https://statsapi.mlb.com/api/v1.1/game/632265/feed/live/timestamps (all game timestamps)

    for idx in range(len(pas_df)):
        pa_events_df = events_df[events_df["pa"]==idx+1]
        pa_events_df = pa_events_df[(pa_events_df["category"] == "pitch") | (pa_events_df["category"] == "atBat")]
        pa_events_df.sort_values(by="pitch_num",ascending=True,inplace=True)
        data_dict["eventsByAb"].append(pa_events_df.to_dict("records"))

    for ab in data_dict["eventsByAb"]: # Iterates through each PA (and each PA's pitch/event)
        strikezone = draw_strikezone(ab) # Draws SVG strikezone for each PA
        data_dict["strikezones"].append(strikezone)

    data_dict["stats"] = {
        "away":{
            "batting":away_batting.to_dict("records"),
            "pitching":away_pitching.to_dict("records"),
            "fielding":away_fielding.to_dict("records")
        },
        "home":{
            "batting":home_batting.to_dict("records"),
            "pitching":home_pitching.to_dict("records"),
            "fielding":home_fielding.to_dict("records")
        }
    }

    
    
   # ######### Code may or may not be for testing purposes ###################################################
    # if params["at_bat"] is None:
    #     pass
    # else:
    #     matchup_events = events_df[events_df["pa"] == int(params["at_bat"])]
    #     zoneTopInitial = matchup_events.iloc[0].zoneTop
    #     zoneBottomInitial = matchup_events.iloc[0].zoneBottom
    #     data_dict["selectedMatchup"] = {
    #         "zoneTopInitial":zoneTopInitial,
    #         "zoneBottomInitial":zoneBottomInitial,
    #         "events":matchup_events.to_dict("records")
    #     }
   # #########################################################################################################

    return data_dict

def game_str_display(game_obj):
    to_print = []
    gm = game_obj
    bat_cols = ["Player","Pos","AVG","AB","H","R","SO","BB"]
    hm = gm.home_batting_stats()
    aw = gm.away_batting_stats()
    max_batters = len(hm) if len(hm) > len(aw) else len(aw)
    matchup = gm.matchup_info()

    outs = gm.outs
    balls = gm.balls
    strikes = gm.strikes

    ol  = "\u203e"
    vl  = "\u2503"
    ld  = "\u2571"
    rd  = "\u2572"
    x   = "\u2573"
    dot = "\u25C9 "
    cir = "\u25cc "

    top_border = f'|{ol*143}|'
    bot_border = f'|{"_"*143}|'

    blank_row = "|{:143}|".format("")                   # blank row
    blank_row_sep = "|{:71}|{:71}|".format("","")       # blank row with VERTICAL separator

    dyn_row_mid   = "|{:^143.143}|"
    dyn_row_left  = "|{:<143.143}|"
    dyn_row_right = "|{:>143.143}|"

    midalign_col   = "{:^66.66}"
    leftalign_col  = "{:<66.66}"
    rightalign_col = "{:>66.66}"

    ls = gm.linescore()

    ls_tot_fmt = " {:^3}"
    ls_fmt = "{:^3}"
    head_ls = ["     ","   "]
    away_ls = ["     ",ls_fmt.format(gm.away_abbrv)]
    home_ls = ["     ",ls_fmt.format(gm.home_abbrv)]
    for inn in ls['innings']:
        inning_ord = inn["inningOrdinal"]
        head_ls.append(ls_fmt.format(inning_ord))

        aw_runs = str(inn.get("away",{}).get("runs","-"))
        away_ls.append(ls_fmt.format(aw_runs))

        hm_runs = str(inn.get("home",{}).get("runs","-"))
        home_ls.append(ls_fmt.format(hm_runs))

    aw_runs_total = str(ls["total"]["away"]["runs"])
    aw_hits_total = str(ls["total"]["away"]["hits"])
    aw_errs_total = str(ls["total"]["away"]["errors"])
    hm_runs_total = str(ls["total"]["home"]["runs"])
    hm_hits_total = str(ls["total"]["home"]["hits"])
    hm_errs_total = str(ls["total"]["home"]["errors"])


    head_ls.append(f'|{ls_tot_fmt.format("R")}{ls_tot_fmt.format("H")}{ls_tot_fmt.format("E")}')

    away_ls.append(f'|{ls_tot_fmt.format(aw_runs_total)}{ls_tot_fmt.format(aw_hits_total)}{ls_tot_fmt.format(aw_errs_total)}')
    home_ls.append(f'|{ls_tot_fmt.format(hm_runs_total)}{ls_tot_fmt.format(hm_hits_total)}{ls_tot_fmt.format(hm_errs_total)}')

    # some_txt = f'{"x"*40} sda;dfj {"8"*80} asd;lfkjasldfjalsdfjas;dlfkj{"Q"*90}'
    l_txt = "l"*20
    r_txt = "r"*20
    # to_print.append("|     {:<66.61}     {:<61.62}     |".format(l_txt,r_txt)) 

    ls_head_row = " ".join(head_ls)
    ls_away_row = " ".join(away_ls)
    ls_home_row = " ".join(home_ls)

    team_name_row = "|      {:60}     |      {:60}     |"
    bat_row  = "|     {:4} {:23.23}   {:>4}{:>6.4}{:>4}{:>4}{:>4}{:>4}{:>4}     |     {:4} {:23.23}   {:>4}{:>6.4}{:>4}{:>4}{:>4}{:>4}{:>4}     |"
    bat_header_str = ("#","Player Name","Pos","AVG","AB","H","R","SO","BB","#","Player Name","Pos","AVG","AB","H","R","SO","BB")
    bat_header_ul = "\u203e"*61

    to_print.append(top_border) # TOP =============================================================== TOP
    title = f"{gm.away_full}  vs  {gm.home_full}"
    to_print.append("|{:^143}|".format(title))
    to_print.append("|{:^143}|".format('-'*(len(title)+2)))
    
    gm_date = gm.game_date
    gm_date_obj = dt.datetime.strptime(gm_date,r"%Y-%m-%d")
    gm_date_str = gm_date_obj.strftime(r"%A, %B ") + f'{int(gm_date_obj.day)} ' + gm_date_obj.strftime('%Y')
    to_print.append("|{:^143}|".format(gm_date_str))
    to_print.append(blank_row)
    # -------------------------- GAME INFO --------------------------
    venue = "{:<10} {:25}".format("Venue:",gm.venue()['name'])
    venue = leftalign_col.format(venue)
    ls_header = rightalign_col.format(ls_head_row)

    weather = "{:<10} {:25}".format("Weather:",gm.sky)
    weather = leftalign_col.format(weather)
    ls_away = rightalign_col.format(ls_away_row)

    wind = "{:<10} {:25}".format("Wind:",gm.wind)
    wind = leftalign_col.format(wind)
    ls_home = rightalign_col.format(ls_home_row)

    to_print.append("|     {:<66.66} {:>66.66}     |".format(venue,ls_header))
    to_print.append("|     {:<66.66} {:>66.66}     |".format(weather,ls_away))
    to_print.append("|     {:<66.66} {:>66.66}     |".format(wind,ls_home))


    to_print.append("|{:_^143}|".format("_"))
    to_print.append(blank_row)
    # --------------------------------------------------------------------- #


    # -------------------------- CURRENT MATCHUP -------------------------- #
    # to_print.append("|     {:<133.133}     |".format("Current MATCHUP"))
    now_batting = matchup.get("atBat",{})
    now_pitching = matchup.get("pitching",{})

    now_batting_id = now_batting.get("id","-")
    now_batting_name = now_batting.get("name","-")

    now_pitching_id = now_pitching.get("id","-")
    now_pitching_name = now_pitching.get("name","-")

    ball_ct = str(gm.balls)
    strike_ct = str(gm.strikes)
    out_ct = str(gm.outs)

    situation    = gm.situation()
    runnersOn    = situation["runnersOn"]
    queue        = situation["queue"]
    on_deck      = queue.get("onDeck",{})
    in_hole      = queue.get("inHole",{})
    on_deck_name = on_deck.get("name","-")
    in_hole_name = in_hole.get("name","-")

    onFirst     = x if runnersOn.get("first",{}).get("isOccuppied") is True else " "
    onSecond    = x if runnersOn.get("second",{}).get("isOccuppied") is True else " "
    onThird     = x if runnersOn.get("third",{}).get("isOccuppied") is True else " "

    ball_ct     = int(ball_ct)
    strike_ct   = int(strike_ct)
    out_ct      = int(out_ct)

    b_rep = dot*ball_ct
    b_emp = cir*(4 - ball_ct)
    b_rep = b_rep + b_emp
    
    s_rep = dot*strike_ct
    s_emp = cir*(3 - strike_ct)
    s_rep = s_rep + s_emp

    o_rep = dot*out_ct
    o_emp = cir*(3 - out_ct)
    o_rep = o_rep + o_emp

    diamond1 = f"     ┌───┐"
    diamond2 = f"     │ {onSecond} │"
    diamond3 = f"     └───┘"
    diamond4 = f"┌───┐     ┌───┐"
    diamond5 = f"│ {onThird} │     │ {onFirst} │"
    diamond6 = f"└───┘     └───┘"

    # print(f"{c.YELLOW}AT BAT{c.END}")

    mid_sec_r01 = "{:<45.45}     {:>89}     |".format("|     {:<66}".format(diamond1),"{:<60}".format(""))

    mid_sec_r02 = "{:<45.45}     {:>89}     |".format("|     {:<66}".format(diamond2),"{:<60}".format(""))

    mid_sec_r03 = "{:<45.45}     {:<89}     |".format("|     {:<25}{:<2}{:<15.15}".format(diamond3,"B",b_rep),"{:<11.11}{:<23.23}{:<11.11}{:<25.25}".format("AT BAT:",now_batting_name,"ON DECK:",on_deck_name))

    mid_sec_r04 = "{:<45.45}     {:<89}     |".format("|     {:<25}{:<2}{:<15.15}".format(diamond4,"S",s_rep),"{:<11.11}{:<23.23}{:<11.11}{:<25.25}".format("ON MOUND:",now_pitching_name,"HOLE",in_hole_name))

    mid_sec_r05 = "{:<45.45}     {:<89}     |".format("|     {:<25}{:<2}{:<15.15}".format(diamond5,"O",o_rep),"{:<70.70}".format(""))

    mid_sec_r06 = "{:<45.45}     {:>89}     |".format("|     {:<66}".format(diamond6),"{:<60}".format(""))

    to_print.append(mid_sec_r01)
    to_print.append(mid_sec_r02)
    to_print.append(mid_sec_r03)
    to_print.append(mid_sec_r04)
    to_print.append(mid_sec_r05)
    to_print.append(mid_sec_r06)

    to_print.append("|{:_^143}|".format("_"))
    to_print.append(blank_row)

    # --------------------------------------------------------------------- #

    to_print.append(blank_row)

    to_print.append(team_name_row.format(gm.away_full,gm.home_full))

    to_print.append(blank_row_sep)
    to_print.append(blank_row_sep)

    bat_head = bat_row.format("#","Player Name","Pos","AVG","AB","H","R","SO","BB","#","Player Name","Pos","AVG","AB","H","R","SO","BB")
    to_print.append(bat_head)
    to_print.append(f"|     {bat_header_ul}     |     {bat_header_ul}     |")
    empty_row = {"Player":"","Pos":"","AVG":"","AB":"","H":"","R":"","SO":"","BB":""}

    aw_ct = 1
    hm_ct = 1
    for idx in range(max_batters):
        try:
            aw_row = aw.iloc[idx].copy()
            if aw_row["Player"] != "Summary":
                if aw_row["substitute"] is True:
                    aw_row["Player"] = "  -- " + aw_row["Player"]
                    aw_idx = ""
                else:
                    aw_idx = f"{aw_ct}."
                    aw_ct += 1
            else:
                aw_idx = ""
        except:
            aw_row = empty_row
            aw_idx = ""
        
        try:
            hm_row = hm.iloc[idx].copy()
            if hm_row["Player"] != "Summary":
                if hm_row["substitute"] is True:
                    hm_row["Player"] = "  -- " + hm_row["Player"]
                    hm_idx = ""
                else:
                    hm_idx = f'{hm_ct}.'
                    hm_ct += 1
            else:
                hm_idx = ""
        except:
            hm_row = empty_row
            hm_idx = ""
        

        to_print.append(bat_row.format(
            aw_idx,aw_row["Player"],aw_row["Pos"],aw_row["AVG"],aw_row["AB"],aw_row["H"],aw_row["R"],aw_row["SO"],aw_row["BB"],
            hm_idx,hm_row["Player"],hm_row["Pos"],hm_row["AVG"],hm_row["AB"],hm_row["H"],hm_row["R"],hm_row["SO"],hm_row["BB"]
            ))

    to_print.append(blank_row)
    to_print.append(bot_border) # BOTTOM ============================================================ BOTTOM

    final_output = "\n".join(to_print)
    return final_output.replace("|",vl)

class timeutils:
    utc_zone = utc_zone
    et_zone = et_zone
    ct_zone = ct_zone
    mt_zone = mt_zone
    pt_zone = pt_zone
    
    class fmt:
        standard = standard_format
        military = military_format
        iso = iso_format
        isoms = iso_format_ms

class keys:
    stats = STATDICT
    hit = COLS_HIT
    hit_adv = COLS_HIT_ADV
    pitch = COLS_PIT
    pitch_adv = COLS_PIT_ADV
    field = COLS_FLD
    other_cols_with_season = W_SEASON
    other_cols_wo_season = WO_SEASON

class metadata:
    def __call__(self) -> list:
        meta_list = [
            'statGroups',
            'statTypes',
            'leagueLeaderTypes',
            'baseballStats'
        ]
        meta_list.sort()
        return meta_list

    def baseballStats(df=False) -> list[dict] | pd.DataFrame:
        url = "https://statsapi.mlb.com/api/v1/baseballStats"
        data = []
        resp = requests.get(url)
        if df is True:
            for d in resp.json():
                stat_groups = []
                for sg in d.get('statGroups',[{}]):
                    stat_groups.append(sg.get('displayName'))
                data.append([
                    d.get('name','-'),
                    d.get('lookupParam','-'),
                    d.get('isCounting','-'),
                    d.get('label','-'),
                    True if 'hitting' in stat_groups else False,
                    True if 'pitching' in stat_groups else False,
                    True if 'fielding' in stat_groups else False,
                    True if 'catching' in stat_groups else False,
                    True if 'game' in stat_groups else False,
                    d.get('orgTypes','-'),
                    d.get('highLowTypes','-'),
                    d.get('streakLevels','-'),
                ])
            return pd.DataFrame(data=data,columns=['name','lookupParam','isCounting','label','hitting','pitching','fielding','catching','game','orgTypes','highLowTypes','streakLevels'])
        else:
            return resp.json()

    def leagueLeaderTypes(df=False) -> list | pd.DataFrame:
        url = "https://statsapi.mlb.com/api/v1/leagueLeaderTypes"
        data = []
        resp = requests.get(url)
        for i in resp.json():
            data.append(i['displayName'])
        return data

    def statGroups(df=False) -> list | pd.DataFrame:
        url = "https://statsapi.mlb.com/api/v1/statGroups"
        data = []
        resp = requests.get(url)
        for i in resp.json():
            data.append(i['displayName'])
        if df is True:
            return pd.DataFrame(data=data)
        return data

    def statTypes(df=False) -> list | pd.DataFrame:
        url = "https://statsapi.mlb.com/api/v1/statTypes"
        data = []
        resp = requests.get(url)
        for i in resp.json():
            data.append(i['displayName'])
        return data



        




