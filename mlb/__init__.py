"""
# SimpleStatsMLB

Author: Joe Rechenmacher


Classes
-------
- Player
- Team
- Franchise
- League
- Game

Functions
---------
- play_search
- pitch_search
- game_search
- schedule_search
- get_matchup_stats
- leaderboards
- last_game
- next_game
- find_team
- find_player
- find_venue
- schedule
- player_hitting
- player_pitching
- player_fielding
- player_hitting_advanced
- player_pitching_advanced
- team_hitting
- team_pitching
- team_fielding
- team_hitting_advanced
- team_pitching_advanced
- team_roster
- league_hitting
- league_pitching
- league_fielding
- league_hitting_advanced
- league_pitching_advanced

Data
----
- bios
- teams
- people
- venues
- seasons
- franchise
- standings
- yby_records
- hall_of_fame
- broadcasts

"""
from .classes import player
from .classes import Player
from .classes import Team
from .classes import franchise
from .classes import League
from .classes import Game

from .functions import play_search
from .functions import pitch_search
from .functions import game_search
from .functions import schedule_search
from .functions import get_matchup_stats
from .functions import leaderboards
from .functions import last_game
from .functions import next_game
from .functions import find_team
from .functions import find_player
from .functions import find_venue
from .functions import players_by_year
from .functions import schedule
from .functions import player_bio
from .functions import player_stats
from .functions import player_hitting
from .functions import player_pitching
from .functions import player_fielding
from .functions import player_hitting_advanced
from .functions import player_pitching_advanced
from .functions import player_game_logs
from .functions import player_date_range
from .functions import player_date_range_advanced
from .functions import player_splits
from .functions import player_splits_advanced
from .functions import player_data
from .functions import team_hitting
from .functions import team_pitching
from .functions import team_fielding
from .functions import team_hitting_advanced
from .functions import team_pitching_advanced
from .functions import team_roster
from .functions import team_game_logs
from .functions import franchise_data
from .functions import team_appearances
from .functions import league_hitting
from .functions import league_pitching
from .functions import league_fielding
from .functions import league_hitting_advanced
from .functions import league_pitching_advanced
from .functions import league_leaders

player_batting = player_hitting
team_batting = team_hitting

from .functions import game_highlights
from .functions import get_video_link

from .constants import (
    STATDICT,
    COLS_HIT,
    COLS_HIT_ADV,
    COLS_PIT,
    COLS_PIT_ADV,
    COLS_FLD
)


from .utils import default_season
from .utils import timeutils
# from .utils import utc_zone
# from .utils import et_zone
# from .utils import ct_zone
# from .utils import iso_format
# from .utils import maketable
# from .utils import showtable

# from .utils import compile_codes
# from .utils import draw_pitches
# from .utils import draw_strikezone
# from .utils import simplify_time
# from .utils import prettify_time
# from .utils import prepare_game_data
# from .utils import curr_year as current_year

from .mlbdata import get
from .mlbdata import save_all
from .mlbdata import save_bios
from .mlbdata import save_teams
from .mlbdata import save_venues
from .mlbdata import save_people
from .mlbdata import save_seasons
from .mlbdata import save_standings
from .mlbdata import save_yby_records
from .mlbdata import get_season_info
from .mlbdata import get_bios_df as bios
from .mlbdata import get_teams_df as teams
from .mlbdata import get_people_df as people
from .mlbdata import get_venues_df as venues
from .mlbdata import get_leages_df as leagues
from .mlbdata import get_seasons_df as seasons
from .mlbdata import get_standings_df as standings
from .mlbdata import get_yby_records as yby_records
from .mlbdata import get_hall_of_fame as hall_of_fame
from .mlbdata import get_broadcasts_df as broadcasts
from .mlbdata import get_bbref_hitting_war_df as bbref_war_hit
from .mlbdata import get_bbref_pitching_war_df as bbref_war_pitch

from .updatedb import update_hof
from .updatedb import update_bios
from .updatedb import update_people
from .updatedb import update_venues
from .updatedb import update_seasons
from .updatedb import update_leagues
from .updatedb import update_yby_records
from .updatedb import update_bbref_hitting_war
from .updatedb import update_bbref_pitching_war
update_bbref_batting_war = update_bbref_hitting_war


from .transactions import draft as get_draft
from .transactions import prospects as get_prospects
from .transactions import free_agents as get_free_agents
from .transactions import transactions as get_transactions

from .utils_team import team_leaders

from .async_mlb import fetch
from .async_mlb import fetch_text





def demo():
    urls = []
    years = range(1901,2022)
    for year in years:
        urls.append(f"https://statsapi.mlb.com/api/v1/teams/145?hydrate=standings&season={year}")
    return fetch(urls)


sit_codes = "h,a,d,n,g,t,3,4,5,6,7,8,9,10,11,l,r,vl,vr,sah,sbh,sti,twn,tls,taw,tal,b1,b2,b3,b4,b5,b6,b7,b8,b9,lo,i01,i02,i03,i04,i05,i06,i07,i08,i09,ix,e,r0,r1,r2,r3,r12,r23,r123,ron,ron2,risp,risp2,o0,o1,o2,fp,ac,bc,ec,2s,fc,c00,c01,c02,c10,c11,c12,c20,c21,c22,c30,c31,c32"
urls = [
    ""
]
