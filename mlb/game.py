import requests
import datetime as dt
from dateutil.parser import parse
from typing import Union, Optional, Dict, List, Literal

import pandas as pd

from .functions import _team_data
from .functions import _franchise_data
from .functions import _player_data
from .functions import fetch as _fetch

from .helpers import mlb_date as md
from .helpers import mlb_datetime as mdt
from .helpers import mlb_wrapper
from .helpers import venue_name_wrapper
from .helpers import league_name_wrapper
from .helpers import team_name_data
from .helpers import person_name_data
from .helpers import standings_wrapper
from .helpers import roster_wrapper
from .helpers import edu_wrapper
from .helpers import player_stats
from .helpers import team_stats

from .helpers import get_tz
from .helpers import _teams_data_collection
from .helpers import _people_data_collection
from .helpers import _parse_team
from .helpers import _parse_league
from .helpers import _parse_division
from .helpers import _parse_person
from .helpers import _parse_venue
from .helpers import mlb_team
from .helpers import mlb_person

from .helpers import umpires

from .constants import BASE, POSITION_DICT, ORDINALS

from .utils import iso_format_ms
from .utils import utc_zone
from .utils import prettify_time
from .utils import default_season

class Game:
    """# Game

    MLB Game instance

    Paramaters
    ----------
    game_pk : int or str
        Unique primary key for specific game

    timecode : str
        specify a value to retrieve a "snapshot" of the game at a specific 
        point in time

        Format = "YYYYmmdd_HHMMDD"

    tz : str
        preferred timezone to view datetime values ("ct","et","mt", or "pt")

    Methods:
    --------

    boxscore() -> dict
        returns a python dictionary of boxscore information

    linescore() -> dict
        returns a python dictionary of the linescore in various formats

    situation() -> dict
        get a dict of information for the current situation

    diamond() -> dict
        returns a python dictionary detailing the current defensive lineup

    matchup_info() -> dict
        returns a python dictionary detailing information for the current 
        matchup

    matchup_event_log() -> DataFrame
        returns a dataframe of events that have taken place for the current 
        matchup/at-bat

    away_batting_stats() -> DataFrame
        returns a dataframe of game batting stats for the away team

    away_pitching_stats() -> DataFrame
        returns a dataframe of game pitching stats for the away team

    away_fielding_stats() -> DataFrame
        returns a dataframe of game fielding stats for the away team

    home_batting_stats() -> DataFrame
        returns a dataframe of game batting stats for the home team

    home_pitching_stats() -> DataFrame
        returns a dataframe of game pitching stats for the home team

    home_fielding_stats() -> DataFrame
        returns a dataframe of game fielding stats for the home team

    events() -> DataFrame
        returns a dataframe of every pitching event for every plate appearance 
        thus far

    plays() -> DataFrame
        returns a dataframe of all play (plate appearance) results

    get_content() -> dict
        returns a dictionary of broadcast information, highlight/recap video 
        urls, and editorial data

    flags() -> dict
        returns a dictionary of notable attributes about the game
    """

    def __init__(self,game_pk, timecode=None, tz='et'):
        self.last_updated = dt.datetime.now()
        if timecode == '':
            timecode = None
        if timecode is not None and timecode.find('_') == -1:
            timecode = parse(timecode).strftime(r'%Y%m%d_%H%M%S')
        
        tz_obj = get_tz(tz)
        self._tz = tz
        
        self.__game_pk = game_pk

        BASE = 'https://statsapi.mlb.com/api'
        
        game_url = f'{BASE}/v1.1/game/{game_pk}/feed/live?'
        params = {'hydrate':'venue,flags,preState',
                  'timecode':timecode}

        gm = requests.get(game_url,params=params).json()
        self._raw_game_data = gm

        self.meta = gm['metaData']
        gameData = gm['gameData']
        liveData = gm['liveData']

        # GAME Information
        self._linescore = liveData['linescore']
        self._boxscore = liveData['boxscore']
        self._flags = gameData['flags']

        self.abstractState  = gameData['status']['abstractGameState']
        self.abstract_state = self.abstractState
        self.detailedState  = gameData['status']['detailedState']
        self.detailed_state = self.detailedState
        self.gameState  = self.abstract_state
        self.game_state = self.abstract_state
        self.info = self._boxscore['info']
        self.sky = gameData['weather'].get('condition', '-')
        self.temp = gameData['weather'].get('temp', '-')
        self.wind = gameData['weather'].get('wind', '-')
        self.first_pitch = gameData.get('gameInfo', {}).get('firstPitch', '-')
        self.attendance = gameData.get('gameInfo', {}).get('attendance', '-')
        self.start = gameData.get('datetime', {}).get('time', '-')
        self.start_iso = gameData.get('datetime', {}).get('dateTime', '-')

        self._game_datetime = mdt(self.start_iso,tz=tz_obj)

        datetime = gameData['datetime']
        self.game_date = datetime['officialDate']
        self.gameDate = self.game_date
        self.daynight = datetime['dayNight']

        self._venue = gameData['venue']

        self._officials = self._boxscore.get('officials', [{}, {}, {}, {}])

        if len(self._officials) != 0:
            _ump_home = self._officials[0].get('official', {})
            _ump_first = self._officials[1].get('official', {})
            _ump_second = self._officials[2].get('official', {})
            _ump_third = self._officials[3].get('official', {})
        else:
            _ump_home = {}
            _ump_first = {}
            _ump_second = {}
            _ump_third = {}
        
        self._umpires = umpires(
            first  = _ump_first.get('fullName',''),
            second = _ump_second.get('fullName',''),
            third  = _ump_third.get('fullName',''),
            home   = _ump_home.get('fullName','')
            )

        # ALL PLAYERS IN GAME
        self._players = gameData['players']

        # AWAY Team Data
        away = gameData['teams']['away']
        _away_data = self._boxscore['teams']['away']
        _away_score_data = self._linescore['teams']['away']
        self.away_id = away['id']
        self._away_team_full = away['name']
        self._away_team = away['clubName']
        self._away_team_abbrv = away['abbreviation']
        self._away_stats = _away_data['teamStats']
        self._away_player_data = _away_data['players']
        self._away_lineup = _away_data['batters']
        self._away_starting_order = _away_data['battingOrder']
        self._away_pitcher_lineup = _away_data['pitchers']
        self._away_bullpen = _away_data['bullpen']
        self._away_rhe = self._linescore['teams']['away']
        self._away_bench = _away_data['bench']
        self.away_full = self._away_team_full
        self.away_club = self._away_team
        self.away_abbrv = self._away_team_abbrv
        self.away_runs = _away_score_data.get('runs')
        self.away_hits = _away_score_data.get('hits')
        self.away_errs = _away_score_data.get('errors')
        self.away_record = f"{away['record']['wins']}-{away['record']['losses']}"

        # HOME Team Data
        home = gameData['teams']['home']
        _home_data = self._boxscore['teams']['home']
        _home_score_data = self._linescore['teams']['home']
        self.home_id = home['id']
        self._home_team_full = home['name']
        self._home_team = home['clubName']
        self._home_team_abbrv = home['abbreviation']
        self._home_stats = _home_data['teamStats']
        self._home_player_data = _home_data['players']
        self._home_lineup = _home_data['batters']
        self._home_starting_order = _home_data['battingOrder']
        self._home_pitcher_lineup = _home_data['pitchers']
        self._home_bullpen = _home_data['bullpen']
        self._home_rhe = self._linescore['teams']['home']
        self._home_bench = _home_data['bench']
        self.home_full = self._home_team_full
        self.home_club = self._home_team
        self.home_abbrv = self._home_team_abbrv
        self.home_runs = _home_score_data.get('runs')
        self.home_hits = _home_score_data.get('hits')
        self.home_errs = _home_score_data.get('errors')
        self.home_record = f"{home['record']['wins']}-{home['record']['losses']}"

        self._curr_defense = self._linescore['defense']
        self._curr_offense = self._linescore['offense']
        self._curr_play = liveData['plays'].get('currentPlay', {})

        self.balls = self._linescore.get('balls', 0)
        self.strikes = self._linescore.get('strikes', 0)
        self.outs = self._linescore.get('outs', 0)
        self._inning = self._linescore.get('currentInning', '-')
        self._inning_ordinal = self._linescore.get('currentInningOrdinal', '-')
        self._inning_state = self._linescore.get('inningState', '-')

        self._inn_half = self._linescore.get('inningHalf', '-')
        self._inn_label = f'{self._inn_half} of the {self._inning_ordinal}'
        self._scheduled_innings = self._linescore.get('scheduledInnings', 9)

        # PLAYS and EVENTS
        self._all_plays = liveData['plays']['allPlays']
        self._scoring_plays = []

        self._all_events = []
        self._pitch_events = []
        self._bip_events = []
        for play in self._all_plays:
            for event in play['playEvents']:
                self._all_events.append(event)
                if event['isPitch'] == True:
                    self._pitch_events.append(event)
                    if event['details']['isInPlay'] == True:
                        self._bip_events.append(event)
            try:
                if play['about']['isScoringPlay'] == True:
                    self._scoring_plays.append(play)
            except:
                pass

    def __str__(self):
        return f"{self.game_id} | {self._away_team_abbrv} ({self._away_rhe.get('runs',0)}) @ {self._home_team_abbrv} ({self._home_rhe.get('runs',0)})"

    def __repr__(self):
        return f"{self.game_id} | {self._away_team_abbrv} ({self._away_rhe.get('runs',0)}) @ {self._home_team_abbrv} ({self._home_rhe.get('runs',0)})"

    def __getitem__(self, key):
        return getattr(self, key)

    @property
    def game_pk(self):
        """Unique Game Primary Key/ID"""
        return self.__game_pk

    @property
    def game_datetime(self):
        """Represents date & start time of game"""
        return self._game_datetime
    
    @property
    def scheduled_innings(self) -> int:
        """Number of scheduled innings"""
        return int(self._scheduled_innings)

    @property
    def inning(self) -> str:
        """Current inning as an string formatted integer"""
        return str(self._inning)

    @property
    def inning_half(self) -> str:
        """Label for the current inning

        "Top", "Bottom"

        """
        return str(self._inning_half)

    @property
    def inning_state(self) -> str:
        """State of current inning"""
        return str(self._inning_state)

    @property
    def inning_ordinal(self) -> str:
        """Ordinal display for current inning

        "1st", "2nd", "3rd", etc...

        """
        return str(self._inning_ordinal)
    
    @property
    def umpires(self):
        """Umpire Names"""
        return self._umpires

    @property
    def venue(self) -> dict:
        """Venue Data"""
        v = self._venue
        venue_name = v["name"]
        venue_mlbam = v["id"]
        fieldInfo = v["fieldInfo"]
        capacity = fieldInfo["capacity"]
        roof = fieldInfo["roofType"]
        turf = fieldInfo["turfType"]
        try:
            dimensions = {
                "leftLine": fieldInfo["leftLine"],
                "leftCenter": fieldInfo["leftCenter"],
                "center": fieldInfo["center"],
                "rightCenter": fieldInfo["rightCenter"],
                "rightLine": fieldInfo["rightLine"],
            }
        except:
            dimensions = {
                "leftLine": None,
                "leftCenter": None,
                "center": None,
                "rightCenter": None,
                "rightLine": None,
            }
        loc = v["location"]
        latitude = loc.get("defaultCoordinates", {}).get("latitude", None)
        longitude = loc.get("defaultCoordinates", {}).get("longitude", None)
        address1 = loc.get("address1", None)
        address2 = loc.get("address2", None)
        city = loc.get("city", None)
        state = loc.get("state", None)
        stateAbbrev = loc.get("stateAbbrev", None)
        zipCode = loc.get("postalCode", None)
        phone = loc.get("phone", None)

        return {
            'name': venue_name,
            'mlbam': venue_mlbam,
            'capacity': capacity,
            'roof': roof,
            'turf': turf,
            'dimensions': dimensions,
            'lat': latitude,
            'long': longitude,
            'address1': address1,
            'address2': address2,
            'city': city,
            'state': state,
            'stateAbbrev': stateAbbrev,
            'zipCode': zipCode,
            'phone': phone,
        }

    def boxscore(self, tz=None) -> dict:
        if tz is None:
            tz = self._tz
        # compiles score, batting lineups, players on field, current matchup, 
        #   count, outs, runners on base, etc.
        away = {
            "full": self._away_team_full,
            "club": self._away_team,
            "mlbam": self.away_id,
        }
        home = {
            "full": self._home_team_full,
            "club": self._home_team,
            "mlbam": self.home_id,
        }

        if self._inn_half == "Top":  # might have to adjust to using "inning state"
            team_batting = away
            team_fielding = home
        else:
            team_batting = home
            team_fielding = away

        diamond = self.diamond()
        situation = self.situation()
        matchup = self.matchup_info()
        scoreAway = self._away_rhe.get("runs", 0)
        scoreHome = self._home_rhe.get("runs", 0)
        firstPitch = prettify_time(self.first_pitch, tz=tz)
        scheduledStart = prettify_time(self.start_iso, tz=tz)
        umps = {
            "home": self._ump_home,
            "first": self._ump_first,
            "second": self._ump_second,
            "third": self._ump_third,
        }

        return {
            "away": away,
            "home": home,
            "batting": team_batting,
            "fielding": team_fielding,
            "diamond": diamond,
            "situation": situation,
            "matchup": matchup,
            "score": {"away": scoreAway, "home": scoreHome},
            "gameState": self.gameState,
            "firstPitch": firstPitch,
            "scheduledStart": scheduledStart,
            "umpires": umps,
        }

    def linescore(self) -> dict:
        """
        Returns a tuple of game's current linescore
        """

        ls = self._linescore
        ls_total = {
            "away": {
                "runs": ls.get("teams", {}).get("away", {}).get("runs", "-"),
                "hits": ls.get("teams", {}).get("away", {}).get("hits", "-"),
                "errors": ls.get("teams", {}).get("away", {}).get("errors", "-"),
            },
            "home": {
                "runs": ls.get("teams", {}).get("home", {}).get("runs", "-"),
                "hits": ls.get("teams", {}).get("home", {}).get("hits", "-"),
                "errors": ls.get("teams", {}).get("home", {}).get("errors", "-"),
            },
        }

        ls_inns = []
        ls_innings_array = ls["innings"]
        for inn in ls_innings_array:
            ls_inns.append(
                {
                    "away": {
                        "runs": inn.get("away", {}).get("runs", "-"),
                        "hits": inn.get("away", {}).get("hits", "-"),
                        "errors": inn.get("away", {}).get("errors", "-"),
                    },
                    "home": {
                        "runs": inn.get("home", {}).get("runs", "-"),
                        "hits": inn.get("home", {}).get("hits", "-"),
                        "errors": inn.get("home", {}).get("errors", "-"),
                    },
                    "inning": inn.get("num", "-"),
                    "inningOrdinal": ORDINALS[str(inn.get("num", "-"))],
                }
            )
        
        if  0 < len(ls_innings_array) < self.scheduled_innings:
            most_recent_inn = int(ls_inns[-1]["inning"])
            inns_til_9 = self.scheduled_innings - len(ls_innings_array)
            rem_innings = list(range(inns_til_9))
            for inn in rem_innings:
                next_inn = most_recent_inn + inn + 1
                ls_inns.append(
                    {
                        "away": {"runs": "-", "hits": "-", "errors": "-"},
                        "home": {"runs": "-", "hits": "-", "errors": "-"},
                        "inning": str(next_inn),
                        "inningOrdinal": ORDINALS[str(next_inn)],
                    }
                )
        return {"total": ls_total, "innings": ls_inns, "away": {}, "home": {}}

    def situation(self) -> dict:
        """Returns a python dictionary detailing the current game situation 
        (count, outs, men-on, batting queue):

        Returned Keys:
        --------------
        outs :
            number of current outs (int)
        balls :
            number of current balls (int)
        strikes :
            number of current strikes (int)
        runnersOn : dict
            dictionary with bases as keys and bools as values
        basesOccupied :
            a list of integers representing currently occupied bases
        queue :
            dictionary of batting team's next two batters (onDeck,inHole)
        """
        # outs, balls, strikes, runnersOn, batting queue
        try:
            onDeck = self._curr_offense["onDeck"]
            inHole = self._curr_offense["inHole"]
        except:
            return {
                "outs": self.outs,
                "balls": self.balls,
                "strikes": self.strikes,
                "runnersOn": {},
                "basesOccupied": [],
                "queue": {
                    "onDeck": {"id": "", "name": ""},
                    "inHole": {"id": "", "name": ""},
                },
            }

        matchup = self._curr_play["matchup"]

        basesOccupied = []
        runnersOn = {}
        if "first" in self._curr_offense.keys():
            basesOccupied.append(1)
            runnersOn["first"] = {
                "id": self._curr_offense["first"]["id"],
                "name": self._curr_offense["first"]["fullName"],
                "isOccupied": True,
            }
        else:
            runnersOn["first"] = {"isOccupied": False}

        if "second" in self._curr_offense.keys():
            basesOccupied.append(2)
            runnersOn["second"] = {
                "id": self._curr_offense["second"]["id"],
                "name": self._curr_offense["second"]["fullName"],
                "isOccupied": True,
            }
        else:
            runnersOn["second"] = {"isOccupied": False}

        if "third" in self._curr_offense.keys():
            basesOccupied.append(3)
            runnersOn["third"] = {
                "id": self._curr_offense["third"]["id"],
                "name": self._curr_offense["third"]["fullName"],
                "isOccupied": True,
            }
        else:
            runnersOn["third"] = {"isOccupied": False}

        return {
            "outs": self.outs,
            "balls": self.balls,
            "strikes": self.strikes,
            "runnersOn": runnersOn,
            "basesOccupied": basesOccupied,
            "queue": {
                "onDeck": {"id": onDeck["id"], "name": onDeck["fullName"]},
                "inHole": {"id": inHole["id"], "name": inHole["fullName"]},
            },
        }

    def diamond(self, print_as_df=True):
        """
        Returns current defensive team's roster
        
        'print_as_df' : bool, Default True
            whether or not method will return pandas.Dataframe 
            (returns python dict if False)
        """
        try:
            diamond = {
                1: self._curr_defense["pitcher"],
                2: self._curr_defense["catcher"],
                3: self._curr_defense["first"],
                4: self._curr_defense["second"],
                5: self._curr_defense["third"],
                6: self._curr_defense["shortstop"],
                7: self._curr_defense["left"],
                8: self._curr_defense["center"],
                9: self._curr_defense["right"],
            }
        except:
            return {}
        curr_diamond = []
        for key, value in diamond.items():
            curr_diamond.append([POSITION_DICT[key], value["fullName"]])

        df = pd.DataFrame(curr_diamond)

        diamond = {
            "pitcher": {
                "name": self._curr_defense["pitcher"]["fullName"],
                "id": self._curr_defense["pitcher"]["id"],
            },
            "catcher": {
                "name": self._curr_defense["catcher"]["fullName"],
                "id": self._curr_defense["catcher"]["id"],
            },
            "first": {
                "name": self._curr_defense["first"]["fullName"],
                "id": self._curr_defense["first"]["id"],
            },
            "second": {
                "name": self._curr_defense["second"]["fullName"],
                "id": self._curr_defense["second"]["id"],
            },
            "third": {
                "name": self._curr_defense["third"]["fullName"],
                "id": self._curr_defense["third"]["id"],
            },
            "shortstop": {
                "name": self._curr_defense["shortstop"]["fullName"],
                "id": self._curr_defense["shortstop"]["id"],
            },
            "left": {
                "name": self._curr_defense["left"]["fullName"],
                "id": self._curr_defense["left"]["id"],
            },
            "center": {
                "name": self._curr_defense["center"]["fullName"],
                "id": self._curr_defense["center"]["id"],
            },
            "right": {
                "name": self._curr_defense["right"]["fullName"],
                "id": self._curr_defense["right"]["id"],
            },
        }

        return diamond

    def matchup_info(self):
        """
        Gets current matchup info in form of python dictionary:

        Returned dict keys:
        * `at_bat`: current batter (dict)
        * `pitching`: current pitcher (dict)
        * `zone`: current batter's strike zone metrics (tuple)
        """

        try:
            matchup = self._curr_play["matchup"]
            zone_top = self._curr_play["playEvents"][-1]["pitchData"]["strikeZoneTop"]
            zone_bot = self._curr_play["playEvents"][-1]["pitchData"][
                "strikeZoneBottom"
            ]
        except:
            return {"atBat": {}, "pitching": {}, "zone": (3.5, 1.5)}
        atBat = {
            "name": matchup["batter"]["fullName"],
            "id": matchup["batter"]["id"],
            "bats": matchup["batSide"]["code"],
            "zone_top": self._players[f'ID{matchup["batter"]["id"]}']["strikeZoneTop"],
            "zone_bot": self._players[f'ID{matchup["batter"]["id"]}'][
                "strikeZoneBottom"
            ],
            "stands": matchup["batSide"]["code"],
        }
        pitching = {
            "name": matchup["pitcher"]["fullName"],
            "id": matchup["pitcher"]["id"],
            "throws": matchup["pitchHand"]["code"],
        }

        return {"atBat": atBat, "pitching": pitching, "zone": (zone_top, zone_bot)}

    def matchup_event_log(self) -> pd.DataFrame:
        """
        Gets a pitch-by-pitch log of the current batter-pitcher matchup:

        Column labels:
        * `Pitch #`: pitch number for current matchup (current event included)
        * `Details`: details on pitch result (e.g. 'Ball', 'Called Strike', 'Ball In Dirt', 'Foul', etc.)
        * `Pitch`: pitch type (e.g. 'Curveball', 'Four-Seam Fastball')
        * `Release Speed`: pitch speed when released (in MPH)
        * `End Speed`: pitch speed when ball crosses plate (in MPH)
        * `Zone`: strike zone
        * `Spin`: ball spin rate (in RPM)
        * `pX`: x-coordinate (horizontal) of pitch (in feet)
        * `pZ`: z-coordinate (vertical) of pitch relative to batter's strike zone (in feet)
        * `hX`: x-coordinate
        * `hY`: y-coordinate
        * `Hit Location`: location the ball was hit to, if applicable (field pos: 1-9)

        """

        headers = [
            "pitch_num",
            "details",
            "zone_top",
            "zone_bot",
            "zoneTopInitial",
            "zoneBottomInitial",
            "pitch_type",
            "pitch_code",
            "release_speed",
            "end_speed",
            "spin",
            "zone",
            "pX",
            "pZ",
            "hit_location",
            "hX",
            "hY",
            "play_id",
        ]
        try:
            pa_events = self._curr_play["playEvents"]
        except:
            empty_df = pd.DataFrame(columns=headers)
            return empty_df

        events_data = []

        for ab_log in pa_events:
            play_id = ab_log.get("playId")
            if ab_log["isPitch"] == False:
                pass
            else:
                event = []

                pitchNumber = ab_log["pitchNumber"]

                details = ab_log["details"]["description"]

                try:
                    pitchType = ab_log["details"]["type"]["description"]
                    pitchCode = ab_log["details"]["type"]["code"]
                except:
                    pitchType = "unknown"
                    pitchCode = "unknown"

                try:
                    start_vel = ab_log["pitchData"]["startSpeed"]
                except:
                    start_vel = "--"

                try:
                    end_vel = ab_log["pitchData"]["endSpeed"]
                except:
                    end_vel = "--"

                try:
                    pX_coord = ab_log["pitchData"]["coordinates"]["pX"]
                except:
                    pX_coord = "--"
                try:
                    pZ_coord = ab_log["pitchData"]["coordinates"]["pZ"]
                except:
                    pZ_coord = "--"

                try:
                    zoneTopInitial = pa_events[0]["pitchData"]["strikeZoneTop"]
                    zoneBottomInitial = pa_events[0]["pitchData"]["strikeZoneBottom"]
                except:
                    try:
                        zoneTopInitial = pa_events[0]["pitchData"]["strikeZoneTop"]
                        zoneBottomInitial = pa_events[0]["pitchData"][
                            "strikeZoneBottom"
                        ]
                    except:
                        zoneTopInitial = 3.5
                        zoneBottomInitial = 1.5
                try:
                    zone_top = ab_log["pitchData"]["strikeZoneTop"]
                    zone_bot = ab_log["pitchData"]["strikeZoneBottom"]
                except:
                    zone_top = 3.5
                    zone_bot = 1.5

                try:
                    spin = ab_log["pitchData"]["breaks"]["spinRate"]
                except:
                    spin = ""
                try:
                    zone = ab_log["pitchData"]["zone"]
                except:
                    zone = ""
                try:
                    hit_location = ab_log["hitData"]["location"]
                except:
                    hit_location = ""
                try:
                    hX = ab_log["hitData"]["coordinates"]["coordX"]
                    hY = ab_log["hitData"]["coordinates"]["coordY"]
                except:
                    hX = ""
                    hY = ""

                event.append(pitchNumber)
                event.append(details)
                event.append(zone_top)
                event.append(zone_bot)
                event.append(zoneTopInitial)
                event.append(zoneBottomInitial)
                event.append(pitchType)
                event.append(pitchCode)
                event.append(start_vel)
                event.append(end_vel)
                event.append(spin)
                event.append(zone)
                event.append(pX_coord)
                event.append(pZ_coord)
                event.append(hit_location)
                event.append(hX)
                event.append(hY)
                event.append(play_id)

                events_data.append(event)

        matchup_df = pd.DataFrame(data=events_data, columns=headers)
        matchup_df.sort_values(by=["pitch_num"], inplace=True)

        return matchup_df

    def plays(self) -> pd.DataFrame:
        """
        Get detailed log of each plate appearance in game

        Note:
        ----------
            Dataframe begins with most recent plate appearance
        """
        headers = [
            "bat_tm_mlbam",
            "bat_tm_name",
            "pa",
            "inning",
            "batter",
            "bat_side",
            "pitcher",
            "pa_pitch_count",
            "event",
            "event_type",
            "details",
            "pitch_type",
            "pitch_code",
            "release_velocity",
            "end_velocity",
            "spin_rate",
            "zone",
            "exit_velocity",
            "launch_angle",
            "distance",
            "location",
            "hit_trajectory",
            "hX",
            "hY",
            "category",
            "timeElasped",
            "timeStart",
            "timeEnd",
            "batterID",
            "pitcherID",
            "isHome",
            "play_id",
        ]

        events_data = []
        for play in self._all_plays:
            events = play["playEvents"]
            for e in events:
                if "game_advisory" in e.get("details", {}).get("eventType", "").lower():
                    pass
                else:
                    firstEvent = e
                    break
                
            # Sometimes, the 'playEvents' array isn't populated for a few 
            # seconds; so we skip over it for the time being
            if len(events) == 0:
                continue
            lastEvent = events[-1]
            play_id = lastEvent.get("playId", "-")
            pitchData = lastEvent.get('pitchData',{})
            try:
                ab_num = play["atBatIndex"] + 1
            except:
                ab_num = "--"
            try:
                bat_side = play["matchup"]["batSide"]["code"]
            except:
                bat_side = "--"

            try:
                innNum = play["about"]["inning"]
            except:
                innNum = "--"
            try:
                innHalf = play["about"]["halfInning"]
            except:
                innHalf = "--"
            if innHalf == "bottom":
                inning = f"Bot {innNum}"
            else:
                inning = f"Top {innNum}"

            try:
                batter = play["matchup"]["batter"]
                batterName = batter["fullName"]
                batterID = batter["id"]
            except:
                batterName = "--"
                batterID = "--"
            try:
                pitcher = play["matchup"]["pitcher"]
                pitcherName = pitcher["fullName"]
                pitcherID = pitcher["id"]
            except:
                pitcherName = "--"
                pitcherID = "--"
            try:
                pitchNum = lastEvent["pitchNumber"]
            except:
                pitchNum = "--"
            try:
                event = play["result"]["event"]
                event_type = play["result"]["eventType"]
            except:
                event = "--"
                event_type = "--"
            try:
                details = play["result"]["description"]
            except:
                details = "--"
            try:
                pitchType = lastEvent["details"]["type"]["description"]
                pitchCode = lastEvent["details"]["type"]["code"]
            except:
                pitchType = "--"
                pitchCode = "--"
            try:
                releaseSpeed = pitchData["startSpeed"]
            except:
                releaseSpeed = "--"
            try:
                endSpeed = pitchData["endSpeed"]
            except:
                endSpeed = "--"
            try:
                spinRate = pitchData["breaks"]["spinRate"]
            except:
                spinRate = "--"
            try:
                zone = pitchData["zone"]
            except:
                zone = "--"

            # Hit Data (and Trajectory - if ball is in play)
            try:
                hitData = lastEvent["hitData"]
            except:
                pass
            try:
                launchSpeed = hitData["launchSpeed"]
            except:
                launchSpeed = "--"
            try:
                launchAngle = hitData["launchAngle"]
            except:
                launchAngle = "--"
            try:
                distance = hitData["totalDistance"]
            except:
                distance = "--"
            try:
                hitLocation = hitData["location"]
            except:
                hitLocation = "--"
            try:
                hX = hitData["coordinates"]["coordX"]
                hY = hitData["coordinates"]["coordY"]
            except:
                hX = "--"
                hY = "--"

            try:
                hitTrajectory = hitData["trajectory"]
            except:
                hitTrajectory = "--"

            # Event Category/Type
            try:
                category = lastEvent["type"]
            except:
                category = "--"

            # Time information
            try:
                startTime = firstEvent["startTime"]
                startTime_obj = dt.datetime.strptime(startTime, iso_format_ms).replace(
                    tzinfo=utc_zone
                )
                startTime = dt.datetime.strftime(startTime_obj, iso_format_ms)

            except:
                startTime = "--"
            try:
                endTime = lastEvent["endTime"]
                endTime_obj = dt.datetime.strptime(endTime, iso_format_ms).replace(
                    tzinfo=utc_zone
                )
                endTime = dt.datetime.strftime(endTime_obj, iso_format_ms)
                # endTime = dt.datetime.strptime(play["playEvents"][-1]["endTime"],iso_format_ms).replace(tzinfo=utc_zone)

                elasped = endTime_obj - startTime_obj

            except Exception as e:
                endTime = "--"
                elasped = "--"
                print(f"ERROR: -- {e} --")

            # Is Home Team Batting?
            if f"ID{batterID}" in self._home_player_data.keys():
                homeBatting = True
                batTeamID = self.home_id
                battingTeam = self._home_team
            else:
                homeBatting = False
                batTeamID = self.away_id
                battingTeam = self._away_team

            event_data = [
                batTeamID,
                battingTeam,
                ab_num,
                inning,
                batterName,
                bat_side,
                pitcherName,
                pitchNum,
                event,
                event_type,
                details,
                pitchType,
                pitchCode,
                releaseSpeed,
                endSpeed,
                spinRate,
                zone,
                launchSpeed,
                launchAngle,
                distance,
                hitLocation,
                hitTrajectory,
                hX,
                hY,
                category,
                prettify_time(elasped),
                prettify_time(startTime),
                prettify_time(endTime),
                batterID,
                pitcherID,
                homeBatting,
                play_id,
            ]

            events_data.append(event_data)

        df = pd.DataFrame(data=events_data, columns=headers).sort_values(
            by=["pa"], ascending=False
        )

        return df

    def events(self) -> pd.DataFrame:
        """Get detailed log of every pitch event

        NOTE: Dataframe begins with most recent pitch event
        """
        headers = [
            "bat_tm_mlbam",
            "bat_tm_name",
            "pa",
            "event",
            "event_type",
            "inning",
            "pitch_idx",
            "batter",
            "batter_mlbam",
            "bat_side",
            "zone_top",
            "zone_bot",
            "pitcher",
            "pitcher_mlbam",
            "details",
            "pitch_num",
            "pitch_type",
            "pitch_code",
            "call",
            "count",
            "outs",
            "zone_top",
            "zone_bot",
            "release_velocity",
            "end_velocity",
            "spin_rate",
            "zone",
            "pX",
            "pZ",
            "exit_velocity",
            "launch_angle",
            "distance",
            "location",
            "hX",
            "hY",
            "category",
            "end_time",
            "isHome",
            "play_id",
        ]

        events_data = []
        for play in self._all_plays:
            try:
                ab_num = play["about"]["atBatIndex"] + 1
            except:
                ab_num = "--"
            try:
                batter = play["matchup"]["batter"]
            except:
                batter = "--"
            try:
                pitcher = play["matchup"]["pitcher"]
            except:
                pitcher = "--"
            try:
                zone_top = self._players[f'ID{batter["id"]}']["strikeZoneTop"]
                zone_bot = self._players[f'ID{batter["id"]}']["strikeZoneBottom"]
            except:
                zone_top = "-"
                zone_bot = "-"
            try:
                bat_side = play["matchup"]["batSide"]["code"]
            except:
                bat_side = "-"
                
            play_events = play["playEvents"]
            if len(play_events) == 0:
                continue
            last_idx = play_events[-1]["index"]

            for event_idx, event in enumerate(play["playEvents"]):
                play_id = event.get("playId")
                try:
                    if event_idx != last_idx:
                        desc = "--"
                    else:
                        desc = play["result"]["description"]
                except:
                    desc = "--"

                try:
                    event_label = play["result"]["event"]
                    event_type = play["result"]["eventType"]
                except:
                    event_label = "--"
                    event_type = "--"

                try:
                    pitchNumber = event["pitchNumber"]
                except:
                    pitchNumber = 0

                # Times
                try:
                    startTime = dt.datetime.strptime(
                        event["startTime"], iso_format_ms
                    ).replace(tzinfo=utc_zone)
                    # startTimeStr = startTime.strftime(r"%H:%M:%S (%Y-%m-%d)")
                except:
                    startTime = "--"
                try:
                    endTime = dt.datetime.strptime(
                        event["endTime"], iso_format_ms
                    ).replace(tzinfo=utc_zone)
                    # endTimeStr = endTime.strftime(r"%H:%M:%S (%Y-%m-%d)")
                except:
                    endTime = "--"
                    # endTimeStr = "--"
                try:
                    elapsed = endTime - startTime
                    elapsed = prettify_time(elapsed)
                except:
                    elapsed = "--"

                # Call and Pitch Type and Count/Outs
                try:
                    pitchType = event["details"]["type"]["description"]
                except:
                    pitchType = "--"
                try:
                    pitchCode = event["details"]["type"]["code"]
                except:
                    pitchCode = "--"
                try:
                    call = event["details"]["description"]
                except:
                    call = "--"
                try:
                    count = f'{event["count"]["balls"]}-{event["count"]["strikes"]}'
                except:
                    count = "--"
                try:
                    outs = event["count"]["outs"]
                except:
                    outs = "--"

                # Pitch Data
                try:
                    pitchData = event["pitchData"]
                except:
                    pass
                try:
                    startSpeed = pitchData["startSpeed"]
                except:
                    startSpeed = "--"
                try:
                    endSpeed = pitchData["endSpeed"]
                except:
                    endSpeed = "--"
                try:
                    spinRate = pitchData["breaks"]["spinRate"]
                except:
                    spinRate = "--"
                try:
                    zone = pitchData["zone"]
                except:
                    zone = "--"
                try:
                    pX = pitchData["coordinates"]["pX"]
                    pZ = pitchData["coordinates"]["pZ"]
                except:
                    pX = "--"
                    pZ = "--"
                try:
                    zone_top = pitchData["strikeZoneTop"]
                    zone_bot = pitchData["strikeZoneBottom"]
                except:
                    zone_top = 3.5
                    zone_bot = 1.5

                # Hit Data
                if "hitData" in event.keys():
                    try:
                        hitData = event["hitData"]
                    except:
                        pass
                    try:
                        launchSpeed = hitData["launchSpeed"]
                    except:
                        launchSpeed = "--"
                    try:
                        launchAngle = hitData["launchAngle"]
                    except:
                        launchAngle = "--"
                    try:
                        distance = hitData["totalDistance"]
                    except:
                        distance = "--"
                    try:
                        hitLocation = hitData["location"]
                    except:
                        hitLocation = "--"
                    try:
                        hX = hitData["coordinates"]["coordX"]
                        hY = hitData["coordinates"]["coordY"]
                    except:
                        hX = "--"
                        hY = "--"
                else:
                    launchSpeed = "--"
                    launchAngle = "--"
                    distance = "--"
                    hitLocation = "--"
                    hX = "--"
                    hY = "--"

                # type
                try:
                    category = event["type"]
                except:
                    category = "--"

                # Is Home Team Batting?
                if f'ID{batter["id"]}' in self._home_player_data.keys():
                    homeBatting = True
                    batTeamID = self.home_id
                    battingTeam = self._home_team
                else:
                    homeBatting = False
                    batTeamID = self.away_id
                    battingTeam = self._away_team

                #
                event_data = [
                    batTeamID,
                    battingTeam,
                    ab_num,
                    event_label,
                    event_type,
                    play["about"]["inning"],
                    event_idx,
                    batter["fullName"],
                    batter["id"],
                    bat_side,
                    zone_top,
                    zone_bot,
                    pitcher["fullName"],
                    pitcher["id"],
                    desc,
                    pitchNumber,
                    pitchType,
                    pitchCode,
                    call,
                    count,
                    outs,
                    zone_top,
                    zone_bot,
                    startSpeed,
                    endSpeed,
                    spinRate,
                    zone,
                    pX,
                    pZ,
                    launchSpeed,
                    launchAngle,
                    distance,
                    hitLocation,
                    hX,
                    hY,
                    category,
                    endTime,
                    homeBatting,
                    play_id,
                ]

                events_data.append(event_data)

        df = pd.DataFrame(data=events_data, columns=headers).sort_values(
            by=["pa", "pitch_idx"], ascending=False
        )
        #
        return df

    def scoring_event_log(self) -> pd.DataFrame:
        """Get detailed log of every scoring play pitch event

        NOTE: Dataframe begins with most recent pitch event
        """
        headers = [
            "bat_tm_mlbam",
            "bat_tm_name",
            "pa",
            "event",
            "event_type",
            "inning",
            "pitch_idx",
            "batter",
            "batter_mlbam",
            "pitcher",
            "pitcher_mlbam",
            "details",
            "pitch_num",
            "pitch_type",
            "pitch_code",
            "call",
            "count",
            "outs",
            "release_velocity",
            "end_velocity",
            "spin_rate",
            "zone",
            "pX",
            "pZ",
            "exit_velocity",
            "launch_angle",
            "distance",
            "location",
            "hX",
            "hY",
            "category",
            "end_time",
            "isHome",
        ]

        events_data = []
        for play in self._scoring_plays:
            try:
                ab_num = play["about"]["atBatIndex"] + 1
            except:
                ab_num = "--"
            try:
                batter = play["matchup"]["batter"]
            except:
                batter = "--"
            try:
                pitcher = play["matchup"]["pitcher"]
            except:
                pitcher = "--"

            last_idx = play["playEvents"][-1]["index"]

            for event_idx, event in enumerate(play["playEvents"]):
                try:
                    if event_idx != last_idx:
                        desc = "--"
                    else:
                        desc = play["result"]["description"]
                except:
                    desc = "--"

                try:
                    event_label = play["result"]["event"]
                    event_type = play["result"]["eventType"]
                except:
                    event_label = "--"
                    event_type = "--"
                # Times

                try:
                    pitchNumber = event["pitchNumber"]
                except:
                    pitchNumber = 0
                try:
                    # startTime = dt.datetime.strptime(event["startTime"],iso_format_ms).replace(tzinfo=utc_zone)
                    startTime = dt.datetime.strptime(event["startTime"], iso_format_ms)
                    startTimeStr = startTime.strftime(r"%H:%M:%S (%Y-%m-%d)")
                except:
                    startTime = "--"
                    # startTimeStr = "--"
                try:
                    # endTime = dt.datetime.strptime(event["endTime"],iso_format_ms).replace(tzinfo=utc_zone)
                    endTime = dt.datetime.strptime(event["endTime"], iso_format_ms)
                    # endTimeStr = endTime.strftime(r"%H:%M:%S (%Y-%m-%d)")
                except:
                    endTime = "--"
                    # endTimeStr = "--"
                try:
                    elapsed = endTime - startTime
                    elapsed = prettify_time(elapsed)
                except:
                    elapsed = "--"

                # Call and Pitch Type and Count/Outs
                try:
                    pitchType = event["details"]["type"]["description"]
                except:
                    pitchType = "--"
                try:
                    pitchCode = event["details"]["type"]["code"]
                except:
                    pitchCode = "--"
                try:
                    call = event["details"]["description"]
                except:
                    call = "--"
                try:
                    count = f'{event["count"]["balls"]}-{event["count"]["strikes"]}'
                except:
                    count = "--"
                try:
                    outs = event["count"]["outs"]
                except:
                    outs = "--"

                # Pitch Data
                try:
                    pitchData = event["pitchData"]
                except:
                    pass
                try:
                    startSpeed = pitchData["startSpeed"]
                except:
                    startSpeed = "--"
                try:
                    endSpeed = pitchData["endSpeed"]
                except:
                    endSpeed = "--"
                try:
                    spinRate = pitchData["breaks"]["spinRate"]
                except:
                    spinRate = "--"
                try:
                    zone = pitchData["zone"]
                except:
                    zone = "--"
                try:
                    pX = pitchData["coordinates"]["pX"]
                    pZ = pitchData["coordinates"]["pZ"]
                except:
                    pX = "--"
                    pZ = "--"

                # Hit Data
                if "hitData" in event.keys():
                    try:
                        hitData = event["hitData"]
                    except:
                        pass
                    try:
                        launchSpeed = hitData["launchSpeed"]
                    except:
                        launchSpeed = "--"
                    try:
                        launchAngle = hitData["launchAngle"]
                    except:
                        launchAngle = "--"
                    try:
                        distance = hitData["totalDistance"]
                    except:
                        distance = "--"
                    try:
                        hitLocation = hitData["location"]
                    except:
                        hitLocation = "--"
                    try:
                        hX = hitData["coordinates"]["coordX"]
                        hY = hitData["coordinates"]["coordY"]
                    except:
                        hX = "--"
                        hY = "--"
                else:
                    launchSpeed = "--"
                    launchAngle = "--"
                    distance = "--"
                    hitLocation = "--"
                    hX = "--"
                    hY = "--"

                # type
                try:
                    category = event["type"]
                except:
                    category = "--"

                # Is Home Team Batting?
                if f'ID{batter["id"]}' in self._home_player_data.keys():
                    homeBatting = True
                    batTeamID = self.home_id
                    battingTeam = self._home_team
                else:
                    homeBatting = False
                    batTeamID = self.away_id
                    battingTeam = self._away_team

                #
                event_data = [
                    batTeamID,
                    battingTeam,
                    ab_num,
                    event_label,
                    event_type,
                    play["about"]["inning"],
                    event_idx,
                    batter["fullName"],
                    batter["id"],
                    pitcher["fullName"],
                    pitcher["id"],
                    desc,
                    pitchNumber,
                    pitchType,
                    pitchCode,
                    call,
                    count,
                    outs,
                    startSpeed,
                    endSpeed,
                    spinRate,
                    zone,
                    pX,
                    pZ,
                    launchSpeed,
                    launchAngle,
                    distance,
                    hitLocation,
                    hX,
                    hY,
                    category,
                    endTime,
                    homeBatting,
                ]

                events_data.append(event_data)

        df = pd.DataFrame(data=events_data, columns=headers).sort_values(
            by=["pa", "pitch_idx"], ascending=False
        )
        #
        return df

    # TEAMS' INDIVIDUAL BATTER STATS
    def away_batting_stats(self) -> pd.DataFrame:
        """
        Get current game batting stats for players on AWAY team

        Returns:
        ----------
            `pandas.Dataframe`


        Example:
        ----------
        >>> stats = away_batting_stats()
        >>> print(stats["Player","Pos","AB","R","H"])
                       Player Pos  AB  R   H
        0       Leody Taveras  CF   5  0   1
        1  Isiah Kiner-Falefa  SS   4  2   3
        2       Adolis Garcia  RF   4  1   1
        3      Nathaniel Lowe  1B   4  1   0
        4           DJ Peters  DH   5  2   4
        5          Jonah Heim   C   4  0   0
        6          Nick Solak  2B   4  0   0
        7        Jason Martin  LF   4  0   0
        8     Yonny Hernandez  3B   4  1   2
        9             Summary      38  7  11


        """
        # these stats will be only for this CURRENT GAME (with the exception of a player's batting average stat)
        if (
            self.gameState == "Live"
            or self.gameState == "Final"
            or self.gameState == "Preview"
        ):
            tm = self._away_stats["batting"]
            players = self._away_player_data
            # headers = ["Player","Pos","AB","R","H","RBI","SO","BB","AVG","HR","2B","3B","FO","GO","IBB","SacBunts","SacFlies","GIDP","batting","substitute","bbrefID","mlbam"]
            headers = [
                "Player",
                "Pos",
                "AB",
                "R",
                "H",
                "RBI",
                "SO",
                "BB",
                "AVG",
                "HR",
                "2B",
                "3B",
                "FO",
                "GO",
                "IBB",
                "sB",
                "sF",
                "GIDP",
                "at_bat",
                "is_sub",
                "mlbam",
            ]
            rows = []
            for playerid in self._away_lineup:
                player = players[f"ID{playerid}"]
                name = player["person"]["fullName"]
                position = player["position"]["abbreviation"]
                stats = player["stats"]["batting"]
                if len(stats) == 0:
                    pass
                else:
                    at_bats = stats["atBats"]
                    # pas = stats["plateAppearances"]
                    runs = stats["runs"]
                    hits = stats["hits"]
                    dbls = stats["doubles"]
                    trpls = stats["triples"]
                    hrs = stats["homeRuns"]
                    rbis = stats["rbi"]
                    sos = stats["strikeOuts"]
                    bbs = stats["baseOnBalls"]
                    flyouts = stats["flyOuts"]
                    groundouts = stats["groundOuts"]
                    ibbs = stats["intentionalWalks"]
                    sacbunts = stats["sacBunts"]
                    sacflies = stats["sacFlies"]
                    gidp = stats["groundIntoDoublePlay"]
                    avg = player["seasonStats"]["batting"]["avg"]
                    isCurrentBatter = player["gameStatus"]["isCurrentBatter"]
                    isSubstitute = player["gameStatus"]["isSubstitute"]
                    # try:
                    #     bbrefID = self._people[self._people["mlbam"]==playerid].bbrefID.item()
                    # except:
                    #     search_url = f"https://www.baseball-reference.com/redirect.fcgi?player=1&mlb_ID={playerid}"
                    #     req = requests.get(search_url)
                    #     resp = req.url
                    #     bbrefID = resp[resp.rfind("/")+1:resp.rfind(".")]

                    # row_data = [name,position,at_bats,runs,hits,rbis,sos,bbs,avg,hrs,dbls,trpls,flyouts,groundouts,ibbs,sacbunts,sacflies,gidp,isCurrentBatter,isSubstitute,bbrefID,playerid]
                    row_data = [
                        name,
                        position,
                        at_bats,
                        runs,
                        hits,
                        rbis,
                        sos,
                        bbs,
                        avg,
                        hrs,
                        dbls,
                        trpls,
                        flyouts,
                        groundouts,
                        ibbs,
                        sacbunts,
                        sacflies,
                        gidp,
                        isCurrentBatter,
                        isSubstitute,
                        playerid,
                    ]
                    rows.append(row_data)
            summary = [
                "Summary",
                " ",
                tm["atBats"],
                tm["runs"],
                tm["hits"],
                tm["rbi"],
                tm["strikeOuts"],
                tm["baseOnBalls"],
                tm["avg"],
                tm["homeRuns"],
                tm["doubles"],
                tm["triples"],
                tm["flyOuts"],
                tm["groundOuts"],
                tm["intentionalWalks"],
                tm["sacBunts"],
                tm["sacFlies"],
                tm["groundIntoDoublePlay"],
                " ",
                " ",
                " ",
            ]
            rows.append(summary)
            df = pd.DataFrame(data=rows, columns=headers)
            return df
        else:
            print("error. check API for game's state")
            return pd.DataFrame()

    def home_batting_stats(self) -> pd.DataFrame:
        """
        Get current game batting stats for players on HOME team

        * Note: Last entry in dataframe is "Summary"

        Returns:
        ----------
            `pandas.Dataframe`

        Example:
        ----------
        >>> stats = home_batting_stats()
        >>> print(stats["Player","Pos","AB","R","H"])
                     Player Pos  AB  R  H
        0      Tim Anderson  SS   4  0  2
        1       Luis Robert  CF   4  0  0
        2        Jose Abreu  1B   4  0  0
        3   Yasmani Grandal   C   4  1  1
        4      Eloy Jimenez  LF   3  0  0
        5    Billy Hamilton  LF   0  1  0
        6      Yoan Moncada  3B   4  2  3
        7      Gavin Sheets  DH   4  1  1
        8        Adam Engel  RF   2  0  0
        9      Leury Garcia  2B   3  0  1
        10          Summary      32  5  8

        >>>
        """
        # these stats will be only for this CURRENT GAME (with the exception of a player's batting average stat)
        if (
            self.gameState == "Live"
            or self.gameState == "Final"
            or self.gameState == "Preview"
        ):
            tm = self._home_stats["batting"]
            players = self._home_player_data
            headers = [
                "Player",
                "Pos",
                "AB",
                "R",
                "H",
                "RBI",
                "SO",
                "BB",
                "AVG",
                "HR",
                "2B",
                "3B",
                "FO",
                "GO",
                "IBB",
                "sB",
                "sF",
                "GIDP",
                "at_bat",
                "is_sub",
                "mlbam",
            ]
            rows = []
            for playerid in self._home_lineup:
                player = players[f"ID{playerid}"]
                name = player["person"]["fullName"]
                position = player["position"]["abbreviation"]
                stats = player["stats"]["batting"]
                if len(stats) == 0:
                    pass
                else:
                    at_bats = stats["atBats"]
                    # pas = stats["plateAppearances"]
                    runs = stats["runs"]
                    hits = stats["hits"]
                    dbls = stats["doubles"]
                    trpls = stats["triples"]
                    hrs = stats["homeRuns"]
                    rbis = stats["rbi"]
                    sos = stats["strikeOuts"]
                    bbs = stats["baseOnBalls"]
                    flyouts = stats["flyOuts"]
                    groundouts = stats["groundOuts"]
                    ibbs = stats["intentionalWalks"]
                    sacbunts = stats["sacBunts"]
                    sacflies = stats["sacFlies"]
                    gidp = stats["groundIntoDoublePlay"]
                    avg = player["seasonStats"]["batting"]["avg"]
                    isCurrentBatter = player["gameStatus"]["isCurrentBatter"]
                    isSubstitute = player["gameStatus"]["isSubstitute"]

                    row_data = [
                        name,
                        position,
                        at_bats,
                        runs,
                        hits,
                        rbis,
                        sos,
                        bbs,
                        avg,
                        hrs,
                        dbls,
                        trpls,
                        flyouts,
                        groundouts,
                        ibbs,
                        sacbunts,
                        sacflies,
                        gidp,
                        isCurrentBatter,
                        isSubstitute,
                        playerid,
                    ]
                    rows.append(row_data)
            summary = [
                "Summary",
                " ",
                tm["atBats"],
                tm["runs"],
                tm["hits"],
                tm["rbi"],
                tm["strikeOuts"],
                tm["baseOnBalls"],
                tm["avg"],
                tm["homeRuns"],
                tm["doubles"],
                tm["triples"],
                tm["flyOuts"],
                tm["groundOuts"],
                tm["intentionalWalks"],
                tm["sacBunts"],
                tm["sacFlies"],
                tm["groundIntoDoublePlay"],
                " ",
                " ",
                " ",
            ]
            rows.append(summary)
            df = pd.DataFrame(data=rows, columns=headers)
            return df
        else:
            print("error. check API for game's state")
            return pd.DataFrame()

    # TEAMS' INDIVIDUAL PITCHER STATS
    def away_pitching_stats(self) -> pd.DataFrame:
        """Get current game pitching stats for players on AWAY team

        * Note: Last entry in dataframe is "Summary"

        Returns:
        ----------
            `pandas.Dataframe`

        Example:
        ----------
        >>> stats = away_pitching_stats()
        >>> print(stats[["Player","IP","R","H","ER","SO"]])
                    Player   IP  R  H  ER  SO
        0     Matt Manning  5.0  0  2   0   7
        1       Jose Urena  1.2  3  3   3   2
        2       Alex Lange  0.1  0  2   0   1
        3  Kyle Funkhouser  1.0  2  1   2   2
        0          Summary  7.3  5  8   5  12

        >>>
        """
        # these stats will be only for this CURRENT GAME (with the exception of a player's ERA stat)
        players = self._away_player_data
        headers = [
            "Player",
            "Ct",
            "IP",
            "R",
            "H",
            "ER",
            "SO",
            "BB",
            "K",
            "B",
            f"ERA ({self.game_date[:4]})",
            "Strike %",
            "HR",
            "2B",
            "3B",
            "PkOffs",
            "Outs",
            "IBB",
            "HBP",
            "SB",
            "WP",
            "BF",
            "mlbam",
        ]
        rows = []
        for playerid in self._away_pitcher_lineup:
            player = players[f"ID{playerid}"]
            stats = player["stats"]["pitching"]

            name = player["person"]["fullName"]
            # pos = player["position"]["abbreviation"]
            pitch_ct = stats.get("pitchesThrown", "")
            ip = float(stats.get("inningsPitched", 0))
            sos = stats.get("strikeOuts", 0)
            bbs = stats.get("baseOnBalls", 0)
            strikes = stats.get("strikes", 0)
            balls = stats.get("balls", 0)
            try:
                strike_perc = float(stats["strikePercentage"])
            except:
                strike_perc = "--"
            runs = stats["runs"]
            ers = stats["earnedRuns"]
            hits = stats["hits"]
            hrs = stats["homeRuns"]
            dbls = stats["doubles"]
            trpls = stats["triples"]
            pkoffs = stats["pickoffs"]
            outs = stats["outs"]
            ibbs = stats["intentionalWalks"]
            hbp = stats["hitByPitch"]
            sbs = stats["stolenBases"]
            wps = stats["wildPitches"]
            batters_faced = stats["battersFaced"]
            era = player["seasonStats"]["pitching"]["era"]
            # try:bbrefID = self._people[self._people["mlbam"]==playerid].bbrefID.item()
            # except: # retrieves player's bbrefID if not in current registry
            #     search_url = f"https://www.baseball-reference.com/redirect.fcgi?player=1&mlb_ID={playerid}"
            #     req = requests.get(search_url)
            #     resp = req.url
            #     bbrefID = resp[resp.rfind("/")+1:resp.rfind(".")]

            row_data = [
                name,
                pitch_ct,
                ip,
                runs,
                hits,
                ers,
                sos,
                bbs,
                strikes,
                balls,
                era,
                strike_perc,
                hrs,
                dbls,
                trpls,
                pkoffs,
                outs,
                ibbs,
                hbp,
                sbs,
                wps,
                batters_faced,
                playerid,
            ]
            rows.append(row_data)

        df = pd.DataFrame(data=rows, columns=headers)

        return df

    def home_pitching_stats(self) -> pd.DataFrame:
        """
        Get current game pitching stats for players on HOME team

        * Note: Last entry in dataframe is "Summary"

        Returns:
        ----------
            `pandas.Dataframe`

        Example:
        ----------
        >>> stats = home_pitching_stats()
        >>> print(stats[["Player","IP","R","H","ER","SO"]])
                   Player   IP  R  H  ER  SO
        0   Lucas Giolito  5.0  1  2   1   3
        1       Ryan Burr  1.0  0  1   0   1
        2  Dallas Keuchel  0.2  3  4   3   1
        3     Matt Foster  0.1  0  0   0   0
        4    Aaron Bummer  1.0  0  0   0   0
        5   Liam Hendriks  1.0  0  0   0   2
        0         Summary  8.3  4  7   4   7

        >>>
        """
        # these stats will be only for this CURRENT GAME (with the exception of a player's ERA stat)
        players = self._home_player_data
        headers = [
            "Player",
            "Ct",
            "IP",
            "R",
            "H",
            "ER",
            "SO",
            "BB",
            "K",
            "B",
            f"ERA ({self.game_date[:4]})",
            "Strike %",
            "HR",
            "2B",
            "3B",
            "PkOffs",
            "Outs",
            "IBB",
            "HBP",
            "SB",
            "WP",
            "BF",
            "mlbam",
        ]
        rows = []
        for playerid in self._home_pitcher_lineup:
            player = players[f"ID{playerid}"]
            stats = player["stats"]["pitching"]

            name = player["person"]["fullName"]
            # pos = player["position"]["abbreviation"]
            pitch_ct = stats.get("pitchesThrown", "")
            ip = float(stats.get("inningsPitched", 0))
            sos = stats.get("strikeOuts", 0)
            bbs = stats.get("baseOnBalls", 0)
            strikes = stats.get("strikes", 0)
            balls = stats.get("balls", 0)
            try:
                strike_perc = float(stats["strikePercentage"])
            except:
                strike_perc = "--"
            runs = stats["runs"]
            ers = stats["earnedRuns"]
            hits = stats["hits"]
            hrs = stats["homeRuns"]
            dbls = stats["doubles"]
            trpls = stats["triples"]
            pkoffs = stats["pickoffs"]
            outs = stats["outs"]
            ibbs = stats["intentionalWalks"]
            hbp = stats["hitByPitch"]
            sbs = stats["stolenBases"]
            wps = stats["wildPitches"]
            batters_faced = stats["battersFaced"]
            era = player["seasonStats"]["pitching"]["era"]

            row_data = [
                name,
                pitch_ct,
                ip,
                runs,
                hits,
                ers,
                sos,
                bbs,
                strikes,
                balls,
                era,
                strike_perc,
                hrs,
                dbls,
                trpls,
                pkoffs,
                outs,
                ibbs,
                hbp,
                sbs,
                wps,
                batters_faced,
                playerid,
            ]
            rows.append(row_data)

        df = pd.DataFrame(data=rows, columns=headers)

        return df

    # TEAMS' INDIVIDUAL FIELDER STATS
    def away_fielding_stats(self) -> pd.DataFrame:
        """
        Get current game fielding stats for players on AWAY team

        * Note: Last entry in dataframe is "Summary"

        Returns:
        ----------
            `pandas.Dataframe`

        Example:
        ----------
        >>> stats = away_fielding_stats()
        >>> print(stats[["Player","Pos","Putouts","Errors"]].head())
                       Player Pos  Putouts  Errors
        0         Akil Baddoo  CF        2       0
        1     Robbie Grossman  LF        1       0
        2     Jonathan Schoop  DH        0       0
        3      Miguel Cabrera  1B        6       0
        4   Jeimer Candelario  3B        0       0
        """
        # These stats will be only for this CURRENT GAME 
        #   (with the exception of a player's fielding average stat)
        tm = self._away_stats["fielding"]
        players = self._away_player_data
        headers = [
            "Player",
            "Pos",
            "Putouts",
            "Assists",
            "Errors",
            "Chances",
            "Stolen Bases",
            "Pickoffs",
            "Passed Balls",
            f"Fld % ({self.game_date[:4]})",
            "mlbam",
        ]
        rows = []
        for playerid in self._away_lineup + self._away_pitcher_lineup:
            player = players[f"ID{playerid}"]
            stats = player["stats"]["fielding"]

            name = player["person"]["fullName"]
            pos = player["position"]["abbreviation"]
            putouts = stats["putOuts"]
            assists = stats["assists"]
            errors = stats["errors"]
            chances = stats["chances"]
            sbs = stats["stolenBases"]
            pkoffs = stats["pickoffs"]
            passedballs = stats["passedBall"]
            field_perc = player["seasonStats"]["fielding"]["fielding"]

            row_data = [
                name,
                pos,
                putouts,
                assists,
                errors,
                chances,
                sbs,
                pkoffs,
                passedballs,
                field_perc,
                playerid,
            ]
            rows.append(row_data)
        summary = [
            "Summary",
            " ",
            tm["putOuts"],
            tm["assists"],
            tm["errors"],
            tm["chances"],
            tm["stolenBases"],
            tm["pickoffs"],
            tm["passedBall"],
            " ",
            " ",
        ]
        rows.append(summary)
        df = pd.DataFrame(data=rows, columns=headers)
        return df

    def home_fielding_stats(self) -> pd.DataFrame:
        """
        Get current game fielding stats for players on HOME team

        * Note: Last entry in dataframe is "Summary"

        Returns:
        ----------
            `pandas.Dataframe`

        Example:
        ----------
        >>> stats = home_fielding_stats()
        >>> print(stats[["Player","Pos","Putouts","Errors"]].head())
                     Player Pos  Putouts  Errors
        0      Tim Anderson  SS        1       0
        1       Luis Robert  CF        4       0
        2        Jose Abreu  1B       10       0
        3   Yasmani Grandal   C        7       0
        4      Eloy Jimenez  LF        2       0
        """
        # These stats will be only for this CURRENT GAME 
        #   (with the exception of a player's fielding average stat)
        tm = self._home_stats["fielding"]
        players = self._home_player_data
        headers = [
            "Player",
            "Pos",
            "Putouts",
            "Assists",
            "Errors",
            "Chances",
            "Stolen Bases",
            "Pickoffs",
            "Passed Balls",
            f"Fld % ({self.game_date[:4]})",
            "mlbam",
        ]
        rows = []
        for playerid in self._home_lineup + self._home_pitcher_lineup:
            player = players[f"ID{playerid}"]
            stats = player["stats"]["fielding"]

            name = player["person"]["fullName"]
            pos = player["position"]["abbreviation"]
            putouts = stats["putOuts"]
            assists = stats["assists"]
            errors = stats["errors"]
            chances = stats["chances"]
            sbs = stats["stolenBases"]
            pkoffs = stats["pickoffs"]
            passedballs = stats["passedBall"]
            field_perc = player["seasonStats"]["fielding"]["fielding"]

            row_data = [
                name,
                pos,
                putouts,
                assists,
                errors,
                chances,
                sbs,
                pkoffs,
                passedballs,
                field_perc,
                playerid,
            ]
            rows.append(row_data)
        summary = [
            "Summary",
            " ",
            tm["putOuts"],
            tm["assists"],
            tm["errors"],
            tm["chances"],
            tm["stolenBases"],
            tm["pickoffs"],
            tm["passedBall"],
            " ",
            " ",
        ]
        rows.append(summary)
        df = pd.DataFrame(data=rows, columns=headers)
        return df

    def timestamps(self) -> pd.DataFrame:
        """Get timestamps for all plays as "timecodes" """
        plays = self._all_plays
        ts = []
        for p in plays:
            play_type = p.get("result", {}).get("type")
            playStartTime = p.get("about", {}).get("startTime")
            playEndTime = p.get("playEndTime", "-")
            playEvents = p.get("playEvents", [])
            # print(playEvents)
            try:
                abIndex = p.get("atBatIndex")
                ab_num = abIndex + 1
            except:
                pass
            for e in playEvents:
                play_id = e.get("playId")
                eventStart = e.get("startTime")
                eventEnd = e.get("endTime")
                event_idx = e.get("index")
                event_num = event_idx + 1
                details = e.get("details", {})
                event_type = details.get("eventType")
                event_type2 = details.get("event")
                event_desc = details.get("description")
                if eventStart is None:
                    start_tc = "-"
                else:
                    start_tc = dt.datetime.strptime(
                        eventStart, r"%Y-%m-%dT%H:%M:%S.%fZ"
                    )
                    start_tc = start_tc.strftime(r"%Y%m%d_%H%M%S")
                if eventEnd is None:
                    end_tc = "-"
                else:
                    end_tc = dt.datetime.strptime(eventEnd, r"%Y-%m-%dT%H:%M:%S.%fZ")
                    end_tc = end_tc.strftime(r"%Y%m%d_%H%M%S")

                ts.append(
                    [
                        abIndex,
                        play_type,
                        # playStartTime,
                        # playEndTime,
                        event_idx,
                        f"{event_type2} ({event_type})",
                        event_desc,
                        eventStart,
                        start_tc,
                        eventEnd,
                        end_tc,
                        play_id,
                    ]
                )
        df = pd.DataFrame(
            data=ts,
            columns=(
                "ab_idx",
                "type",
                "event_idx",
                "event_type",
                "event_desc",
                "event_start",
                "start_tc",
                "event_end",
                "end_tc",
                "play_id",
            ),
        )

        return df

    def flags(self) -> dict:
        f = self._flags
        return f

    def get_content(self):
        url = BASE + f"/game/{self.gamePk}/content"
        resp = requests.get(url)
        return resp.json()

    def raw_feed_data(self):
        """Return the raw JSON data"""
        return self._raw_game_data

    def get_feed_data(self, timecode=None):
        if timecode is not None:
            url = f"https://statsapi.mlb.com/api/v1.1/game/{self.gamePk}/feed/live?timecode={timecode}"
        else:
            url = f"https://statsapi.mlb.com/api/v1.1/game/{self.gamePk}/feed/live"
        resp = requests.get(url)
        return resp.json()

    def context_splits(self, batterID, pitcherID):  
        #  applicable DYNAMIC splits for the current matchup
        """This class method has not been configured yet"""
        pass

    def away_season_stats(self):
        """This method has not been configured yet"""
        pass

    def home_season_stats(self):
        """This method has not been configured yet"""
        pass
    
    gamePk  = game_pk
    game_id = game_pk
    gameId  = game_pk