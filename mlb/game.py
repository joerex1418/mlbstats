import requests
import datetime as dt
from dateutil.parser import parse
from typing import Union, Optional, Dict, List, Literal
from pprint import pprint as pp

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

from .constants import BASE, POSITION_DICT, ORDINALS, STATDICT

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
        
        self.__all_players_game_data = {}
        self.__all_players_game_data.update(self._boxscore['teams']['away']['players'])
        self.__all_players_game_data.update(self._boxscore['teams']['home']['players'])
        
        away_probable_mlbam = gameData.get('probablePitchers',{}).get('away',{}).get('id',0)
        home_probable_mlbam = gameData.get('probablePitchers',{}).get('home',{}).get('id',0)

        _away_probable_bio = self.player_bio(away_probable_mlbam)
        _home_probable_bio = self.player_bio(home_probable_mlbam)
        self.away_probable = person_name_data(mlbam=away_probable_mlbam,
                                              _full=_away_probable_bio.get('fullName'),
                                              _given=_away_probable_bio.get('fullFMLName'),
                                              _first=_away_probable_bio.get('firstName'),
                                              _middle=_away_probable_bio.get('middleName'),
                                              _last=_away_probable_bio.get('lastName'),
                                              )
        
        self.home_probable = person_name_data(mlbam=home_probable_mlbam,
                                              _full=_home_probable_bio.get('fullName'),
                                              _given=_home_probable_bio.get('fullFMLName'),
                                              _first=_home_probable_bio.get('firstName'),
                                              _middle=_home_probable_bio.get('middleName'),
                                              _last=_home_probable_bio.get('lastName'),
                                              )
        

        # AWAY Team Data
        away = gameData['teams']['away']
        self._away_info = away
        _away_data = self._boxscore['teams']['away']
        _away_score_data = self._linescore['teams']['away']
        self.away_id = away['id']
        self._away_team_full = away['name']
        self._away_team = away['clubName']
        self._away_team_abbrv = away['abbreviation']
        self._away_stats = _away_data['teamStats']
        self._away_players = _away_data['players']
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
        
        self.__away = team_name_data(mlbam=away['id'],full=away['name'],short=away['shortName'],
                                     franchise=away['franchiseName'],location=away['locationName'],
                                     club=away['clubName'],season=away['season'],abbreviation=away['abbreviation'])

        # HOME Team Data
        home = gameData['teams']['home']
        self._home_info = home
        _home_data = self._boxscore['teams']['home']
        _home_score_data = self._linescore['teams']['home']
        self.home_id = home['id']
        self._home_team_full = home['name']
        self._home_team = home['clubName']
        self._home_team_abbrv = home['abbreviation']
        self._home_stats = _home_data['teamStats']
        self._home_players = _home_data['players']
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
        
        
        self.__home = team_name_data(mlbam=home['id'],full=home['name'],short=home['shortName'],
                                     franchise=home['franchiseName'],location=home['locationName'],
                                     club=home['clubName'],season=home['season'],abbreviation=home['abbreviation'])

        self._curr_defense = self._linescore['defense']
        self._curr_offense = self._linescore['offense']
        self._curr_play = liveData['plays'].get('currentPlay', {})

        self.__inning = self._linescore.get('currentInning', '-')
        self.__inning_ordinal = self._linescore.get('currentInningOrdinal', '-')
        self.__inning_state = self._linescore.get('inningState', '-')
        
        if self.__inning_state == 'Top' or self.__inning_state == 'Bottom':
            self.balls = self._linescore.get('balls', 0)
            self.strikes = self._linescore.get('strikes', 0)
            self.outs = self._linescore.get('outs', 0)
        else:
            self.balls = 0
            self.strikes = 0
            self.outs = 0

        self._inn_half = self._linescore.get('inningHalf', '-')
        self._inn_label = f'{self._inn_half} of the {self.__inning_ordinal}'
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

        self.__away_player_data  = self.__get_player_data('away')
        self.__home_player_data  = self.__get_player_data('home')

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
    def away(self) -> team_name_data:
        """Away team info"""
        return self.__away

    @property
    def home(self) -> team_name_data:
        """Home team info"""
        return self.__home
    
    @property
    def inning(self) -> str:
        """Current inning as an string formatted integer"""
        return str(self.__inning)

    @property
    def inning_half(self) -> str:
        """Label for the current inning

        "Top", "Bottom"

        """
        return str(self.__inning_half)

    @property
    def inning_state(self) -> str:
        """State of current inning"""
        return str(self.__inning_state)

    @property
    def inning_ordinal(self) -> str:
        """Ordinal display for current inning

        "1st", "2nd", "3rd", etc...

        """
        return str(self.__inning_ordinal)
    
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

        data = {
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
        
        return mlb_wrapper(**data)
     
    def __get_player_data(self,home_or_away,lineup_type=None,**kwargs):
        team_data = self._boxscore['teams'][home_or_away]
        all_player_data = {}
        
        lineup_types = []
        if lineup_type is None:
            lineup_types.append('batters')
            lineup_types.append('pitchers')
            lineup_types.append('bullpen')
            lineup_types.append('bench')
        elif type(lineup_type) is str:
            lineup_types.append(lineup_type)
        
        for lineup_type in lineup_types:
            data = []
            lineup_datalist = team_data.get(lineup_type,[])
            if len(lineup_datalist) > 0:
                for mlbam in lineup_datalist:
                    try:
                        player_bio = self.player_bio(mlbam)
                        player_data = self.player_game_data(mlbam)
                        game_status = player_data.get('gameStatus',{})
                        batting_order = player_data.get('battingOrder','0999')

                        player_stats = self.player_stats(mlbam)
                        name_full = player_bio['fullName']
                        name_box  = player_bio['lastInitName']
                        try:
                            all_positions = '|'.join(list([pos['abbreviation'] for pos in player_data['allPositions']]))
                        except:
                            all_positions = ''
                        
                        for scope in ['game','season']:
                            for group in ['batting','pitching','fielding']:
                                player_stats[scope][group] = pd.Series(player_stats[scope][group]).rename(STATDICT).to_dict()
                        
                        p_data = {'mlbam':mlbam,'name_full':name_full,'name_box':name_box,
                                    'pos':all_positions,'order':batting_order,
                                    'is_batting':game_status.get('isCurrentBatter',False),
                                    'is_pitching':game_status.get('isCurrentPitcher',False),
                                    'is_on_bench':game_status.get('isOnBench',False),
                                    'is_sub':game_status.get('isSubstitute',False),
                                    'batting':{'game':player_stats['game']['batting'],
                                            'season':player_stats['season']['batting']},
                                    'pitching':{'game':player_stats['game']['pitching'],
                                                'season':player_stats['season']['pitching']},
                                    'fielding':{'game':player_stats['game']['fielding'],
                                                'season':player_stats['season']['fielding']},
                                    }

                        data.append(p_data)
                    except:
                        pass
                    
                df = pd.DataFrame.from_dict(data=data).sort_values(by='order')
            else:
                df = pd.DataFrame(data=[],columns=self.__generate_pdata_list())
                
            all_player_data[lineup_type] = df
        
        return all_player_data

    def __get_team_stats(self):
        pass

    def away_team_stats(self):
        """Away team stats for game and current season"""
        batting = self._away_stats['batting']
        batting = pd.Series(batting).rename(STATDICT).to_dict()
        
        pitching = self._away_stats['pitching']
        pitching = pd.Series(pitching).rename(STATDICT).to_dict()
        
        fielding = self._away_stats['fielding']
        fielding = pd.Series(fielding).rename(STATDICT).to_dict()
        
        return {'batting':batting,'pitching':pitching,'fielding':fielding}
        
    def home_team_stats(self):
        """Home team stats for game and current season"""
        batting = self._home_stats['batting']
        batting = pd.Series(batting).rename(STATDICT).to_dict()
        
        pitching = self._home_stats['pitching']
        pitching = pd.Series(pitching).rename(STATDICT).to_dict()
        
        fielding = self._home_stats['fielding']
        fielding = pd.Series(fielding).rename(STATDICT).to_dict()
        
        return {'batting':batting,'pitching':pitching,'fielding':fielding}
        
    @property
    def away_batters(self,default_index=None) -> pd.DataFrame:
        """Away batters game/season stats, bio, and game status"""
        if default_index is None:
            return self.__away_player_data['batters']
        else:
            return self.__away_player_data['batters'].set_index(default_index)
    
    @property
    def away_pitchers(self,default_index=None) -> pd.DataFrame:
        """Away pitchers game/season stats, bio, and game status"""
        if default_index is None:
            return self.__away_player_data['pitchers']
        else:
            return self.__away_player_data['pitchers'].set_index(default_index)
    
    @property
    def away_bullpen(self,default_index=None) -> pd.DataFrame:
        """Away bullpen game/season stats, bio, and game status"""
        if default_index is None:
            return self.__away_player_data['bullpen']
        else:
            return self.__away_player_data['bullpen'].set_index(default_index)
    
    @property
    def away_bench(self,default_index=None) -> pd.DataFrame:
        """Away bench game/season stats, bio, and game status"""
        if default_index is None:
            return self.__away_player_data['bench']
        else:
            return self.__away_player_data['bench'].set_index(default_index)
    
    @property
    def home_batters(self,default_index=None) -> pd.DataFrame:
        """Home batters game/season stats, bio, and game status"""
        if default_index is None:
            return self.__home_player_data['batters']
        else:
            return self.__home_player_data['batters'].set_index(default_index)
    
    @property
    def home_pitchers(self,default_index=None) -> pd.DataFrame:
        """Home pitchers game/season stats, bio, and game status"""
        if default_index is None:
            return self.__home_player_data['pitchers']
        else:
            return self.__home_player_data['pitchers'].set_index(default_index)
    
    @property
    def home_bullpen(self,default_index=None) -> pd.DataFrame:
        """Home bullpen game/season stats, bio, and game status"""
        if default_index is None:
            return self.__home_player_data['bullpen']
        else:
            return self.__home_player_data['bullpen'].set_index(default_index)
    
    @property
    def home_bench(self,default_index=None) -> pd.DataFrame:
        """Home bench game/season stats, bio, and game status"""
        if default_index is None:
            return self.__home_player_data['bench']
        else:
            return self.__home_player_data['bench'].set_index(default_index)

    def player_bio(self,mlbam) -> dict:
        """Get bio information for a specific player
        
        Parameters:
        -----------
        mlbam : int | str
            Player's unique MLB Advanced Media ID
        
        """
        
        return self._players.get(f'ID{mlbam}',{})
        
    def player_stats(self,mlbam) -> dict:
        """Get game and season stats for a specific player
        
        Parameters:
        -----------
        mlbam : int | str
            Player's unique MLB Advanced Media ID
        
        """
        
        away_data = self._boxscore['teams']['away']['players']
        home_data = self._boxscore['teams']['home']['players']
        
        all_player_data = {}
        all_player_data.update(away_data)
        all_player_data.update(home_data)
        
        player_data = all_player_data.get(f'ID{mlbam}',{})
        player_stats = {'game':player_data.get('stats',{}),
                        'season':player_data.get('seasonStats',{})}
        return player_stats

    def player_game_data(self,mlbam) -> dict:
        """Get player meta data for the game"""
        all_player_data = self.__all_players_game_data
        player = all_player_data.get(f'ID{mlbam}')
        return player

    def linescore(self) -> dict:
        """
        Returns a tuple of game's current linescore
        """

        ls = self._linescore
        ls_total = {
            'away': {
                'runs': ls.get('teams', {}).get('away', {}).get('runs', '-'),
                'hits': ls.get('teams', {}).get('away', {}).get('hits', '-'),
                'errors': ls.get('teams', {}).get('away', {}).get('errors', '-'),
            },
            'home': {
                'runs': ls.get('teams', {}).get('home', {}).get('runs', '-'),
                'hits': ls.get('teams', {}).get('home', {}).get('hits', '-'),
                'errors': ls.get('teams', {}).get('home', {}).get('errors', '-'),
            },
        }

        ls_inns = []
        ls_innings_array = ls['innings']
        for inn in ls_innings_array:
            ls_inns.append(
                {
                    'away': {
                        'runs': inn.get('away', {}).get('runs', '-'),
                        'hits': inn.get('away', {}).get('hits', '-'),
                        'errors': inn.get('away', {}).get('errors', '-'),
                    },
                    'home': {
                        'runs': inn.get('home', {}).get('runs', '-'),
                        'hits': inn.get('home', {}).get('hits', '-'),
                        'errors': inn.get('home', {}).get('errors', '-'),
                    },
                    'inning': inn.get('num', '-'),
                    'inningOrdinal': ORDINALS[str(inn.get('num', '-'))],
                }
            )
        
        if  0 < len(ls_innings_array) < self.scheduled_innings:
            most_recent_inn = int(ls_inns[-1]['inning'])
            inns_til_9 = self.scheduled_innings - len(ls_innings_array)
            rem_innings = list(range(inns_til_9))
            for inn in rem_innings:
                next_inn = most_recent_inn + inn + 1
                ls_inns.append(
                    {
                        'away': {'runs': '-', 'hits': '-', 'errors': '-'},
                        'home': {'runs': '-', 'hits': '-', 'errors': '-'},
                        'inning': str(next_inn),
                        'inningOrdinal': ORDINALS[str(next_inn)],
                    }
                )
        return {'total': ls_total, 'innings': ls_inns, 'away': {}, 'home': {}}

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
        * `batting`: current batter (dict)
        * `pitching`: current pitcher (dict)
        * `zone`: current batter's strike zone metrics (tuple)
        """
        matchup  = self._curr_play.get('matchup',{})
        batter = matchup.get('batter',{})
        pitcher = matchup.get('pitcher',{})
        
        batting = {'name': batter.get('fullName','-'),
                   'id': batter.get('id','-'),
                   'bats': matchup.get('batSide',{}).get('code','R'),
                   'zone_top': self._players[f'ID{batter.get("id")}'].get('strikeZoneTop',3.5),
                   'zone_bot': self._players[f'ID{batter.get("id")}'].get('strikeZoneBottom',1.5),
                   'stands': matchup.get('batSide',{}).get('code','R'),
                 }
        pitching = {'name': pitcher.get('fullName','-'),
                    'id': pitcher.get('id','-'),
                    'throws': matchup.get('pitchHand',{}).get('code','R'),}

        return {'batting': batting, 'pitching': pitching, 'zone': (3.5, 1.5)}

    def matchup_event_log(self) -> pd.DataFrame:
        """Gets a pitch-by-pitch log of the current batter-pitcher matchup:

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
            'pitch_num',
            'details',
            'zone_top',
            'zone_bot',
            'zoneTopInitial',
            'zoneBottomInitial',
            'bat_side',
            'pitch_type',
            'pitch_type_id',
            'pitch_code',
            'release_speed',
            'end_speed',
            'spin',
            'zone',
            'pX',
            'pZ',
            'hit_location',
            'hX',
            'hY',
            'play_id',
        ]
        try:
            pa_events = self._curr_play['playEvents']
            matchup = self._curr_play.get('matchup',{})
            bat_side = matchup.get('batSide',{}).get('code','R')
        except:
            return pd.DataFrame(columns=headers)

        events_data = []

        for ab_log in pa_events:
            play_id = ab_log.get('playId')
            if ab_log['isPitch'] == False:
                pass
            else:
                event = []

                pitch_number = ab_log['pitchNumber']

                details = ab_log['details']
                pitch_desc = details['description']
                pitch_type = details.get('type',{}).get('description','unknown')
                pitch_type_id = details.get('type',{}).get('code','UN')
                pitch_code = details.get('code','UN')

                _pitch_data = ab_log.get('pitchData',{})
                _hit_data = ab_log.get('hitData',{})

                start_vel = _pitch_data.get('startSpeed','--')
                end_vel = _pitch_data.get('endSpeed','--')

                pX = _pitch_data.get('coordinates',{}).get('pX','--')
                pZ = _pitch_data.get('coordinates',{}).get('pZ','--')

                try:
                    zoneTopInitial = pa_events[0]['pitchData']['strikeZoneTop']
                    zoneBottomInitial = pa_events[0]['pitchData']['strikeZoneBottom']
                except:
                    try:
                        zoneTopInitial = pa_events[0]['pitchData']['strikeZoneTop']
                        zoneBottomInitial = pa_events[0]['pitchData']['strikeZoneBottom']
                    except:
                        zoneTopInitial = 3.5
                        zoneBottomInitial = 1.5

                zone_top = _pitch_data.get('strikeZoneTop',3.5)
                zone_bot = _pitch_data.get('strikeZoneBottom',1.5)

                spin = _pitch_data.get('breaks',{}).get('spinRate','')
                zone = _pitch_data.get('zone','')
                hit_location = _hit_data.get('hitData',{}).get('location','')

                hX = _hit_data.get('coordinates',{}).get('coordX','')
                hY = _hit_data.get('coordinates',{}).get('coordY','')
                
                events_data.append({
                    'pitch_num':pitch_number,
                    'details':pitch_desc,
                    'zone_top':zone_top,
                    'zone_bot':zone_bot,
                    'zoneTopInitial':zoneTopInitial,
                    'zoneBottomInitial':zoneBottomInitial,
                    'bat_side':bat_side,
                    'pitch_type':pitch_type,
                    'pitch_type_id':pitch_type_id,
                    'pitch_code':pitch_code,
                    'release_speed':start_vel,
                    'end_speed':end_vel,
                    'spin':spin,
                    'zone':zone,
                    'pX':pX,
                    'pZ':pZ,
                    'hit_location':hit_location,
                    'hX':hX,
                    'hY':hY,
                    'play_id':play_id
                })

        matchup_df = pd.DataFrame.from_dict(data=events_data)
        if matchup_df.empty:
            matchup_df = pd.DataFrame(columns=headers)
        
        matchup_df.sort_values(by=["pitch_num"], inplace=True)

        return matchup_df

    def plays(self) -> pd.DataFrame:
        """
        Get detailed log of each plate appearance in game

        NOTE: Dataframe begins with most recent plate appearance
        """

        events_data = []
        for play in self._all_plays:
            ev_data = {}
            events = play['playEvents']
            for e in events:
                if 'game_advisory' in e.get('details', {}).get('eventType', '').lower():
                    pass
                else:
                    firstEvent = e
                    break
                
            # Sometimes, the 'playEvents' array isn't populated for a few seconds; 
            # So we skip over it for the time being
            if len(events) == 0:
                continue
            _play_matchup = play.get('matchup',{})
            lastEvent = events[-1]
            play_id = lastEvent.get("playId", "-")
            pitchData = lastEvent.get('pitchData',{})
            try:
                ab_num = play["atBatIndex"] + 1
            except:
                ab_num = "--"
            
            bat_side = _play_matchup.get('batSide',{}).get('code','--')

            innNum = play.get('about',{}).get('inning','--')
            innHalf = play.get('about',{}).get('halfInning','--')

            if innHalf == "bottom":
                inning = f"Bot {innNum}"
            else:
                inning = f"Top {innNum}"

            batter = _play_matchup.get('batter',{})
            batter_name = batter.get('fullName','--')
            batter_mlbam = batter.get('id','--')
            
            pitcher = _play_matchup.get('pitcher',{})
            pitcher_name = pitcher.get('fullName','--')
            pitcher_mlbam = pitcher.get('id','--')
            
            pa_pitch_count = lastEvent.get('pitchNumber','--')

            _play_result = play.get('result',{})
            event = _play_result.get('event','--')
            event_type = _play_result.get('eventType','--')
            details = _play_result.get('description','--')

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
            _hitData = lastEvent.get('hitData',{})
            launch_speed = _hitData.get('launchSpeed','-')
            launch_angle = _hitData.get('launchAngle','-')
            distance = _hitData.get('totalDistance','-')
            location = _hitData.get('location','-')
            hX = _hitData.get('coordinates',{}).get('coordX','-')
            hY = _hitData.get('coordinates',{}).get('coordY','-')
            hitTrajectory = _hitData.get('trajectory','-')

            # Event Category/Type
            category = lastEvent.get('type','-')

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
            if f"ID{batter_mlbam}" in self._home_players.keys():
                is_home = True
                bat_tm_mlbam = self.home_id
                bat_tm_name = self._home_team
            else:
                is_home = False
                bat_tm_mlbam = self.away_id
                bat_tm_name = self._away_team

            ev_data = {'bat_tm_mlbam':bat_tm_mlbam,
                        'bat_tm_name':bat_tm_name,
                        'pa':ab_num,
                        'inning':inning,
                        'batter':batter_name,
                        'bat_side':bat_side,
                        'pitcher':pitcher_name,
                        'pa_pitch_count':pa_pitch_count,
                        'event':event,
                        'event_type':event_type,
                        'details':details,
                        'pitch_type':pitchType,
                        'pitch_code':pitchCode,
                        'release_velocity':releaseSpeed,
                        'end_velocity':endSpeed,
                        'spin_rate':spinRate,
                        'zone':zone,
                        'exit_velocity':launch_speed,
                        'launch_angle':launch_angle,
                        'distance':distance,
                        'location':location,
                        'hit_trajectory':hitTrajectory,
                        'hX':hX,
                        'hY':hY,
                        'category':category,
                        'timeElasped':prettify_time(elasped),
                        'timeStart':prettify_time(startTime),
                        'timeEnd':prettify_time(endTime),
                        'batter_mlbam':batter_mlbam,
                        'pitcher_mlbam':pitcher_mlbam,
                        'is_home':is_home,
                        'play_id':play_id,
                        }

            events_data.append(ev_data)

        df = pd.DataFrame.from_dict(data=events_data).sort_values(by='pa',
                                                              ascending=False)

        return df

    def events(self) -> pd.DataFrame:
        """Get detailed log of every pitch event

        NOTE: Dataframe begins with most recent pitch event
        """
        
        events_data = []
        for play in self._all_plays:
            
            try:
                ab_num = play['about']['atBatIndex'] + 1
            except:
                ab_num = '--'
            
            matchup = play.get('matchup',{'batter':{},'pitcher':{}})
            
            batter = matchup['batter']
            batter_name = batter.get('fullName','--')
            batter_mlbam = batter.get('id','--')
            
            pitcher = matchup['pitcher']
            pitcher_name = pitcher.get('fullName','--')
            pitcher_mlbam = pitcher.get('id','--')

            zone_top = self._players.get(f'ID{batter_mlbam}',{}
                                         ).get('strikeZoneTop','-')
            zone_bot = self._players.get(f'ID{batter_mlbam}',{}
                                         ).get('strikeZoneBottom','-')
                
            bat_side = matchup.get('batSide',{}).get('code')
                
            play_events = play['playEvents']
            if len(play_events) == 0:
                continue
            last_idx = play_events[-1]['index']

            for event_idx, event in enumerate(play_events):
                play_id = event.get('playId')
                try:
                    if event_idx != last_idx:
                        desc = '--'
                    else:
                        desc = play['result']['description']
                except:
                    desc = '--'

                event_label = play.get('result',{}).get('event','--')
                event_type  = play.get('result',{}).get('eventType','--')
                pitch_number = event.get('pitchNumber',0)

                # Times
                try:
                    startTime = dt.datetime.strptime(
                        event['startTime'], iso_format_ms
                    ).replace(tzinfo=utc_zone)
                except:
                    startTime = '--'
                try:
                    endTime = dt.datetime.strptime(
                        event['endTime'], iso_format_ms
                    ).replace(tzinfo=utc_zone)
                except:
                    endTime = '--'
                try:
                    elapsed = endTime - startTime
                    elapsed = prettify_time(elapsed)
                except:
                    elapsed = '--'

                # Call and Pitch Type and Count/Outs
                _event_details = event['details']
                pitchType = _event_details.get('type',{}).get('description','--')
                pitchCode = _event_details.get('type',{}).get('code','--')
                call = _event_details.get('description','--')
                
                _count_data = event.get('count',{})
                balls = _count_data.get('balls',0)
                strikes = _count_data.get('strikes',0)
                count = f'{balls}-{strikes}'
                outs = _count_data.get('outs',0)


                # Pitch Data
                _pitchData = event.get('pitchData',{})
                
                startSpeed = _pitchData.get('startSpeed','-')
                endSpeed   = _pitchData.get('endSpeed','-')
                spinRate   = _pitchData.get('breaks',{}).get('spinRate','-')
                zone = _pitchData.get('zone','-')

                pX = _pitchData.get('coordinates',{}).get('pX','-')
                pZ = _pitchData.get('coordinates',{}).get('pZ','-')

                zone_top = _pitchData.get('strikeZoneTope',3.5)
                zone_bot = _pitchData.get('strikeZoneTope',1.5)

                # Hit Data
                _hitData = event.get('hitData',{})
                launch_speed = _hitData.get('launchSpeed','-')
                launch_angle = _hitData.get('launchAngle','-')
                distance = _hitData.get('totalDistance','-')
                location = _hitData.get('location','-')
                hX = _hitData.get('coordinates',{}).get('coordX','-')
                hY = _hitData.get('coordinates',{}).get('coordY','-')

                category = event.get('type','--')

                # Is Home Team Batting?
                if f'ID{batter["id"]}' in self._home_players.keys():
                    is_home = True
                    bat_tm_mlbam = self.home_id
                    bat_tm_name = self._home_team
                else:
                    is_home = False
                    bat_tm_mlbam = self.away_id
                    bat_tm_name = self._away_team
                    
                ev_data = {'bat_tm_mlbam':bat_tm_mlbam,
                            'bat_tm_name':bat_tm_name,
                            'pa':ab_num,
                            'event':event_label,
                            'event_type':event_type,
                            'inning':play['about']['inning'],
                            'pitch_idx':event_idx,
                            'batter':batter_name,
                            'batter_mlbam':batter_mlbam,
                            'bat_side':bat_side,
                            'zone_top':zone_top,
                            'zone_bot':zone_bot,
                            'pitcher':pitcher_name,
                            'pitcher_mlbam':pitcher_mlbam,
                            'desc':desc,
                            'pitch_num':pitch_number,
                            'pitch_type':pitchType,
                            'pitch_code':pitchCode,
                            'call':call,
                            'count':count,
                            'outs':outs,
                            'release_velocity':startSpeed,
                            'end_velocity':endSpeed,
                            'spin_rate':spinRate,
                            'zone':zone,
                            'pX':pX,
                            'pZ':pZ,
                            'exit_velocity':launch_speed,
                            'launch_angle':launch_angle,
                            'distance':distance,
                            'location':location,
                            'hX':hX,
                            'hY':hY,
                            'category':category,
                            'end_time':endTime,
                            'is_home':is_home,
                            'play_id':play_id,
                            }

                events_data.append(ev_data)

        df = pd.DataFrame.from_dict(events_data).sort_values(
            by=["pa", "pitch_idx"], ascending=False)
        
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
            players = self._away_players
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
            players = self._home_players
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
        players = self._away_players
        headers = [
            'Player',
            'Ct',
            'IP',
            'R',
            'H',
            'ER',
            'SO',
            'BB',
            'K',
            'B',
            f'ERA ({self.game_date[:4]})',
            'Strike %',
            'HR',
            '2B',
            '3B',
            'PkOffs',
            'Outs',
            'IBB',
            'HBP',
            'SB',
            'WP',
            'BF',
            'mlbam',
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
        players = self._home_players
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
        players = self._away_players
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
        players = self._home_players
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
        url = BASE + f'/game/{self.gamePk}/content'
        resp = requests.get(url)
        return resp.json()

    def raw_feed_data(self):
        """Return the raw JSON data"""
        return self._raw_game_data

    def get_feed_data(self, timecode=None):
        if timecode is not None:
            url = f'https://statsapi.mlb.com/api/v1.1/game/{self.gamePk}/feed/live?timecode={timecode}'
        else:
            url = f'https://statsapi.mlb.com/api/v1.1/game/{self.gamePk}/feed/live'
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
    
    def __generate_pdata_list(self):
        new_list = ['mlbam','name_full','name_box','pos','order',
                    'is_batting','is_pitching','is_on_bench','is_sub',
                    'batting','pitching','fielding']
        return new_list
    
    gamePk  = game_pk
    game_id = game_pk
    gameId  = game_pk