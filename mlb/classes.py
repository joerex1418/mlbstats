import requests
import pandas as pd
import datetime as dt

from .functions import _team_data
from .functions import _franchise_data
from .functions import _player_data
from .functions import fetch as _fetch
from .functions import find_player

from .helpers import mlb_date as md
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
from .helpers import stats_roster
from .helpers import stats_totals

from .helpers import _teams_data_collection, _people_data_collection
from .helpers import _parse_team,_parse_league,_parse_division,_parse_person,_parse_venue
from .helpers import mlb_team, mlb_person


from .constants import BASE, POSITION_DICT, ORDINALS

# from .utils import curr_year
from .utils import iso_format_ms
from .utils import utc_zone
from .utils import prettify_time
from .utils import default_season

from typing import Union, Optional, Dict, List, Literal


class Person:
    """# Person
    
        The "mlb.Person" class is a comprehensive wrapper for many different types of data for a single person.
    All of the data (with the exception of content in the "bio" attribute/property) is pulled from the Official MLB 
    Stats API (https://statsapi.mlb.com/api/v1) The attributes intended for end-user retrieval have been made into
    read-only property attributes.
    
    Parameters:
    -----------
    mlbam : int (required)
        Official "MLB Advanced Media" ID for a player
    
    -------------------------------------------
    Accessing the data
    ------------------
        Data can be accessed by using standard dot-notation (e.g. `person.name`) or by treating the class as a
    subscriptable python dictionary (e.g. `person["name"]`).
    
        Many of the properties are subscriptable wrappers for different types of data (See examples below).
    For convenience and where it is applicable, if the wrapper is not accessing a child attribute, a string representation
    of the object is returned. Additionally, some wrappers (like 'mlb_date') are callable and will return a non-string
    object. For example, `person.birth.date` returns a date string ('YYYY-mm-dd') while `person.birth.date()` returns a datetime.date object
    
    Examples
    --------
    ```
    import mlb
    
    >>> abreu = mlb.Person(547989)
    
    # Player's given name
    >>> abreu.name.given
    'Jose Dariel Abreu'
    
    # Player's birth date as a datetime object
    >>> abreu.birth.date.obj
    datetime.date(1987, 1, 29)
    
    # ...or as a date string (long format)
    >>> abreu.birth.date.full
    'January 29, 1987'
    
    # Obtaining birthdate by subscripting
    >>> abreu['birth']['date']['full']
    'January 29, 1987'
    
    ```
    
    """
    
    @classmethod
    def from_search(cls,query:str):
        try:
            mlbam = int(find_player(query).iloc[0]['mlbam'])
            return cls(mlbam)
        except:
            print(Exception(cls,'Player not found'))
            return None
    
    def __new__(cls,mlbam:int,**kwargs):
        self = object.__new__(cls)
        _pd_df = pd.DataFrame
        data = _player_data(mlbam)
        
        _bio  : Union[list,None]  = data['bio']
        _info : dict       = data['info']
        _trx  : _pd_df     = data['transactions']
        _stats  : dict     = data['stats']
        _teams  : dict     = data['teams']
        _awards : _pd_df   = data['awards']
        _past_teams : pd.DataFrame = data['past_teams']
        
        # name info
        _name_data = {
            '_given':_info['givenName'],
            '_full':_info['fullName'],
            '_first':_info['firstName'],
            '_middle':_info['middleName'],
            '_last':_info['lastName'],
            '_nick':_info['nickName'],
            '_pronunciation':_info['pronunciation'],
        }

        # position info
        pos = _info['primary_position']
        _position_data = {
            '_code':pos.get('code','-'),
            '_name':pos.get('name','-'),
            '_type':pos.get('type','-'),
            '_abbrv':pos.get('abbreviation','-'),
            '_abbreviation':pos.get('abbreviation','-'),
        }
        
        # birth info
        _birth_data = {
            'date':  md(_info['birthDate']),
            'city':  _info['birthCity'],
            'state': _info['birthState'],
            'province':_info['birthState'],
            'country' :_info['birthCountry']
        }

        # death info
        _death_data = {
            'date':  md(_info['deathDate']),
            'city':  _info['deathCity'],
            'state': _info['deathState'],
            'province':_info['deathState'],
            'country' :_info['deathCountry']
        }
        
        # debut game info
        _deb : list = _info['debut_data']
        
        if len(_deb) > 0:
            gm_data = _deb[0]['splits'][0]
            team = gm_data.get("team",{})
            opponent = gm_data.get("opponent",{})
            gamepk = gm_data['game']['gamePk']
            _debut_data = {
                'date': md(_info['first_game']),
                'gamepk':gamepk,
                'game_id':gamepk,
                'game_pk':gamepk,
                'gamePk':gamepk,
                'team': team_name_data(
                    _full=team.get("name"),
                    _mlbam=team.get("id"),
                    _short=team.get("shortName"),
                    _club=team.get("clubName"),
                    _location=team.get("locationName"),
                    _franchise=team.get("franchiseName"),
                    _season=team.get("season"),
                    _slug=f"{team.get('clubName').lower().replace(' ','-')}-{team.get('id')}",
                    ),
                'opponent': team_name_data(
                    _full=opponent.get("name"),
                    _mlbam=opponent.get("id"),
                    _short=opponent.get("shortName"),
                    _club=opponent.get("clubName"),
                    _location=opponent.get("locationName"),
                    _franchise=opponent.get("franchiseName"),
                    _season=opponent.get("season"),
                    _slug=f"{opponent.get('clubName').lower().replace(' ','-')}-{opponent.get('id')}",
                    ),
                }
        else:
            _debut_data = {'date':md(data['first_game'])}
        
        # last game info
        _last_game_data = {
            'date':md(data.get('lastPlayedDate','-')),
        }
        
        self._mlbam      = _info['mlbam']
        self._bio        = _bio
        self._name       = person_name_data(**_name_data)
        self._position   = mlb_wrapper(**_position_data)
        self._birth      = mlb_wrapper(**_birth_data)
        self._death      = mlb_wrapper(**_death_data)
        self._debut      = mlb_wrapper(**_debut_data)
        self._last_game  = mlb_wrapper(**_last_game_data)
        
        self._zone_top  = _info['zoneTop']
        self._zone_bot  = _info['zoneBot']
        
        # transaction df
        self._trx = _trx
        
        # awards df
        self._awards = _awards
        
        # stats info
        self._stats = player_stats(
            hit_car_reg=_stats['hitting']['career'],hit_car_adv=_stats['hitting']['career_advanced'],
            pit_car_reg=_stats['pitching']['career'],pit_car_adv=_stats['pitching']['career_advanced'],
            fld_car_reg=_stats['fielding']['career'],
            
            hit_yby_reg=_stats['hitting']['yby'],hit_yby_adv=_stats['hitting']['yby_advanced'],
            pit_yby_reg=_stats['pitching']['yby'],pit_yby_adv=_stats['pitching']['yby_advanced'],
            fld_yby_reg=_stats['fielding']['yby'],
        )
        
        # roster entries
        self._roster_entries = _info['roster_entries']
        
        _past_teams_data = []
        for idx in range(len(_past_teams)):
            t = _past_teams.iloc[idx]
            mlb_tm = mlb_team( raw_data=t.to_dict(),
                    mlbam=t['mlbam'],
                    full=t['full'],
                    season=t['season'],
                    location=t['location'],
                    franchise=t['franchise'],
                    mascot=t['mascot'],
                    club=t['club'],
                    short=t['short'],
                    lg_mlbam=t['lg_mlbam'],
                    lg_name_full=t['lg_name_full'],
                    lg_name_short=t['lg_name_short'],
                    lg_abbrv=t['lg_abbrv'],
                    div_mlbam=t['div_mlbam'],
                    div_name_full=t['div_name_full'],
                    div_name_short=t['div_name_short'],
                    div_abbrv=t['div_abbrv'],
                    venue_mlbam=t['venue_mlbam'],
                    venue_name=t['venue_name']
                    )
            _past_teams_data.append(mlb_tm)
        self._past_teams = pd.DataFrame.from_dict(_past_teams)
        
        # education info
        edu : pd.DataFrame = _info['education']
        
        self._edu = edu_wrapper(edu_df=edu)
        
        return self
    
    def __str__(self):
        return self._name.full
    
    def __repr__(self):
        return f'({self._name.full}, {self._mlbam})'
    
    def __len__(self) -> int:
        max_ = 0
        for grp in (self.stats.hitting,self.stats.pitching,self.stats.fielding):
            try:
                ct = len(set(grp.yby.regular['season']))
                if ct > max_:
                    max_ = ct
            except:
                pass
        return max_
    
    def __getitem__(self, _name: str):
        return self.__dict__[f'_{_name}']

    @property
    def mlbam(self) -> int:
        """Player's official \"MLB Advanced Media\" ID """
        return int(self._mlbam)
    
    @property
    def name(self) -> person_name_data:
        """Name variations for player"""
        return self._name
    @property
    def bio(self):
        """Player bio from player Bullpen Page by 'Baseball-Reference'"""
        return self._bio
    
    @property
    def birth(self) -> md:
        """Details of player's birth
        
        Keys/Attributes:
        ----------------
        - 'date' : mlb_date
        - 'city' : str
        - 'state' (or 'province') : str
        - 'country' : str

        """
        return self._birth
    
    @property
    def death(self):
        """Details of player's death (if applicable)
        
        Keys/Attributes:
        ----------------
        - 'date' : mlb_date
        - 'city' : str
        - 'state' (or 'province') : str
        - 'country' : str
        """
        return self._death
    
    @property
    def position(self):
        """Wrapper for player's primary position
        
        Keys/Attributes:
        ----------------
        - 'code' : int (3)
        - 'name' : str ('First Base')
        - 'type' (or 'province') : str ('Infielder')
        - 'abbreviation' : str ('1B')
        """
        return self._position
    
    @property
    def stats(self) -> player_stats:
        """Player stats
        
        Stats Groups
        ------------
        - hitting
        - pitching
        - fielding
        
        Stats Categories
        ----------------
        - yby.regular
        - yby.advanced
        - career.regular
        - career.advanced
        
        Note:
        -----
            The official API differentiates two subtypes of each stat type. For example, the 'career' stat type retrieves *standard* statistics of a player's entire MLB career. 'careerAdvanced' retrieves more advanced statistics of the same stat type (some stat keys may overlap).
            
            For this reason, a "subtype" must be specified when retrieving a dataframe of statistics using '.regular' or '.advanced' ('reg' | 'adv' for short hand). See example below.
            
            Please Note that the "fielding" stat group does not have "advanced" stat data. `fielding.yby.advanced` returns standard stat data.
        
        Examples
        ---------
        ```
        import mlb
        
        >>> abreu = mlb.Person(547989)
        # Get player's regular/standard career stats
        >>> abreu.stats.hitting.yby.regular
        # OR
        >>> abreu.stats.hitting.yby.reg
        
        # Get player's advanced career stats
        >>> abreu.stats.hitting.yby.advanced
        # OR
        >>> abreu.stats.hitting.yby.adv
        ```
        
        """
        return self._stats
    
    @property
    def transactions(self) -> Union[pd.DataFrame,None]:
        """Dataframe of transactions involving person, i.e. trades, status changes, etc."""
        return self._trx
    
    @property
    def trx(self) -> transactions:
        """Dataframe of transactions involving person, i.e. trades, status changes, etc.
        
        ALIAS for 'transactions'
        """
        return self._trx
    
    @property
    def roster_entries(self):
        """Data for player's recorded roster entries"""
        return self._roster_entries
    
    @property
    def awards(self):
        """Awards received by person"""
        return self._awards

    @property
    def teams(self) -> _teams_data_collection:
        """Teams played for"""
        return self._past_teams
    
    @property
    def debut_game(self):
        """Data for player's debut game"""
        return self._debut
    
    @property
    def last_game(self):
        """Data for player's last/most recent game"""
        return self._last_game
    
    @property
    def zone_top(self):
        return self._zone_top
    
    @property
    def zone_bot(self):
        return self._zone_bot
    
    @property
    def education(self) -> edu_wrapper:
        """Player's education information
        
        Keys:
        -----
        - education.highschool
        - edu.college

        """
        return self._edu
    
    @property
    def edu(self) -> edu_wrapper:
        """Player's education information
        
        Keys:
        -----
        - education.highschool
        - education.college

        ALIAS for 'education'
        """
        return self._edu


class Franchise:
    """# Franchise
    
        The "mlb.Franchise" class is a comprehensive wrapper for retrieving and accesing data for a team's franchise in bulk fashion.
    All data is pulled from the Official MLB Stats API (https://statsapi.mlb.com/api/v1) The attributes intended for end-user 
    retrieval have been made into read-only property attributes.
    
    Parameters
    ----------

    mlbam : int | str (required)
        Official "MLB Advanced Media" ID for franchise (team)

    ### See also:
    - 'mlb.Team()'
    - 'mlb.teams()'
    
    
    """
    def __init__(self,mlbam:int):
        data = _franchise_data(int(mlbam))

        records         = data["records"]
        record_splits   = data["record_splits"] # like standings splits
        yby_data        = data["yby_data"]
        team_info       = data["team_info"]
        hitting         = data["hitting"]
        hitting_adv     = data["hitting_advanced"]
        pitching        = data["pitching"]
        pitching_adv    = data["pitching_advanced"]
        fielding        = data["fielding"]
        roster          = data["all_players"]
        hof             = data["hof"]
        retired         = data["retired_numbers"]
        
        ti = team_info
        self.__mlbam = ti['mlbam']
        self.__franchise = ti['franchise_name']
        self.__name = mlb_wrapper(
            full=ti['full_name'],
            location=ti['location_name'],
            franchise=self.__franchise,
            mascot=ti['team_name'],
            club=ti['club_name'],
            short=ti['short_name']
            )
        
        self.__league : league_name_wrapper = league_name_wrapper(
            _mlbam=ti['league_mlbam'],
            _name=ti['league_name'],
            _short=ti['league_short'],
            _abbrv=ti['league_abbrv']
        )
        self.__division : league_name_wrapper = league_name_wrapper(
            _mlbam=ti['div_mlbam'],
            _name=ti['div_name'],
            _short=ti['div_short'],
            _abbrv=ti['div_abbrv']
        )
        self.__venue : venue_name_wrapper = venue_name_wrapper(
            _name=ti['venue_name'],
            _mlbam=ti['venue_mlbam']
        )
        self.__standings = standings_wrapper(
            records=records,
            splits=record_splits
            )

        self.__yby_data = yby_data

        self.__hitting = mlb_wrapper(reg=hitting,adv=hitting_adv)
        self.__pitching = mlb_wrapper(reg=pitching,adv=pitching_adv)
        self.__fielding = mlb_wrapper(reg=fielding)

        
        self.__stats = mlb_wrapper(
            hitting=self.__hitting,
            pitching=self.__pitching,
            fielding=self.__fielding
        )

        self.__legends : pd.DataFrame = hof
        self.__retired : pd.DataFrame = retired

        self.__roster = roster_wrapper(
            all         = roster,
            pitcher     = roster[roster['pos']=='P'].reset_index(drop=True),
            catcher     = roster[roster['pos']=='C'].reset_index(drop=True),
            first       = roster[roster['pos']=='1B'].reset_index(drop=True),
            second      = roster[roster['pos']=='2B'].reset_index(drop=True),
            third       = roster[roster['pos']=='3B'].reset_index(drop=True),
            short       = roster[roster['pos']=='SS'].reset_index(drop=True),
            left        = roster[roster['pos']=='LF'].reset_index(drop=True),
            center      = roster[roster['pos']=='CF'].reset_index(drop=True),
            right       = roster[roster['pos']=='RF'].reset_index(drop=True),
            dh          = roster[roster['pos']=='DH'].reset_index(drop=True),
            designated_hitter = roster[roster['pos']=='DH'].reset_index(drop=True),
            infield     = roster[roster['pos'].isin(['1B','2B','3B','SS'])].reset_index(drop=True),
            outfield    = roster[roster['pos'].isin(['OF','LF','CF','RF'])].reset_index(drop=True),
            active      = roster[roster['status']=='Active']
        )

        self.__first_year = int(ti['first_year'])
        self.__last_year = int(ti.get("last_year",default_season()))

    def __str__(self):
        return self.__franchise

    def __repr__(self):
        return self.__franchise

    @property
    def mlbam(self) -> int:
        """Team's MLB Advanced Media ID"""
        return int(self.__mlbam)

    @property
    def name(self):
        """Various names team names/aliases"""
        return self.__name
    
    @property
    def venue(self):
        """Information for team's venue"""
        return self.__venue

    @property
    def standings(self):
        """Standings (records, record splits)"""
        return self.__standings

    @property
    def raw_data(self):
        return self.__yby_data

    @property
    def stats(self):
        """Team stats data"""
        return self.__stats

    @property
    def roster(self) -> roster_wrapper:
        """Roster data"""
        return self.__roster

    @property
    def retired(self) -> pd.DataFrame:
        """Retired numbers"""
        return self.__retired

    @property
    def legends(self) -> pd.DataFrame:
        """Team's "Hall of Fame" Inductees"""
        return self.__legends
    
    @property
    def league(self) -> mlb_wrapper:
        """Information for team's league"""
        return self.__league
    
    @property
    def division(self) -> mlb_wrapper:
        """Information for team's division"""
        return self.__division

    @property
    def first_year(self) -> int:
        """First year of play"""
        return self.__first_year

    @property
    def last_year(self) -> int:
        """Last/most recent year of play"""
        return self.__last_year


class Team:
    """# Team

    Represents a collection of team data for a single year

    Parameters
    ----------

    mlbam : int
        Team's official MLB ID

    season : int
        Retrieve data for a specific season. Default value is None -- retrieves data for the most recent season
    
    """
    
    def __new__(cls,mlbam:int,season:Optional[int]=None,**kwargs):
        cls.today = md(dt.datetime.today().date().strftime(r"%Y-%m-%d"))

        if season is None:
            season = default_season()

        cls.mlbam = int(mlbam)
        cls.season = int(season)
        
        data : Union[dict,None] = _team_data(cls.mlbam,cls.season)
        cls.raw_data = data

        ti : dict = data['team_info']
        
        cls.name = mlb_wrapper(
            full=ti['full_name'],
            location=ti['location_name'],
            franchise=ti['franchise_name'],
            mascot=ti['team_name'],
            club=ti['club_name'],
            short=ti['short_name']
            )
        cls.league = mlb_wrapper(
            mlbam=ti['league_mlbam'],
            name=ti['league_name'],
            short=ti['league_short'],
            abbrv=ti['league_abbrv']
            )
        cls.division = mlb_wrapper(
            mlbam=ti['div_mlbam'],
            name=ti['div_name'],
            short=ti['div_short'],
            abbrv=ti['div_abbrv']
        )
        cls.venue = mlb_wrapper(
            name=ti['venue_name'],
            mlbam=ti['venue_mlbam']
        )
            
        cls.schedule     : pd.DataFrame = data['schedule']
        cls.drafts       : pd.DataFrame = data['drafts']
        cls.transactions : pd.DataFrame = data['transactions']
        cls.coaches      : pd.DataFrame = data['coaches']
    
        cls._stats = team_stats(
            totals={'hit_reg':data['total_hitting_reg'],'hit_adv':data['total_hitting_adv'],
                    'pit_reg':data['total_pitching_reg'],'pit_adv':data['total_pitching_reg'],
                    'fld_reg':data['total_fielding_reg']},
            roster={'hit_reg':data['hitting_reg'],'hit_adv':data['hitting_adv'],
                    'pit_reg':data['pitching_reg'],'pit_adv':data['pitching_adv'],
                    'fld_reg':data['fielding_reg']},
            )
        
        self = object.__new__(cls)
        return self

    def __str__(self):
        return self.name.full

    def __repr__(self):
        return self.name.full
    
    def __call__(self,_team_key:str):
        """Get raw data by key value
        
        Keys :
            - 'team_info'
            - 'hitting_reg'
            - 'pitching_reg'
            - 'fielding_reg'
            - 'hitting_adv' 
            - 'pitching_adv'
            - 'p_splits_reg'
            - 'p_splits_adv'
            - 'total_hitting'
            - 'total_pitching'
            - 'total_fielding'
            - 'coaches'
            - 'drafts'
            - 'transactions' 
            - 'schedule'

        """
        return self.raw_data[_team_key]
    
    def __getitem__(self, __name: str):
        return getattr(self,__name)
    
    def __setattr__(self,_attr,_value):
        raise AttributeError(f'Attribute assingment not supported for team object')

    def get_splits(self):
        pass
    
    @property
    def stats(self):
        """Wrapper for both team's cumulative & player-level stats
        """
        return self._stats


class Game:
    """# Game
    
    MLB Game instance
    
    Paramaters
    ----------
    gamePk : int or str
        numeric ID for the game
    
    timecode : str
        specify a value to retrieve a "snapshot" of the game at a specific point in time
        
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
        returns a python dictionary detailing information for the current matchup

    matchup_event_log() -> DataFrame
        returns a dataframe of events that have taken place for the current matchup/at-bat

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

    event_log() -> DataFrame
        returns a dataframe of every pitching event for every plate appearance thus far
    
    pa_log() -> DataFrame
        returns a dataframe of all plate appearance results

    get_content() -> dict
        returns a dictionary of broadcast information, highlight/recap video urls, and editorial data

    flags() -> dict
        returns a dictionary of notable attributes about the game
    """
    
    def __init__(self,gamePk,timecode=None,tz="ct"):
        game_id = gamePk
        self.__tz = tz
        self.game_id = game_id
        self.gamePk  = game_id

        if timecode is None:
            timecodeQuery = ""
        else:
            timecodeQuery = f"&timecode={timecode}"

        BASE = "https://statsapi.mlb.com/api"
        game_url = BASE + f"/v1.1/game/{self.game_id}/feed/live?hydrate=venue,flags,preState{timecodeQuery}"

        gm = requests.get(game_url).json()
        self.__raw_game_data = gm

        gameData = gm["gameData"]
        liveData = gm["liveData"]

     # GAME Information
        self.__linescore    = liveData["linescore"]
        self.__boxscore     = liveData["boxscore"]
        self.__flags        = gameData["flags"]

        self.gameState      = gameData["status"]["abstractGameState"]
        self.game_state     = self.gameState
        self.detailedState  = gameData["status"]["detailedState"]
        self.detailed_state = self.detailedState
        self.info           = self.__boxscore["info"]
        self.sky            = gameData["weather"].get("condition","-")
        self.temp           = gameData["weather"].get("temp","-")
        self.wind           = gameData["weather"].get("wind","-")
        self.first_pitch    = gameData.get("gameInfo",{}).get("firstPitch","-")
        self.attendance     = gameData.get("gameInfo",{}).get("attendance","-")
        self.start          = gameData.get("datetime",{}).get("time","-")
        self.start_iso      = gameData.get("datetime",{}).get("dateTime","-")

        datetime = gameData["datetime"]
        self.game_date      = datetime["officialDate"]
        self.gameDate       = self.game_date
        self.daynight       = datetime["dayNight"]

        self.__venue        = gameData["venue"]

        self.__officials    = self.__boxscore.get("officials",[{},{},{},{}])
        
        if len(self.__officials) != 0:
            self.__ump_home     = self.__officials[0].get("official",{})
            self.__ump_first    = self.__officials[1].get("official",{})
            self.__ump_second   = self.__officials[2].get("official",{})
            self.__ump_third    = self.__officials[3].get("official",{})
        
        else:
            self.__ump_home     = {}
            self.__ump_first    = {}
            self.__ump_second   = {}
            self.__ump_third    = {}

        self.__umpires      = {"home":self.__ump_home,"first":self.__ump_first,"second":self.__ump_second,"third":self.__ump_third}
     
     # ALL PLAYERS IN GAME
        self.__players = gameData["players"]

     # AWAY Team Data
        away = gameData["teams"]["away"]
        _away_data = self.__boxscore["teams"]["away"]
        _away_score_data = self.__linescore['teams']['away']
        self.away_id                = away["id"]
        self.__away_team_full       = away["name"]
        self.__away_team            = away["clubName"]
        self.__away_team_abbrv      = away["abbreviation"]
        self.__away_stats           = _away_data["teamStats"]
        self.__away_player_data     = _away_data["players"]
        self.__away_lineup          = _away_data["batters"]
        self.__away_starting_order  = _away_data["battingOrder"]
        self.__away_pitcher_lineup  = _away_data["pitchers"]
        self.__away_bullpen         = _away_data["bullpen"]
        self.__away_rhe             = self.__linescore["teams"]["away"]
        self.__away_bench           = _away_data["bench"]
        self.away_full              = self.__away_team_full
        self.away_club              = self.__away_team
        self.away_abbrv             = self.__away_team_abbrv
        self.away_runs              = _away_score_data['runs']
        self.away_hits              = _away_score_data['hits']
        self.away_errs              = _away_score_data['errors']
        self.away_record            = f'{away["record"]["wins"]}-{away["record"]["losses"]}'

     # HOME Team Data
        home = gameData["teams"]["home"]
        _home_data = self.__boxscore["teams"]["home"]
        _home_score_data = self.__linescore['teams']['home']
        self.home_id                = home["id"]
        self.__home_team_full       = home["name"]
        self.__home_team            = home["clubName"]
        self.__home_team_abbrv      = home["abbreviation"]
        self.__home_stats           = _home_data["teamStats"]
        self.__home_player_data     = _home_data["players"]
        self.__home_lineup          = _home_data["batters"]
        self.__home_starting_order  = _home_data["battingOrder"]
        self.__home_pitcher_lineup  = _home_data["pitchers"]
        self.__home_bullpen         = _home_data["bullpen"]
        self.__home_rhe             = self.__linescore["teams"]["home"]
        self.__home_bench           = _home_data["bench"]
        self.home_full              = self.__home_team_full
        self.home_club              = self.__home_team
        self.home_abbrv             = self.__home_team_abbrv
        self.home_runs              = _home_score_data['runs']
        self.home_hits              = _home_score_data['hits']
        self.home_errs              = _home_score_data['errors']
        self.home_record            = f'{home["record"]["wins"]}-{home["record"]["losses"]}'

        self.__curr_defense         = self.__linescore["defense"]
        self.__curr_offense         = self.__linescore["offense"]
        self.__curr_play            = liveData["plays"].get("currentPlay",{})

        self.balls                  = self.__linescore.get("balls",0)
        self.strikes                = self.__linescore.get("strikes",0)
        self.outs                   = self.__linescore.get("outs",0)
        self.__inning               = self.__linescore.get("currentInning","-")
        self.__inning_ordinal       = self.__linescore.get("currentInningOrdinal","-")
        self.__inning_state         = self.__linescore.get("inningState","-")

        self.__inn_half             = self.__linescore.get("inningHalf","-")
        self.__inn_label            = f"{self.__inn_half} of the {self.__inning_ordinal}"
        self.__scheduled_innings    = self.__linescore.get('scheduledInnings',9)

     # PLAYS and EVENTS
        self.__all_plays = liveData["plays"]["allPlays"]
        self.__scoring_plays = []

        self.__all_events = []
        self.__pitch_events = []
        self.__bip_events = []
        for play in self.__all_plays:
            for event in play["playEvents"]:
                self.__all_events.append(event)
                if event["isPitch"] == True:
                    self.__pitch_events.append(event)
                    if event["details"]["isInPlay"] == True:
                        self.__bip_events.append(event)
            try:
                if play["about"]["isScoringPlay"] == True:
                    self.__scoring_plays.append(play)
            except:
                pass

        # self.__str_display = game_str_display(self)

    def __str__(self):
        return f"{self.game_id} | {self.__away_team_abbrv} ({self.__away_rhe.get('runs',0)}) @ {self.__home_team_abbrv} ({self.__home_rhe.get('runs',0)})"

    def __repr__(self):
        return f"{self.game_id} | {self.__away_team_abbrv} ({self.__away_rhe.get('runs',0)}) @ {self.__home_team_abbrv} ({self.__home_rhe.get('runs',0)})"

    def __getitem__(self,key):
        return getattr(self,key)

    # def overview(self):
    #     print(self.__str_display)
    @property
    def scheduled_innings(self) -> int:
        """Number of scheduled innings"""
        return int(self.__scheduled_innings)

    @property
    def inning(self) -> str:
        """Current inning as an string formatted integer
        
        """
        return str(self.__inning)

    @property
    def inning_half(self) -> str:
        """Label for the current inning
        
        "Top", "Bottom"
        
        """
        return str(self.__inning_half)
    
    @property
    def inning_state(self) -> str:
        """State of current inning
        
        """
        return str(self.__inning_state)
    
    @property
    def inning_ordinal(self) -> str:
        """Ordinal display for current inning
        
        "1st", "2nd", "3rd", etc...
        
        """
        return str(self.__inning_ordinal)

    def umpires(self,base=None):
        """Get info for umpires. If 'base' is None, all umpire data is returned
        """
        if base is None:
            return self.__umpires
        return self.__umpires[base]

    def boxscore(self,tz=None) -> dict:
        if tz is None:
            tz = self.__tz
        # compiles score, batting lineups, players on field, current matchup, count, outs, runners on base, etc.
        away = {"full":self.__away_team_full,"club":self.__away_team,"mlbam":self.away_id}
        home = {"full":self.__home_team_full,"club":self.__home_team,"mlbam":self.home_id}

        if self.__inn_half == "Top": # might have to adjust to using "inning state"
            team_batting = away
            team_fielding = home
        else:
            team_batting = home
            team_fielding = away

        diamond = self.diamond()
        situation = self.situation()
        matchup = self.matchup_info()
        scoreAway = self.__away_rhe.get("runs",0)
        scoreHome = self.__home_rhe.get("runs",0)
        firstPitch = prettify_time(self.first_pitch,tz=tz)
        scheduledStart = prettify_time(self.start_iso,tz=tz)
        umps = {"home":self.__ump_home,"first":self.__ump_first,"second":self.__ump_second,"third":self.__ump_third}

        return {
            "away":away,
            "home":home,
            "batting":team_batting,
            "fielding":team_fielding,
            "diamond":diamond,
            "situation":situation,
            "matchup":matchup,
            "score":{"away":scoreAway,"home":scoreHome},
            "gameState":self.gameState,
            "firstPitch":firstPitch,
            "scheduledStart":scheduledStart,
            "umpires":umps
            }
          
    def linescore(self) -> dict:
        """
        Returns a tuple of game's current linescore
        """

        ls = self.__linescore
        ls_total = {
            "away":{
                "runs":ls.get("teams",{}).get("away",{}).get("runs","-"),
                "hits":ls.get("teams",{}).get("away",{}).get("hits","-"),
                "errors":ls.get("teams",{}).get("away",{}).get("errors","-")
                },
            "home":{
                "runs":ls.get("teams",{}).get("home",{}).get("runs","-"),
                "hits":ls.get("teams",{}).get("home",{}).get("hits","-"),
                "errors":ls.get("teams",{}).get("home",{}).get("errors","-")
                }
        }
        
        ls_inns = []
        ls_innings_array = ls['innings']
        for inn in ls_innings_array:
            # inn = ls_innings_array[idx]
            ls_inns.append({
            "away":{
                "runs":inn.get("away",{}).get("runs","-"),
                "hits":inn.get("away",{}).get("hits","-"),
                "errors":inn.get("away",{}).get("errors","-")
                },
            "home":{
                "runs":inn.get("home",{}).get("runs","-"),
                "hits":inn.get("home",{}).get("hits","-"),
                "errors":inn.get("home",{}).get("errors","-")
                },
            "inning":inn.get("num","-"),
            "inningOrdinal":ORDINALS[str(inn.get("num","-"))]
            })
        if len(ls_innings_array) < self.scheduled_innings:
            most_recent_inn = int(ls_inns[-1]['inning'])
            inns_til_9 = self.scheduled_innings - len(ls_innings_array)
            rem_innings = list(range(inns_til_9))
            for inn in rem_innings:
                next_inn = most_recent_inn + inn + 1
                ls_inns.append({
                "away":{
                    "runs":'-',
                    "hits":'-',
                    "errors":'-'
                    },
                "home":{
                    "runs":'-',
                    "hits":'-',
                    "errors":'-'
                    },
                "inning":str(next_inn),
                "inningOrdinal":ORDINALS[str(next_inn)]
                })
        return {
            "total":ls_total,
            "innings":ls_inns,
            "away":{},
            "home":{}
        }

    def situation(self) -> dict:
        """Returns a python dictionary detailing the current game situation (count, outs, men-on, batting queue):

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
            onDeck = self.__curr_offense["onDeck"]
            inHole = self.__curr_offense["inHole"]
        except:
            return {
                    "outs":self.outs,
                    "balls":self.balls,
                    "strikes":self.strikes,
                    "runnersOn":{},
                    "basesOccupied":[],
                    "queue":{
                        "onDeck":{
                            "id":"",
                            "name":""
                            },
                        "inHole":{
                            "id":"",
                            "name":""
                            }
                        }
                    }

        matchup = self.__curr_play["matchup"]

        basesOccupied = []
        runnersOn = {}
        if "first" in self.__curr_offense.keys():
            basesOccupied.append(1)
            runnersOn["first"] = {
                "id":self.__curr_offense["first"]["id"],
                "name":self.__curr_offense["first"]["fullName"],
                "isOccuppied":True
                }
        else:runnersOn["first"] = {"isOccuppied":False}

        if "second" in self.__curr_offense.keys():
            basesOccupied.append(2)
            runnersOn["second"] = {
                "id":self.__curr_offense["second"]["id"],
                "name":self.__curr_offense["second"]["fullName"],
                "isOccuppied":True
                }
        else:runnersOn["second"] = {"isOccuppied":False}

        if "third" in self.__curr_offense.keys():
            basesOccupied.append(3)
            runnersOn["third"] = {
                "id":self.__curr_offense["third"]["id"],
                "name":self.__curr_offense["third"]["fullName"],
                "isOccuppied":True
                }
        else:runnersOn["third"] = {"isOccuppied":False}

        return {
            "outs":self.outs,
            "balls":self.balls,
            "strikes":self.strikes,
            "runnersOn":runnersOn,
            "basesOccupied":basesOccupied,
            "queue":{
                "onDeck":{
                    "id":onDeck["id"],
                    "name":onDeck["fullName"]
                    },
                "inHole":{
                    "id":inHole["id"],
                    "name":inHole["fullName"]
                    }
                }}

    def venue(self) -> dict:
        v = self.__venue
        venue_name  = v["name"]
        venue_mlbam = v["id"]
        fieldInfo   = v["fieldInfo"]
        capacity    = fieldInfo["capacity"]
        roof        = fieldInfo["roofType"]
        turf        = fieldInfo["turfType"]
        try:
            dimensions = {
                "leftLine":     fieldInfo["leftLine"],
                "leftCenter":   fieldInfo["leftCenter"],
                "center":       fieldInfo["center"],
                "rightCenter":  fieldInfo["rightCenter"],
                "rightLine":    fieldInfo["rightLine"]
            }
        except:
            dimensions = {
                "leftLine":None,
                "leftCenter":None,
                "center":None,
                "rightCenter":None,
                "rightLine":None
            }
        loc = v["location"]
        latitude    = loc.get("defaultCoordinates",{}).get("latitude",None)
        longitude   = loc.get("defaultCoordinates",{}).get("longitude",None)
        address1    = loc.get("address1",None)
        address2    = loc.get("address2",None)
        city        = loc.get("city",None)
        state       = loc.get("state",None)
        stateAbbrev = loc.get("stateAbbrev",None)
        zipCode     = loc.get("postalCode",None)
        phone       = loc.get("phone",None)

        return {
            "name":         venue_name,
            "mlbam":        venue_mlbam,
            "capacity":     capacity,
            "roof":         roof,
            "turf":         turf,
            "dimensions":   dimensions,
            "lat":          latitude,
            "long":         longitude,
            "address1":     address1,
            "address2":     address2,
            "city":         city,
            "state":        state,
            "stateAbbrev":  stateAbbrev,
            "zipCode":      zipCode,
            "phone":        phone
        }

    def diamond(self,print_as_df=True):
        """
        Returns current defensive team's roster
        `print_as_df`: whether or not method will return pandas.Dataframe (return python dict if False)
                        default:`True`
        """
        try:
            diamond = {
                1:self.__curr_defense["pitcher"],
                2:self.__curr_defense["catcher"],
                3:self.__curr_defense["first"],
                4:self.__curr_defense["second"],
                5:self.__curr_defense["third"],
                6:self.__curr_defense["shortstop"],
                7:self.__curr_defense["left"],
                8:self.__curr_defense["center"],
                9:self.__curr_defense["right"],
            }
        except:
            return {}
        curr_diamond = []
        for key, value in diamond.items():
            curr_diamond.append([POSITION_DICT[key],value["fullName"]])

        df = pd.DataFrame(curr_diamond)

        diamond = {
            "pitcher":  {"name":self.__curr_defense["pitcher"]["fullName"],"id":self.__curr_defense["pitcher"]["id"]},
            "catcher":  {"name":self.__curr_defense["catcher"]["fullName"],"id":self.__curr_defense["catcher"]["id"]},
            "first":    {"name":self.__curr_defense["first"]["fullName"],"id":self.__curr_defense["first"]["id"]},
            "second":   {"name":self.__curr_defense["second"]["fullName"],"id":self.__curr_defense["second"]["id"]},
            "third":    {"name":self.__curr_defense["third"]["fullName"],"id":self.__curr_defense["third"]["id"]},
            "shortstop":{"name":self.__curr_defense["shortstop"]["fullName"],"id":self.__curr_defense["shortstop"]["id"]},
            "left":     {"name":self.__curr_defense["left"]["fullName"],"id":self.__curr_defense["left"]["id"]},
            "center":   {"name":self.__curr_defense["center"]["fullName"],"id":self.__curr_defense["center"]["id"]},
            "right":    {"name":self.__curr_defense["right"]["fullName"],"id":self.__curr_defense["right"]["id"]},
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
            matchup = self.__curr_play["matchup"]
            zone_top = self.__curr_play["playEvents"][-1]["pitchData"]["strikeZoneTop"]
            zone_bot = self.__curr_play["playEvents"][-1]["pitchData"]["strikeZoneBottom"]
        except:
            return {"atBat":{},"pitching":{},"zone":(3.5,1.5)}
        atBat = {
            "name":matchup["batter"]["fullName"],
            "id":matchup["batter"]["id"],
            "bats":matchup["batSide"]["code"],
            "zone_top":self.__players[f'ID{matchup["batter"]["id"]}']["strikeZoneTop"],
            "zone_bot":self.__players[f'ID{matchup["batter"]["id"]}']["strikeZoneBottom"],
            "stands":matchup["batSide"]["code"]
            }
        pitching = {
            "name":matchup["pitcher"]["fullName"],
            "id":matchup["pitcher"]["id"],
            "throws":matchup["pitchHand"]["code"]}
        
        return {"atBat":atBat,"pitching":pitching,"zone":(zone_top,zone_bot)}

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
            "play_id"]
        try:
            pa_events = self.__curr_play["playEvents"]
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

                try:start_vel = ab_log["pitchData"]["startSpeed"]
                except:start_vel = "--"

                try:end_vel = ab_log["pitchData"]["endSpeed"]
                except:end_vel = "--"

                try:pX_coord = ab_log["pitchData"]["coordinates"]["pX"]
                except:pX_coord = "--"
                try:pZ_coord = ab_log["pitchData"]["coordinates"]["pZ"]
                except:pZ_coord = "--"

                try:
                    zoneTopInitial = pa_events[0]["pitchData"]["strikeZoneTop"]
                    zoneBottomInitial = pa_events[0]["pitchData"]["strikeZoneBottom"]
                except:
                    try:
                        zoneTopInitial = pa_events[0]["pitchData"]["strikeZoneTop"]
                        zoneBottomInitial = pa_events[0]["pitchData"]["strikeZoneBottom"]
                    except:
                        zoneTopInitial = 3.5
                        zoneBottomInitial = 1.5
                try:
                    zone_top = ab_log["pitchData"]["strikeZoneTop"]
                    zone_bot = ab_log["pitchData"]["strikeZoneBottom"]
                except:
                    zone_top = 3.5
                    zone_bot = 1.5

                try:spin = ab_log["pitchData"]["breaks"]["spinRate"]
                except:spin = ""
                try:zone = ab_log["pitchData"]["zone"]
                except:zone = ""
                try:hit_location = ab_log["hitData"]["location"]
                except:hit_location = ""
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

        matchup_df = pd.DataFrame(data=events_data,columns=headers)
        matchup_df.sort_values(by=["pitch_num"],inplace=True)

        return matchup_df

    def pa_log(self) -> pd.DataFrame:
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
            "play_id"
            ]

        events_data = []
        for play in self.__all_plays:
            for e in play["playEvents"]:
                if "game_advisory" in e.get("details",{}).get("eventType","").lower():
                    pass
                else:
                    firstEvent = e
                    break
            play_id = play["playEvents"][-1].get("playId","-")
            lastEvent = play["playEvents"][-1]
            pitchData = lastEvent["pitchData"]
            try:ab_num = play["atBatIndex"] + 1
            except:ab_num = "--"
            try:bat_side = play["matchup"]["batSide"]["code"]
            except:bat_side = "--"

            try:innNum = play["about"]["inning"]
            except:innNum = "--"
            try:innHalf = play["about"]["halfInning"]
            except:innHalf = "--"
            if innHalf == "bottom":
                inning = f"Bot {innNum}"
            else: inning = f"Top {innNum}"

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
            try:pitchNum = lastEvent["pitchNumber"]
            except:pitchNum = "--"
            try:
                event = play["result"]["event"]
                event_type = play["result"]["eventType"]
            except:
                event = "--"
                event_type = "--"
            try:details = play["result"]["description"]
            except:details = "--"
            try:
                pitchType = lastEvent["details"]["type"]["description"]
                pitchCode = lastEvent["details"]["type"]["code"]
            except:
                pitchType = "--"
                pitchCode = "--"
            try:releaseSpeed = pitchData["startSpeed"]
            except:releaseSpeed = "--"
            try:endSpeed = pitchData["endSpeed"]
            except:endSpeed = "--"
            try:spinRate = pitchData["breaks"]["spinRate"]
            except:spinRate = "--"
            try:zone = pitchData["zone"]
            except:zone = "--"


            # Hit Data (and Trajectory - if ball is in play)
            try:hitData = lastEvent["hitData"]
            except:pass
            try:launchSpeed = hitData["launchSpeed"]
            except:launchSpeed = "--"
            try: launchAngle = hitData["launchAngle"]
            except:launchAngle = "--"
            try: distance = hitData["totalDistance"]
            except:distance = "--"
            try:hitLocation = hitData["location"]
            except:hitLocation = "--"
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
            try:category = lastEvent["type"]
            except:category = "--"

            # Time information
            try:
                startTime = firstEvent["startTime"]
                startTime_obj = dt.datetime.strptime(startTime,iso_format_ms).replace(tzinfo=utc_zone)
                startTime = dt.datetime.strftime(startTime_obj,iso_format_ms)
                
            except:
                startTime = "--"
            try:
                endTime = lastEvent["endTime"]
                endTime_obj = dt.datetime.strptime(endTime,iso_format_ms).replace(tzinfo=utc_zone)
                endTime = dt.datetime.strftime(endTime_obj,iso_format_ms)
                # endTime = dt.datetime.strptime(play["playEvents"][-1]["endTime"],iso_format_ms).replace(tzinfo=utc_zone)
                
                elasped = endTime_obj - startTime_obj
                

            except Exception as e:
                endTime = "--"
                elasped = "--"
                print(f"ERROR: -- {e} --")

            # Is Home Team Batting?
            if f"ID{batterID}" in self.__home_player_data.keys():
                homeBatting = True
                batTeamID = self.home_id
                battingTeam = self.__home_team
            else:
                homeBatting = False
                batTeamID = self.away_id
                battingTeam = self.__away_team

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
                play_id]

            events_data.append(event_data)

        df = pd.DataFrame(data=events_data,columns=headers).sort_values(by=["pa"],ascending=False)

        return df

    def event_log(self) -> pd.DataFrame:
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
            'pX',
            'pZ',
            "exit_velocity",
            "launch_angle",
            "distance",
            "location",
            "hX",
            "hY",
            "category",
            "end_time",
            "isHome",
            "play_id"]

        events_data = []
        for play in self.__all_plays:
            try:ab_num = play["about"]["atBatIndex"]+1
            except:ab_num = "--"
            try:batter = play["matchup"]["batter"]
            except:batter = "--"
            try:pitcher = play["matchup"]["pitcher"]
            except:pitcher = "--"
            try:
                zone_top = self.__players[f'ID{batter["id"]}']["strikeZoneTop"]
                zone_bot = self.__players[f'ID{batter["id"]}']["strikeZoneBottom"]
            except:
                zone_top = "-"
                zone_bot = "-"
            try:bat_side = play["matchup"]["batSide"]["code"]
            except:bat_side = "-"

            last_idx = play["playEvents"][-1]["index"]

            for event_idx,event in enumerate(play["playEvents"]):
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

                try:pitchNumber = event["pitchNumber"]
                except:pitchNumber = 0

               # Times
                try:
                    startTime = dt.datetime.strptime(event["startTime"],iso_format_ms).replace(tzinfo=utc_zone)
                    # startTimeStr = startTime.strftime(r"%H:%M:%S (%Y-%m-%d)")
                except:startTime = "--"
                try:
                    endTime = dt.datetime.strptime(event["endTime"],iso_format_ms).replace(tzinfo=utc_zone)
                    # endTimeStr = endTime.strftime(r"%H:%M:%S (%Y-%m-%d)")
                except:
                    endTime = "--"
                    # endTimeStr = "--"
                try:
                    elapsed = endTime - startTime
                    elapsed = prettify_time(elapsed)
                except:elapsed = "--"

               # Call and Pitch Type and Count/Outs
                try:pitchType = event["details"]["type"]["description"]
                except:pitchType = "--"
                try:pitchCode = event["details"]["type"]["code"]
                except:pitchCode = "--"
                try:call = event["details"]["description"]
                except:call = "--"
                try:count = f'{event["count"]["balls"]}-{event["count"]["strikes"]}'
                except:count = "--"
                try:outs = event["count"]["outs"]
                except: outs = "--"

               # Pitch Data
                try:pitchData = event["pitchData"]
                except:pass
                try:startSpeed = pitchData["startSpeed"]
                except:startSpeed = "--"
                try:endSpeed = pitchData["endSpeed"]
                except:endSpeed = "--"
                try:spinRate = pitchData["breaks"]["spinRate"]
                except:spinRate = "--"
                try:zone = pitchData["zone"]
                except:zone = "--"
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
                    try:hitData = event["hitData"]
                    except:pass
                    try:launchSpeed = hitData["launchSpeed"]
                    except:launchSpeed = "--"
                    try: launchAngle = hitData["launchAngle"]
                    except:launchAngle = "--"
                    try: distance = hitData["totalDistance"]
                    except:distance = "--"
                    try:hitLocation = hitData["location"]
                    except:hitLocation = "--"
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

               #type
                try:category = event["type"]
                except:category = "--"

               # Is Home Team Batting?
                if f'ID{batter["id"]}' in self.__home_player_data.keys():
                    homeBatting = True
                    batTeamID = self.home_id
                    battingTeam = self.__home_team
                else:
                    homeBatting = False
                    batTeamID = self.away_id
                    battingTeam = self.__away_team

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
                    play_id]

                events_data.append(event_data)

        df = pd.DataFrame(data=events_data,columns=headers).sort_values(by=["pa","pitch_idx"],ascending=False)
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
            'pX',
            'pZ',
            "exit_velocity",
            "launch_angle",
            "distance",
            "location",
            "hX",
            "hY",
            "category",
            "end_time",
            "isHome"]

        events_data = []
        for play in self.__scoring_plays:
            try:ab_num = play["about"]["atBatIndex"]+1
            except:ab_num = "--"
            try:batter = play["matchup"]["batter"]
            except:batter = "--"
            try:pitcher = play["matchup"]["pitcher"]
            except:pitcher = "--"

            last_idx = play["playEvents"][-1]["index"]

            for event_idx,event in enumerate(play["playEvents"]):
                try:
                    if event_idx != last_idx:desc = "--"
                    else:desc = play["result"]["description"]
                except:desc = "--"
                
                try:
                    event_label = play["result"]["event"]
                    event_type = play["result"]["eventType"]
                except:
                    event_label = "--"
                    event_type = "--"
               # Times

                try:pitchNumber = event["pitchNumber"]
                except:pitchNumber = 0
                try:
                    # startTime = dt.datetime.strptime(event["startTime"],iso_format_ms).replace(tzinfo=utc_zone)
                    startTime = dt.datetime.strptime(event["startTime"],iso_format_ms)
                    startTimeStr = startTime.strftime(r"%H:%M:%S (%Y-%m-%d)")
                except:
                    startTime = "--"
                    # startTimeStr = "--"
                try:
                    # endTime = dt.datetime.strptime(event["endTime"],iso_format_ms).replace(tzinfo=utc_zone)
                    endTime = dt.datetime.strptime(event["endTime"],iso_format_ms)
                    # endTimeStr = endTime.strftime(r"%H:%M:%S (%Y-%m-%d)")
                except:
                    endTime = "--"
                    # endTimeStr = "--"
                try:
                    elapsed = endTime - startTime
                    elapsed = prettify_time(elapsed)
                except:elapsed = "--"


               # Call and Pitch Type and Count/Outs
                try:pitchType = event["details"]["type"]["description"]
                except:pitchType = "--"
                try:pitchCode = event["details"]["type"]["code"]
                except:pitchCode = "--"
                try:call = event["details"]["description"]
                except:call = "--"
                try:count = f'{event["count"]["balls"]}-{event["count"]["strikes"]}'
                except:count = "--"
                try:outs = event["count"]["outs"]
                except: outs = "--"

               # Pitch Data
                try:pitchData = event["pitchData"]
                except:pass
                try:startSpeed = pitchData["startSpeed"]
                except:startSpeed = "--"
                try:endSpeed = pitchData["endSpeed"]
                except:endSpeed = "--"
                try:spinRate = pitchData["breaks"]["spinRate"]
                except:spinRate = "--"
                try:zone = pitchData["zone"]
                except:zone = "--"
                try:
                    pX = pitchData["coordinates"]["pX"]
                    pZ = pitchData["coordinates"]["pZ"]
                except:
                    pX = "--"
                    pZ = "--"

               # Hit Data
                if "hitData" in event.keys():
                    try:hitData = event["hitData"]
                    except:pass
                    try:launchSpeed = hitData["launchSpeed"]
                    except:launchSpeed = "--"
                    try: launchAngle = hitData["launchAngle"]
                    except:launchAngle = "--"
                    try: distance = hitData["totalDistance"]
                    except:distance = "--"
                    try:hitLocation = hitData["location"]
                    except:hitLocation = "--"
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

               #type
                try:category = event["type"]
                except:category = "--"

               # Is Home Team Batting?
                if f'ID{batter["id"]}' in self.__home_player_data.keys():
                    homeBatting = True
                    batTeamID = self.home_id
                    battingTeam = self.__home_team
                else:
                    homeBatting = False
                    batTeamID = self.away_id
                    battingTeam = self.__away_team

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
                    homeBatting]

                events_data.append(event_data)

        df = pd.DataFrame(data=events_data,columns=headers).sort_values(by=["pa","pitch_idx"],ascending=False)
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
        if self.gameState == "Live" or self.gameState == "Final" or self.gameState == "Preview":
            tm = self.__away_stats["batting"]
            players = self.__away_player_data
            # headers = ["Player","Pos","AB","R","H","RBI","SO","BB","AVG","HR","2B","3B","FO","GO","IBB","SacBunts","SacFlies","GIDP","batting","substitute","bbrefID","mlbam"]
            headers = ["Player","Pos","AB","R","H","RBI","SO","BB","AVG","HR","2B","3B","FO","GO","IBB","sB","sF","GIDP","at_bat","is_sub","mlbam"]
            rows = []
            for playerid in self.__away_lineup:
                player = players[f"ID{playerid}"]
                name = player["person"]["fullName"]
                position = player["position"]["abbreviation"]
                stats = player["stats"]["batting"]
                if len(stats) == 0:pass
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
                    #     bbrefID = self.__people[self.__people["mlbam"]==playerid].bbrefID.item()
                    # except:
                    #     search_url = f"https://www.baseball-reference.com/redirect.fcgi?player=1&mlb_ID={playerid}"
                    #     req = requests.get(search_url)
                    #     resp = req.url
                    #     bbrefID = resp[resp.rfind("/")+1:resp.rfind(".")]

                    # row_data = [name,position,at_bats,runs,hits,rbis,sos,bbs,avg,hrs,dbls,trpls,flyouts,groundouts,ibbs,sacbunts,sacflies,gidp,isCurrentBatter,isSubstitute,bbrefID,playerid]
                    row_data = [name,position,at_bats,runs,hits,rbis,sos,bbs,avg,hrs,dbls,trpls,flyouts,groundouts,ibbs,sacbunts,sacflies,gidp,isCurrentBatter,isSubstitute,playerid]
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
                " "
                ]
            rows.append(summary)
            df = pd.DataFrame(data=rows,columns=headers)
            return df
        else:
            print('error. check API for game\'s state')
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
        if self.gameState == "Live" or self.gameState == "Final" or self.gameState == "Preview":
            tm = self.__home_stats["batting"]
            players = self.__home_player_data
            headers = ["Player","Pos","AB","R","H","RBI","SO","BB","AVG","HR","2B","3B","FO","GO","IBB","sB","sF","GIDP","at_bat","is_sub","mlbam"]
            rows = []
            for playerid in self.__home_lineup:
                player = players[f"ID{playerid}"]
                name = player["person"]["fullName"]
                position = player["position"]["abbreviation"]
                stats = player["stats"]["batting"]
                if len(stats) == 0:pass
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
                    # try:bbrefID = self.__people[self.__people["mlbam"]==playerid].bbrefID.item()
                    # except:
                    #     search_url = f"https://www.baseball-reference.com/redirect.fcgi?player=1&mlb_ID={playerid}"
                    #     req = requests.get(search_url)
                    #     resp = req.url
                    #     bbrefID = resp[resp.rfind("/")+1:resp.rfind(".")]

                    row_data = [name,position,at_bats,runs,hits,rbis,sos,bbs,avg,hrs,dbls,trpls,flyouts,groundouts,ibbs,sacbunts,sacflies,gidp,isCurrentBatter,isSubstitute,playerid]
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
                " "
                ]
            rows.append(summary)
            df = pd.DataFrame(data=rows,columns=headers)
            return df
        else:
            print('error. check API for game\'s state')
            return pd.DataFrame()

    # TEAMS' INDIVIDUAL PITCHER STATS
    def away_pitching_stats(self) -> pd.DataFrame:
        """
        Get current game pitching stats for players on AWAY team

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
        players = self.__away_player_data
        headers = ["Player","Ct","IP","R","H","ER","SO","BB","K","B",f"ERA ({self.game_date[:4]})","Strike %","HR","2B","3B","PkOffs","Outs","IBB","HBP","SB","WP","BF","mlbam"]
        rows = []
        for playerid in self.__away_pitcher_lineup:
            player = players[f"ID{playerid}"]
            stats = player["stats"]["pitching"]

            name = player["person"]["fullName"]
            # pos = player["position"]["abbreviation"]
            pitch_ct = stats.get("pitchesThrown","")
            ip = float(stats.get("inningsPitched",0))
            sos = stats.get("strikeOuts",0)
            bbs = stats.get("baseOnBalls",0)
            strikes = stats.get("strikes",0)
            balls = stats.get("balls",0)
            try:strike_perc = float(stats["strikePercentage"])
            except:strike_perc = "--"
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
            # try:bbrefID = self.__people[self.__people["mlbam"]==playerid].bbrefID.item()
            # except: # retrieves player's bbrefID if not in current registry
            #     search_url = f"https://www.baseball-reference.com/redirect.fcgi?player=1&mlb_ID={playerid}"
            #     req = requests.get(search_url)
            #     resp = req.url
            #     bbrefID = resp[resp.rfind("/")+1:resp.rfind(".")]

            row_data = [name,pitch_ct,ip,runs,hits,ers,sos,bbs,strikes,balls,era,strike_perc,hrs,dbls,trpls,pkoffs,outs,ibbs,hbp,sbs,wps,batters_faced,playerid]
            rows.append(row_data)

        df = pd.DataFrame(data=rows,columns=headers)

        # sum_of_K_percs = 0
        # for num in list(df["Strike %"]):
        #     try:sum_of_K_percs+=num
        #     except:pass
        # try:rounded_strike_perc = round((sum_of_K_percs/len(df)),3)
        # except:rounded_strike_perc = ""
        
        # sum_df = pd.DataFrame(data=[[
        #     "Summary",
        #     df["Ct"].sum(),
        #     df["IP"].sum(),
        #     df["R"].sum(),
        #     df["H"].sum(),
        #     df["ER"].sum(),
        #     df["SO"].sum(),
        #     df["BB"].sum(),
        #     df["K"].sum(),
        #     df["B"].sum(),
        #     self.__away_stats["pitching"]["era"],
        #     rounded_strike_perc,
        #     df["HR"].sum(),
        #     df["2B"].sum(),
        #     df["3B"].sum(),
        #     df["PkOffs"].sum(),
        #     df["Outs"].sum(),
        #     df["IBB"].sum(),
        #     df["HBP"].sum(),
        #     df["SB"].sum(),
        #     df["WP"].sum(),
        #     df["BF"].sum(),
        #     " "
        # ]],columns=headers)

        # return pd.concat([df,sum_df])

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
        players = self.__home_player_data
        headers = ["Player","Ct","IP","R","H","ER","SO","BB","K","B",f"ERA ({self.game_date[:4]})","Strike %","HR","2B","3B","PkOffs","Outs","IBB","HBP","SB","WP","BF","mlbam"]
        rows = []
        for playerid in self.__home_pitcher_lineup:
            player = players[f"ID{playerid}"]
            stats = player["stats"]["pitching"]

            name = player["person"]["fullName"]
            # pos = player["position"]["abbreviation"]
            pitch_ct = stats.get("pitchesThrown","")
            ip = float(stats.get("inningsPitched",0))
            sos = stats.get("strikeOuts",0)
            bbs = stats.get("baseOnBalls",0)
            strikes = stats.get("strikes",0)
            balls = stats.get("balls",0)
            try:strike_perc = float(stats["strikePercentage"])
            except:strike_perc = "--"
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

            row_data = [name,pitch_ct,ip,runs,hits,ers,sos,bbs,strikes,balls,era,strike_perc,hrs,dbls,trpls,pkoffs,outs,ibbs,hbp,sbs,wps,batters_faced,playerid]
            rows.append(row_data)

        df = pd.DataFrame(data=rows,columns=headers)

        # sum_of_K_percs = 0
        # for num in list(df["Strike %"]):
        #     try:sum_of_K_percs+=num
        #     except:pass
        # try:rounded_strike_perc = round((sum_of_K_percs/len(df)),3)
        # except:rounded_strike_perc = ""
        # sum_df = pd.DataFrame(data=[[
        #     "Summary",
        #     df["Ct"].sum(),
        #     df["IP"].sum(),
        #     df["R"].sum(),
        #     df["H"].sum(),
        #     df["ER"].sum(),
        #     df["SO"].sum(),
        #     df["BB"].sum(),
        #     df["K"].sum(),
        #     df["B"].sum(),
        #     self.__home_stats["pitching"]["era"],
        #     rounded_strike_perc,
        #     df["HR"].sum(),
        #     df["2B"].sum(),
        #     df["3B"].sum(),
        #     df["PkOffs"].sum(),
        #     df["Outs"].sum(),
        #     df["IBB"].sum(),
        #     df["HBP"].sum(),
        #     df["SB"].sum(),
        #     df["WP"].sum(),
        #     df["BF"].sum(),
        #     " "
        # ]],columns=headers)

        # return pd.concat([df,sum_df])
    
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
        # these stats will be only for this CURRENT GAME (with the exception of a player's fielding average stat)
        tm = self.__away_stats["fielding"]
        players = self.__away_player_data
        headers = ["Player","Pos","Putouts","Assists","Errors","Chances","Stolen Bases","Pickoffs","Passed Balls",f"Fld % ({self.game_date[:4]})","mlbam"]
        rows = []
        for playerid in (self.__away_lineup+self.__away_pitcher_lineup):
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

            row_data = [name,pos,putouts,assists,errors,chances,sbs,pkoffs,passedballs,field_perc,playerid]
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
            " "
        ]
        rows.append(summary)
        df = pd.DataFrame(data=rows,columns=headers)
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
        # these stats will be only for this CURRENT GAME (with the exception of a player's fielding average stat)
        tm = self.__home_stats["fielding"]
        players = self.__home_player_data
        headers = ["Player","Pos","Putouts","Assists","Errors","Chances","Stolen Bases","Pickoffs","Passed Balls",f"Fld % ({self.game_date[:4]})","mlbam"]
        rows = []
        for playerid in (self.__home_lineup+self.__home_pitcher_lineup):
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

            row_data = [name,pos,putouts,assists,errors,chances,sbs,pkoffs,passedballs,field_perc,playerid]
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
            " "
        ]
        rows.append(summary)
        df = pd.DataFrame(data=rows,columns=headers)
        return df

    def timestamps(self) -> pd.DataFrame:
        plays = self.__all_plays
        ts = []
        for p in plays:
            play_type       = p.get("result",{}).get("type")
            playStartTime   = p.get("about",{}).get("startTime")
            playEndTime     = p.get("playEndTime","-")
            playEvents      = p.get("playEvents",[])
            # print(playEvents)
            try:
                abIndex         = p.get("atBatIndex")
                ab_num          = abIndex + 1
            except:
                pass
            for e in playEvents:
                play_id         = e.get("playId")
                eventStart      = e.get("startTime")
                eventEnd        = e.get("endTime")
                event_idx       = e.get("index")
                event_num       = event_idx + 1
                details         = e.get("details",{})
                event_type      = details.get("eventType")
                event_type2      = details.get("event")
                event_desc      = details.get("description")
                if eventStart is None:
                    start_tc = "-"
                else:
                    start_tc        = dt.datetime.strptime(eventStart,r"%Y-%m-%dT%H:%M:%S.%fZ")
                    start_tc        = start_tc.strftime(r"%Y%m%d_%H%M%S")
                if eventEnd is None:
                    end_tc = "-"
                else:
                    end_tc          = dt.datetime.strptime(eventEnd,r"%Y-%m-%dT%H:%M:%S.%fZ")
                    end_tc          = end_tc.strftime(r"%Y%m%d_%H%M%S")

                ts.append([
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
                    play_id
                ])
        df = pd.DataFrame(data=ts,columns=("ab_idx","type","event_idx","event_type","event_desc","event_start","start_tc","event_end","end_tc","play_id"))

        return df

    def flags(self) -> dict:
        f = self.__flags
        return f

    def get_content(self):
        url = BASE + f"/game/{self.gamePk}/content"
        resp = requests.get(url)
        return resp.json()
    
    def raw_feed_data(self):
        """Return the raw JSON data"""
        return self.__raw_game_data
    
    def get_feed_data(self,timecode=None):
        if timecode is not None:
            url = f"https://statsapi.mlb.com/api/v1.1/game/{self.gamePk}/feed/live?timecode={timecode}"
        else:
            url = f"https://statsapi.mlb.com/api/v1.1/game/{self.gamePk}/feed/live"
        resp = requests.get(url)
        return resp.json()

    def context_splits(self,batterID,pitcherID):  #  applicable DYNAMIC splits for the current matchup
        """This class method has not been configured yet"""
        pass

    def away_season_stats(self):
        """This method has not been configured yet"""
        pass

    def home_season_stats(self):
        """This method has not been configured yet"""
        pass

# ===============================================================
# API Wrapper | Function | Classes
# ===============================================================

class api:
    
    # @classmethod
    def get(path:str,hydrate=None,**query_parameters):
        """## get
        
        Create your own customized API calls to https://statsapi.mlb.com
        
        --------------------------------------------------------------------
        
        Parameters:
        -----------
        
        personId : int (alias -> 'mlbam')
        
        teamId : int (alias -> 'tm_mlbam')
        
        PATHS:
        ------
        * people
            * /changes
            * /freeAgents
            * /{personId}
            * /{personId}
            * /{personId}/stats/game/{gamePk}
            * /{personId}/stats/game/current
            
        
        
        """
        params = {}
        if path[0] == '/':
            path = path[1:]
            
        params = query_parameters
        
        url = f'{BASE}/{path}'
        
        req = requests.Request('GET',url,params=params).prepare()
        
        
    def prepare(path,**query_params):
        """Build
        
        """
        pass

            
            
    class teams:
        def __new__(cls,_mlbam):
            self = object.__new__(cls)
            self._mlbam = _mlbam
        
        @classmethod
        def search_by_name(cls,query:str,season:Optional[Union[int,None]]=None,hydrate:Optional[Union[str,None]]=None):
            if season is None:
                season = default_season()
            if hydrate is not None:
                if type(hydrate) is str:
                    hydrations = hydrate.replace(', ',',')
                elif type(hydrate) is list:
                    hydrations = ','.join(hydrations)
                hydrations = f'&hydrate={hydrations}'
            else:
                hydrations = ''
            resp = requests.get(f'{BASE}/teams?sportId=1&season={season}{hydrations}')
            
            for t in resp.json()['teams']:
                if query.lower() in t.get('name').lower():
                    return mlb_team(raw_data=t,**_parse_team(t))
                
        @classmethod
        def search_by_venue(cls,query:str,season:Optional[int]=None):
            if season is None:
                season = default_season()
                
            resp = requests.get(f'{BASE}/teams?sportId=1&season={season}')
            
            for t in resp.json()['teams']:
                if query.lower() in t.get('venue',{}).get('name','').lower():
                    return mlb_team(**_parse_team(t))
            
            
    class leagues:
        def __init__(self,_mlbam):
            self._mlbam = _mlbam
    
    @classmethod
    def player_search(cls,names:Optional[str]=None,teamIds:Optional[Union[str,list]]=None,**kwargs):
        """Search for a person using the API by name
        
        Paramaters
        ----------
        
        name : str (required)
            Player name to search
            
        personIds
            Insert personId(s) to search and return biographical information for a specific player(s). Format '605151,592450'

        leagueIds
            Insert leagueId(s) to search and return biographical information for players in a specific league(s).

        teamIds
            Insert teamId(s) to search and return biographical information for players on a specific team(s).

        hydrate : str
            Insert hydration(s) to return statistical or biographical data for a specific player(s).

        """
        
        url = f"https://statsapi.mlb.com/api/v1/people/search?"
        
        params = {'sportId':1}
        
        if names is not None:
            params['names'] = names
        else:
            if kwargs.get('season') is not None:
                url = f'https://statsapi.mlb.com/api/v1/sports/1/players?'
                params['season'] = kwargs['season']
        if kwargs.get('hydrate') is not None:
            params['hydrate'] = kwargs['hydrate']
        
        
        if teamIds is not None:
            if type(teamIds) is str:
                teamIds = teamIds.replace(', ',',')
            elif type(teamIds) is list:
                teamIds = ','.join(teamIds).replace(', ',',')
            params['teamIds'] = teamIds
        
        resp = requests.get(url,params=params)

        parsed_data = []
        for p_dict in resp.json()['people']:
            parsed_data.append(pd.Series(_parse_person(_obj=p_dict)))
        
        df = pd.DataFrame(data=parsed_data).reset_index(drop=True)
        return _people_data_collection(df.fillna('-'))

    
    def get_teams(start_season:Optional[int]=None,end_season:Optional[int]=None,hydrate_league=False,hydrate_division=False):
        urls = []
        if start_season is None:
            start_season = 1876
        if end_season is None:
            current_date = dt.date.today()
            current_year = current_date.year
            if current_date.month >= 7:
                end_season = current_year + 1
            else:
                end_season = current_year
        
        hydrations = []
        
        if hydrate_league is True:
            hydrations.append('league')
        if hydrate_division is True:
            hydrations.append('division')
            
        if len(hydrations) != 0:
            hydrations = ','.join(hydrations)
            hydrations = f'&hydrate={hydrations}'
        else:
            hydrations = ''
        
        
        for y in range(start_season,end_season+1):
            urls.append(f'https://statsapi.mlb.com/api/v1/teams?sportId=1&season={y}{hydrations}')
        
        # async
        # loop = _determine_loop()
        # fetched_data = loop.run_until_complete(_fetch(urls))
        fetched_data = _fetch(urls)
        
        parsed_data = []
        
        # data gets parsed by '_parse_team' function from misc.py
        for d in fetched_data:
            # each 'd' value is a JSON response
            list_of_team_dicts : List[dict] = d['teams']
            for tm_dict in list_of_team_dicts:
                parsed_data.append(pd.Series(_parse_team(_obj=tm_dict)))
                
        df = pd.DataFrame(data=parsed_data).reset_index(drop=True).sort_values(by=['season','name_full'],ascending=[False,True])
        return _teams_data_collection(df)
            
    