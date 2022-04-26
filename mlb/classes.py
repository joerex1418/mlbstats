import requests
import datetime as dt
from typing import Union, Optional, Dict, List, Literal
from pprint import pprint as pp

import pandas as pd

from .functions import _team_data
from .functions import _franchise_data
from .functions import _player_data
from .functions import fetch as _fetch

from .helpers import mlb_date as md
from .helpers import mlb_wrapper
from .helpers import player_position_wrapper
from .helpers import venue_name_wrapper
from .helpers import league_name_wrapper
from .helpers import team_name_data
from .helpers import person_name_data
from .helpers import standings_wrapper
from .helpers import roster_wrapper
from .helpers import edu_wrapper
from .helpers import player_stats
from .helpers import team_stats

# from .helpers import stats_roster
# from .helpers import stats_totals

from .helpers import _teams_data_collection
from .helpers import _people_data_collection
from .helpers import _parse_team
from .helpers import _parse_league
from .helpers import _parse_division
from .helpers import _parse_person
from .helpers import _parse_venue
from .helpers import mlb_team
from .helpers import mlb_person

from .constants import BASE, POSITION_DICT, ORDINALS

from .utils import iso_format_ms
from .utils import utc_zone
from .utils import prettify_time
from .utils import default_season


class Person:
    """# Person
    
    The "mlb.Person" class is a comprehensive wrapper for many different types 
    of data for a single person. All of the data 
    (with the exception of content in the 'bio' attribute/property) is pulled 
    from the Official MLB Stats API 
    (https://statsapi.mlb.com/api/v1).
    
    The attributes intended for end-user retrieval have been made into read-only 
    property attributes.

    Parameters:
    -----------
    mlbam : int (required)
        Official "MLB Advanced Media" ID for a player

    ---------------------------------------------------------------------------
    
    Accessing the data
    ------------------
    Data can be accessed by using standard dot-notation (e.g. `person.name`) 
    or by treating the class as a subscriptable python object 
    (e.g. `person["name"]`).

    Many of the properties are subscriptable wrappers for different types of 
    data (See examples below). For convenience and where it is applicable, 
    if the wrapper is not accessing a child attribute, a string representation 
    of the object is returned. Additionally, some wrappers (like 'mlb_date') 
    are callable and will return a non-string object. For example, 
    `person.birth.date` returns a date string ('YYYY-mm-dd') while 
    `person.birth.date()` returns a datetime.date object

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

    def __new__(cls, mlbam: int, **kwargs):
        self = object.__new__(cls)
        _pd_df = pd.DataFrame
        data = _player_data(mlbam)

        _bio: Union[list, None] = data["bio"]
        _info: dict = data["info"]
        _trx: _pd_df = data["transactions"]
        _stats: dict = data["stats"]
        _teams: dict = data["teams"]
        _awards: _pd_df = data["awards"]
        _past_teams: pd.DataFrame = data["past_teams"]
        self.__bats = _info['bats']
        self.__throws = _info['throws']
        self.__weight = _info['weight']
        self.__height = _info['height']
        # name info
        _name_data = {
            "_given": _info["givenName"],
            "_full": _info["fullName"],
            "_first": _info["firstName"],
            "_middle": _info["middleName"],
            "_last": _info["lastName"],
            "_nick": _info["nickName"],
            "_pronunciation": _info["pronunciation"],
        }

        # position info
        pos = _info["primary_position"]
        _position_data = {
            "__code": pos.get("code", "-"),
            "__name": pos.get("name", "-"),
            "__type": pos.get("type", "-"),
            "__abbreviation": pos.get("abbreviation", "-"),
        }

        # birth info
        _birth_data = {
            "date": md(_info["birthDate"]),
            "city": _info["birthCity"],
            "state": _info["birthState"],
            "province": _info["birthState"],
            "country": _info["birthCountry"],
        }

        # death info
        _death_data = {
            "date": md(_info["deathDate"]),
            "city": _info["deathCity"],
            "state": _info["deathState"],
            "province": _info["deathState"],
            "country": _info["deathCountry"],
        }

        # debut game info
        _deb: list = _info["debut_data"]

        if len(_deb) > 0:
            gm_data = _deb[0]["splits"][0]
            team = gm_data.get("team", {})
            opponent = gm_data.get("opponent", {})
            gamepk = gm_data["game"]["gamePk"]
            _debut_data = {
                "date": md(_info["first_game"]),
                "gamepk": gamepk,
                "game_id": gamepk,
                "game_pk": gamepk,
                "gamePk": gamepk,
                "team": team_name_data(
                    _full=team.get("name"),
                    _mlbam=team.get("id"),
                    _short=team.get("shortName"),
                    _club=team.get("clubName"),
                    _location=team.get("locationName"),
                    _franchise=team.get("franchiseName"),
                    _season=team.get("season"),
                    _slug=f"{team.get('clubName').lower().replace(' ','-')}-{team.get('id')}",
                ),
                "opponent": team_name_data(
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
            _debut_data = {"date": md(data["first_game"])}

        # last game info
        _last_game_data = {
            "date": md(data.get("lastPlayedDate", "-")),
        }

        self._mlbam = _info["mlbam"]
        self._bio = _bio
        self._name = person_name_data(**_name_data)
        self.__position = player_position_wrapper(**_position_data)
        self._birth = mlb_wrapper(**_birth_data)
        self._death = mlb_wrapper(**_death_data)
        self._debut = mlb_wrapper(**_debut_data)
        self._last_game = mlb_wrapper(**_last_game_data)

        self._zone_top = _info["zoneTop"]
        self._zone_bot = _info["zoneBot"]

        # transaction df
        self._trx = _trx

        # awards df
        self._awards = _awards

        # stats info
        self._stats = player_stats(
            hit_car_reg=_stats["hitting"]["career"],
            hit_car_adv=_stats["hitting"]["career_advanced"],
            pit_car_reg=_stats["pitching"]["career"],
            pit_car_adv=_stats["pitching"]["career_advanced"],
            fld_car_reg=_stats["fielding"]["career"],
            hit_yby_reg=_stats["hitting"]["yby"],
            hit_yby_adv=_stats["hitting"]["yby_advanced"],
            pit_yby_reg=_stats["pitching"]["yby"],
            pit_yby_adv=_stats["pitching"]["yby_advanced"],
            fld_yby_reg=_stats["fielding"]["yby"],
        )

        # roster entries
        self._roster_entries = _info["roster_entries"]

        _past_teams_data = []
        for idx in range(len(_past_teams)):
            t = _past_teams.iloc[idx]
            mlb_tm = mlb_team(
                raw_data=t.to_dict(),
                mlbam=t["mlbam"],
                full=t["full"],
                season=t["season"],
                location=t["location"],
                franchise=t["franchise"],
                mascot=t["mascot"],
                club=t["club"],
                short=t["short"],
                lg_mlbam=t["lg_mlbam"],
                lg_name_full=t["lg_name_full"],
                lg_name_short=t["lg_name_short"],
                lg_abbrv=t["lg_abbrv"],
                div_mlbam=t["div_mlbam"],
                div_name_full=t["div_name_full"],
                div_name_short=t["div_name_short"],
                div_abbrv=t["div_abbrv"],
                venue_mlbam=t["venue_mlbam"],
                venue_name=t["venue_name"],
            )
            _past_teams_data.append(mlb_tm)
        self._past_teams = pd.DataFrame.from_dict(_past_teams)

        # education info
        edu: pd.DataFrame = _info["education"]

        self._edu = edu_wrapper(edu_df=edu)

        return self

    def __str__(self):
        return self._name.full

    def __repr__(self):
        return f"({self._name.full}, {self._mlbam})"

    def __len__(self) -> int:
        max_ = 0
        for grp in (self.stats.hitting, self.stats.pitching, self.stats.fielding):
            try:
                ct = len(set(grp.yby.regular["season"]))
                if ct > max_:
                    max_ = ct
            except:
                pass
        return max_

    def __getitem__(self, _name: str):
        return self.__dict__[f"_{_name}"]

    @property
    def mlbam(self) -> int:
        """Player's official \"MLB Advanced Media\" ID"""
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
    def height(self):
        """Player's height"""
        return self.__height
    
    @property
    def weight(self):
        """Player's weight"""
        return self.__weight

    @property
    def position(self) -> player_position_wrapper:
        """Wrapper for player's primary position

        Keys/Attributes:
        ----------------
        - 'code' : int (3)
        - 'name' : str ('First Base')
        - 'type' (or 'province') : str ('Infielder')
        - 'abbreviation' : str ('1B')
            - 'abbrv' ALIAS
        """
        return self.__position
    
    @property
    def bats(self) -> str:
        """Code for for player's batting side ('R','L','S')"""
        return self.__bats
    
    @property
    def throws(self) -> str:
        """Code for for player's throwing arm ('R','L','S')"""
        return self.__throws

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
        
        Notes:
        ------

        The official API differentiates two subtypes of each stat type. 
        For example, the 'career' stat type retrieves *standard* statistics of 
        a player's entire MLB career. 'careerAdvanced' retrieves more advanced 
        statistics of the same stat type (some stat keys may overlap).  
        For this reason, a "subtype" must be specified when retrieving a 
        dataframe of statistics using '.regular' or '.advanced' ('reg' | 'adv' 
        for short hand). See example below. Please Note that the "fielding" 
        stat group does not have "advanced" stat data. `fielding.yby.advanced` 
        returns standard stat data.

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
    def transactions(self) -> Union[pd.DataFrame, None]:
        """Dataframe of transactions involving person, i.e. trades, status 
        changes, etc."""
        return self._trx

    @property
    def trx(self) -> transactions:
        """Dataframe of transactions involving person, i.e. trades, status 
        changes, etc.

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
    
    The "mlb.Franchise" class is a comprehensive wrapper for retrieving and 
    accesing data for a team's franchise in bulk fashion. All data is pulled 
    from the Official MLB Stats API (https://statsapi.mlb.com/api/v1). The 
    attributes intended for end-user retrieval have been made into read-only 
    property attributes.

    Parameters
    ----------

    mlbam : int | str (required)
        Official "MLB Advanced Media" ID for franchise (team)

    ### See also:
    - 'mlb.Team()'
    - 'mlb.teams()'

    """

    def __init__(self, mlbam: int):
        data = _franchise_data(int(mlbam))

        records       = data["records"]
        record_splits = data["record_splits"]  # like standings splits
        yby_data      = data["yby_data"]
        team_info     = data["team_info"]
        hitting       = data["hitting"]
        hitting_adv   = data["hitting_advanced"]
        pitching      = data["pitching"]
        pitching_adv  = data["pitching_advanced"]
        fielding      = data["fielding"]
        roster        = data["all_players"]
        hof           = data["hof"]
        retired       = data["retired_numbers"]
        records_df    = data["records_df"]
        splits_df     = data["splits_df"]

        ti = team_info
        self.__mlbam = ti["mlbam"]
        self.__franchise = ti["franchise_name"]
        self.__name = mlb_wrapper(
            full=ti["full_name"],
            location=ti["location_name"],
            franchise=self.__franchise,
            mascot=ti["team_name"],
            club=ti["club_name"],
            short=ti["short_name"],
        )

        self.__league: league_name_wrapper = league_name_wrapper(
            _mlbam=ti["league_mlbam"],
            _name=ti["league_name"],
            _short=ti["league_short"],
            _abbrv=ti["league_abbrv"],
        )
        self.__division: league_name_wrapper = league_name_wrapper(
            _mlbam=ti["div_mlbam"],
            _name=ti["div_name"],
            _short=ti["div_short"],
            _abbrv=ti["div_abbrv"],
        )
        self.__venue: venue_name_wrapper = venue_name_wrapper(
            _name=ti["venue_name"], _mlbam=ti["venue_mlbam"]
        )
        # self.__standings = standings_wrapper(records=records, splits=record_splits)
        self.__standings = standings_wrapper(records=records_df, splits=splits_df)

        self.__yby_data = yby_data

        self.__hitting = mlb_wrapper(reg=hitting, adv=hitting_adv)
        self.__pitching = mlb_wrapper(reg=pitching, adv=pitching_adv)
        self.__fielding = mlb_wrapper(reg=fielding)

        self.__stats = mlb_wrapper(
            hitting=self.__hitting, pitching=self.__pitching, fielding=self.__fielding
        )

        self.__legends: pd.DataFrame = hof
        self.__retired: pd.DataFrame = retired

        self.__roster = roster_wrapper(
            all=roster,
            pitcher=roster[roster["pos"] == "P"].reset_index(drop=True),
            catcher=roster[roster["pos"] == "C"].reset_index(drop=True),
            first=roster[roster["pos"] == "1B"].reset_index(drop=True),
            second=roster[roster["pos"] == "2B"].reset_index(drop=True),
            third=roster[roster["pos"] == "3B"].reset_index(drop=True),
            short=roster[roster["pos"] == "SS"].reset_index(drop=True),
            left=roster[roster["pos"] == "LF"].reset_index(drop=True),
            center=roster[roster["pos"] == "CF"].reset_index(drop=True),
            right=roster[roster["pos"] == "RF"].reset_index(drop=True),
            dh=roster[roster["pos"] == "DH"].reset_index(drop=True),
            designated_hitter=roster[roster["pos"] == "DH"].reset_index(drop=True),
            infield=roster[roster["pos"].isin(["1B", "2B", "3B", "SS"])].reset_index(
                drop=True
            ),
            outfield=roster[roster["pos"].isin(["OF", "LF", "CF", "RF"])].reset_index(
                drop=True
            ),
            active=roster[roster["status"] == "Active"],
        )

        self.__first_year = int(ti["first_year"])
        self.__last_year = int(ti.get("last_year", default_season()))

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
        Retrieve data for a specific season. Default value (None) retrieves 
        data for the most recent season

    """

    def __new__(cls, mlbam: int, season: Optional[int] = None, **kwargs):
        cls.today = md(dt.datetime.today().date().strftime(r"%Y-%m-%d"))

        if season is None:
            season = default_season()

        cls.mlbam = int(mlbam)
        cls.season = int(season)

        data: Union[dict, None] = _team_data(cls.mlbam, cls.season)
        cls.raw_data = data

        ti: dict = data["team_info"]

        cls.name = mlb_wrapper(
            full=ti["full_name"],
            location=ti["location_name"],
            franchise=ti["franchise_name"],
            mascot=ti["team_name"],
            club=ti["club_name"],
            short=ti["short_name"],
        )
        cls.league = mlb_wrapper(
            mlbam=ti["league_mlbam"],
            name=ti["league_name"],
            short=ti["league_short"],
            abbrv=ti["league_abbrv"],
        )
        cls.division = mlb_wrapper(
            mlbam=ti["div_mlbam"],
            name=ti["div_name"],
            short=ti["div_short"],
            abbrv=ti["div_abbrv"],
        )
        cls.venue = mlb_wrapper(name=ti["venue_name"], mlbam=ti["venue_mlbam"])

        cls.schedule: pd.DataFrame = data["schedule"]
        cls.drafts: pd.DataFrame = data["drafts"]
        cls.transactions: pd.DataFrame = data["transactions"]
        cls.coaches: pd.DataFrame = data["coaches"]

        cls._stats = team_stats(
            totals={
                "hit_reg": data["total_hitting_reg"],
                "hit_adv": data["total_hitting_adv"],
                "pit_reg": data["total_pitching_reg"],
                "pit_adv": data["total_pitching_reg"],
                "fld_reg": data["total_fielding_reg"],
            },
            roster={
                "hit_reg": data["hitting_reg"],
                "hit_adv": data["hitting_adv"],
                "pit_reg": data["pitching_reg"],
                "pit_adv": data["pitching_adv"],
                "fld_reg": data["fielding_reg"],
            },
        )

        self = object.__new__(cls)
        return self

    def __str__(self):
        return self.name.full

    def __repr__(self):
        return self.name.full

    def __call__(self, _team_key: str):
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
        return getattr(self, __name)

    def __setattr__(self, _attr, _value):
        raise AttributeError(f"Attribute assingment not supported for team object")

    def get_splits(self):
        pass

    @property
    def stats(self):
        """Wrapper for both team's cumulative & player-level stats"""
        return self._stats


# ===============================================================
# API Wrapper | Function | Classes
# ===============================================================


class api:

    def get(path: str, hydrate=None, **query_parameters):
        """## get
        
        Create your own customized API calls to https://statsapi.mlb.com

        ----------------------------------------------------------------

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
        if path[0] == "/":
            path = path[1:]

        params = query_parameters

        url = f"{BASE}/{path}"

        req = requests.Request("GET", url, params=params).prepare()

    def prepare(path, **query_params):
        """Build"""
        pass

    class teams:
        def __new__(cls, _mlbam):
            self = object.__new__(cls)
            self._mlbam = _mlbam


    class leagues:
        def __init__(self, _mlbam):
            self._mlbam = _mlbam

    @classmethod
    def player_search(
        cls,
        names: Optional[str] = None,
        teamIds: Optional[Union[str, list]] = None,
        **kwargs,
    ):
        """Search for a person using the API by name

        Paramaters
        ----------

        name : str (required)
            Player name to search

        personIds
            Insert personId(s) to search and return biographical information 
            for a specific player(s). Format '605151,592450'

        leagueIds
            Insert leagueId(s) to search and return biographical information 
            for players in a specific league(s).

        teamIds
            Insert teamId(s) to search and return biographical information for 
            players on a specific team(s).

        hydrate : str
            Insert hydration(s) to return statistical or biographical data for 
            a specific player(s).

        """

        url = f"https://statsapi.mlb.com/api/v1/people/search?"

        params = {"sportId": 1}

        if names is not None:
            params["names"] = names
        else:
            if kwargs.get("season") is not None:
                url = f"https://statsapi.mlb.com/api/v1/sports/1/players?"
                params["season"] = kwargs["season"]
        if kwargs.get("hydrate") is not None:
            params["hydrate"] = kwargs["hydrate"]

        if teamIds is not None:
            if type(teamIds) is str:
                teamIds = teamIds.replace(", ", ",")
            elif type(teamIds) is list:
                teamIds = ",".join(teamIds).replace(", ", ",")
            params["teamIds"] = teamIds

        resp = requests.get(url, params=params)

        parsed_data = []
        for p_dict in resp.json()["people"]:
            parsed_data.append(pd.Series(_parse_person(_obj=p_dict)))

        df = pd.DataFrame(data=parsed_data).reset_index(drop=True)
        return _people_data_collection(df.fillna("-"))

    @classmethod
    def team_search(
        cls,
        query: str,
        season: Optional[Union[int, None]] = None,
        hydrate: Optional[Union[str, None]] = None,
    ):
        if season is None:
            season = default_season()
        if hydrate is not None:
            if type(hydrate) is str:
                hydrations = hydrate.replace(", ", ",")
            elif type(hydrate) is list:
                hydrations = ",".join(hydrations)
            hydrations = f"&hydrate={hydrations}"
        else:
            hydrations = ""
        resp = requests.get(f"{BASE}/teams?sportId=1&season={season}{hydrations}")

        for t in resp.json()["teams"]:
            if query.lower() in t.get("name").lower():
                return mlb_team(raw_data=t, **_parse_team(t))

    def get_teams(
        start_season: Optional[int] = None,
        end_season: Optional[int] = None,
        hydrate_league=False,
        hydrate_division=False,
    ):
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
            hydrations.append("league")
        if hydrate_division is True:
            hydrations.append("division")

        if len(hydrations) != 0:
            hydrations = ",".join(hydrations)
            hydrations = f"&hydrate={hydrations}"
        else:
            hydrations = ""

        for y in range(start_season, end_season + 1):
            urls.append(
                f"https://statsapi.mlb.com/api/v1/teams?sportId=1&season={y}{hydrations}"
            )

        # async
        # loop = _determine_loop()
        # fetched_data = loop.run_until_complete(_fetch(urls))
        fetched_data = _fetch(urls)

        parsed_data = []

        # data gets parsed by '_parse_team' function from misc.py
        for d in fetched_data:
            # each 'd' value is a JSON response
            list_of_team_dicts: List[dict] = d["teams"]
            for tm_dict in list_of_team_dicts:
                parsed_data.append(pd.Series(_parse_team(_obj=tm_dict)))

        df = (
            pd.DataFrame(data=parsed_data)
            .reset_index(drop=True)
            .sort_values(by=["season", "name_full"], ascending=[False, True])
        )
        return _teams_data_collection(df)
