from .utils import dt
from .utils import pd
from .utils import keys


class mlb_date:
    """datetime.date wrapper for mlb date representations
    
    Properties
    ----------
    - 'month'
    - 'day'
    - 'year'
    - 'day_of_week' | 'dow'
    - 'day_of_week_short' | 'dow_short'
    - 'month_name'
    - 'month_name_short'
    - 'obj'
    - 'string'
    - 'full'
    - 'short'
    """
    def __init__(self,_date_string:str):
        if _date_string != "-":
            d = dt.datetime.strptime(_date_string,r"%Y-%m-%d").date()
            self.__date_obj = d
            self.__date_str    = self.__date_obj.strftime(r"%Y-%m-%d")
            self.__month_name  = d.strftime(r"%B")
            self.__month_name_short = d.strftime(r"%b")
            self.__dow         = d.strftime(r"%A")
            self.__dow_short   = d.strftime(r"%a")
            self.__day         = str(int(d.strftime(r"%d")))
            self.__month       = d.strftime(r"%m")
            self.__year        = d.strftime(r"%Y")
            self.__full_date = f'{self.__month_name} {self.__day}, {self.__year}'
            self.__short_date = f'{self.__month_name_short} {self.__day}, {self.__year}'
            
        else:
            self.__date_obj = None
            self.__date_str    = '-'
            self.__month_name  = '-'
            self.__month_name_short  = '-'
            self.__dow         = '-'
            self.__dow_short   = '-'
            self.__month       = 0
            self.__day         = 0
            self.__year        = 0
            self.__full_date   = '-'
            self.__short_date  = '-'
            
    def __repr__(self):
        return self.__date_str

    def __str__(self):
        return self.__date_str
    
    def __call__(self) -> dt.date | None:
        """Returns datetime.date object from built-in datetime module"""
        return self.__date_obj

    @property
    def month(self) -> int:
        """Returns month as integer (1-12)"""
        return self.__month
    
    @property
    def day(self) -> int:
        """Returns day as integer (1-31)"""
        return self.__day

    @property
    def year(self) -> int:
        """Returns year as integer"""
        return self.__year

    @property
    def day_of_week(self) -> str:
        """Returns day of week as string (Example: "Wednesday")"""
        return self.__dow
    
    @property
    def dow(self) -> str:
        """Returns day of week as string (Example: "Wednesday")
        
        ALIAS for 'day_of_week'
        """
        return self.__dow
    
    @property
    def day_of_week_short(self) -> str:
        """Returns shortened name of day of week as string (Example: "Wed")"""
        return self.__dow_short
    
    @property
    def dow_short(self) -> str:
        """Returns shortened name of day of week as string (Example: "Wed")"""
        return self.__dow_short
    
    @property
    def month_name(self) -> str:
        """Returns full month name as string (Example: "February")"""
        return self.__month_name

    @property
    def month_name_short(self) -> str:
        """Returns shortened month name as string (Example: "Feb")"""
        return self.__month_name_short

    @property
    def obj(self) -> dt.date | None:
        """Returns datetime.date object from built-in datetime module"""
        return self.__date_obj
    
    @property
    def date_obj(self) -> dt.date | None:
        """Returns datetime.date object from built-in datetime module"""
        return self.__date_obj

    @property
    def full(self) -> str:
        """Returns full date string (Example: "February 3, 2021")"""
        return self.__full_date

    @property
    def short(self) -> str:
        """Returns short date string (Example: "Feb 3, 2021")"""
        return self.__short_date

class mlb_data_wrapper:
    """### Data wrapper
    Namespace/subscriptable object for interacting with nested data
    
    """
    def __init__(self,**kwargs):
        self.__dict__.update(kwargs)
    
    def __getitem__(self, __name: str):
        return getattr(self,__name)
        
    def __call__(self,_attr):
        return getattr(self,_attr)


class venue_name_wrapper(mlb_data_wrapper):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        # might add "historic" properties for venue
    
    def __str__(self) -> str:
        return self._name
    
    def __repr__(self):
        return self._name
    
    def __call__(self,attr:str|None=None) -> str | int:
        if attr is None:
            return self._name
        else:
            return self.__dict__[f'_{attr}']
    
    @property
    def name(self) -> str:
        """The name of the venue"""
        return str(self._name)
    
    @property
    def mlbam(self) -> int:
        """The venue's official MLB ID"""
        return int(self._mlbam)


class league_name_wrapper(mlb_data_wrapper):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
    
    def __str__(self) -> str:
        return self._name
    
    def __repr__(self):
        return self._name
    
    def __call__(self,attr:str|None=None) -> str | int:
        if attr is None:
            return self._name
        else:
            return self.__dict__[f'_{attr}']
    
    @property
    def mlbam(self) -> int:
        """League's official MLB ID"""
        return int(self._mlbam)

    @property
    def name(self) -> str:
        """League's full name"""
        return str(self._name)

    @property
    def short(self) -> str:
        """League short name"""
        return str(self._short)

    @property
    def abbrv(self) -> str:
        """League abbreviation"""
        return str(self._abbrv)

    @property
    def abbreviation(self) -> str:
        """League abbreviation"""
        return str(self._abbrv)



class team_name_data(mlb_data_wrapper):
    """A wrapper containing data on a variations of a TEAM's name
    """
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        
    def __str__(self) -> str:
        return self.full
    
    def __repr__(self) -> str:
        return self.full
    
    @property
    def mlbam(self):
        """Team's official MLB ID"""
        return self._mlbam

    @property
    def full(self):
        """Team's full name"""
        return self._full

    @property
    def short(self):
        """Team's shortened name"""
        try: return self._short
        except: return '-'

    @property
    def franchise(self):
        """Team's parent franchise org name"""
        try: return self._franchise
        except: return '-'
    
    @property
    def location(self):
        """Team's location (typically the city or state that a club is based)"""
        return self._location

    @property
    def club(self):
        """Team's club name"""
        return self._club
    
    @property
    def slug(self):
        """Team's slug name (typcally appears in the URL path of a team's page on a website)"""
        return self._slug
    
    @property
    def season(self):
        """Season of play"""
        try: return self._season
        except: return '-'


class team_data_wrapper_slim(mlb_data_wrapper):
    """simple wrapper for holding small amounts of team data"""
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self._mlbam = kwargs['mlbam']
        self._name : team_name_data = team_name_data(
            _full=kwargs['full'],
            _season=kwargs['season'],
            _location=kwargs['location'],
            _franchise=kwargs['franchise'],
            _mascot=kwargs['mascot'],
            _club=kwargs['club'],
            _short=kwargs['short']
        )
        self._league : league_name_wrapper = league_name_wrapper(
            _mlbam=kwargs['lg_mlbam'],
            _name=kwargs['lg_name_full'],
            _short=kwargs['lg_name_short'],
            _abbrv=kwargs['lg_abbrv']
        )
        self._division : league_name_wrapper = league_name_wrapper(
            _mlbam=kwargs['div_mlbam'],
            _name=kwargs['div_name_full'],
            _short=kwargs['div_name_short'],
            _abbrv=kwargs['div_abbrv']
        )
        self._venue : venue_name_wrapper = venue_name_wrapper(
            _mlbam=kwargs['venue_mlbam'],
            _name=kwargs['venue_name']
        )
    
    @property
    def mlbam(self):
        """Team's official MLB ID"""
        return self._mlbam
    
    @property
    def name(self):
        """Various names team names/aliases"""
        return self._name
    
    @property
    def league(self):
        """Information for team's league
        
        NOTE: The 'name' attribute from the league wrapper can also be retrieved by simply calling this property
        """
        return self._league
    
    @property
    def division(self):
        """Information for team's division
        
        NOTE: The 'name' attribute from the league wrapper can also be retrieved by simply calling this property
        """
        return self._division
    
    @property
    def venue(self):
        """Info for team's venue"""
        return self._venue
    
class person_name_data(mlb_data_wrapper):
    """A wrapper containing data on a variations of a PERSON's name"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def __str__(self):
        return self.full
    
    def __repr__(self):
        return self.full
    
    @property
    def full(self):
        """Person's full name"""
        return self._full
    
    @property
    def given(self):
        """Person's given name (e.g. - 'FIRST MIDDLE LAST)"""
        return self._given
    
    @property
    def first(self):
        """Person's first name"""
        return self._first
    
    @property
    def middle(self):
        """Person's middle name (may sometimes just be the middle initial)"""
        return self._middle
    
    @property
    def last(self):
        """PPerson's last name"""
        return self._last
    
    @property
    def nick(self):
        """Person's nick name (if player has one)"""
        return self._nick
    
    @property
    def pronunciation(self):
        """Pronunciation of person's LAST name"""
        return self._pronunciation

# class _stat_df:
#     def __init__(self,df:pd.DataFrame):
#         self.__df = df
        
#     def __call__(self, filter_col, filter_val:str|None=None):
#         df = self.__df
#         if filter_col == 'game_type':
#             return df[df[filter_col]==filter_val]
    
class stat_collection(mlb_data_wrapper):
    def __init__(
        self,
        stat_group:str,
        career_regular:pd.DataFrame|None=None,career_advanced:pd.DataFrame|None=None,
        season_regular:pd.DataFrame|None=None,season_advanced:pd.DataFrame|None=None,
        yby_regular:pd.DataFrame|None=None,yby_advanced:pd.DataFrame|None=None):

        if stat_group == 'hitting':
            self.__cols_reg = keys.hit
            self.__cols_adv = keys.hit_adv
        elif stat_group == 'pitching':
            self.__cols_reg = keys.pitch
            self.__cols_adv = keys.pitch_adv
        elif stat_group == 'fielding':
            self.__cols_reg = keys.field
            self.__cols_adv = keys.field
        
        else:
            self.__cols_reg = {'hitting':keys.hit,'pitching':keys.pitch,'fielding':keys.field}
            self.__cols_adv = {'hitting':keys.hit_adv,'pitching':keys.pitch_adv,'fielding':keys.field}
            
        self.__regular = mlb_data_wrapper(
            career=career_regular,
            season=season_regular,
            yby=yby_regular
        )
        self.__advanced = mlb_data_wrapper(
            career=career_advanced,
            season=season_advanced,
            yby=yby_advanced
        )
        
        self.__career = mlb_data_wrapper(
            regular=career_regular,
            advanced=career_advanced
        )
        self.__season = mlb_data_wrapper(
            regular=season_regular,
            advanced=season_advanced
        )
        self.__yby = mlb_data_wrapper(
            regular=yby_regular,
            advanced=yby_advanced
        )
    
    @property
    def season(self):
        return self.__season
    
    @property
    def career(self):
        return self.__career
    
    @property
    def yby(self):
        return self.__yby
    
    @property
    def regular(self):
        return self.__regular
    
    @property
    def standard(self):
        return self.__regular
    
    @property
    def advanced(self):
        return self.__advanced
        
    @property
    def regular_cols(self):
        return self.__cols_reg
    
    @property
    def advanced_cols(self):
        return self.__cols_adv
    
    @property
    def group(self):
        return self.__group


class player_stats(mlb_data_wrapper):
    def __init__(self,
                 hit_car_reg:pd.DataFrame|None=None,hit_car_adv:pd.DataFrame|None=None,
                 hit_ssn_reg:pd.DataFrame|None=None,hit_ssn_adv:pd.DataFrame|None=None,
                 hit_yby_reg:pd.DataFrame|None=None,hit_yby_adv:pd.DataFrame|None=None,
                 
                 pit_car_reg:pd.DataFrame|None=None,pit_car_adv:pd.DataFrame|None=None,
                 pit_ssn_reg:pd.DataFrame|None=None,pit_ssn_adv:pd.DataFrame|None=None,
                 pit_yby_reg:pd.DataFrame|None=None,pit_yby_adv:pd.DataFrame|None=None,
                 
                 fld_car_reg:pd.DataFrame|None=None,
                 fld_ssn_reg:pd.DataFrame|None=None,
                 fld_yby_reg:pd.DataFrame|None=None
                 ):
        super().__init__(**{'hit_car_reg':hit_car_reg,'hit_car_adv':hit_car_adv,
                            'hit_ssn_reg':hit_ssn_reg,'hit_ssn_adv':hit_ssn_adv,
                            'hit_yby_reg':hit_yby_reg,'hit_yby_adv':hit_yby_adv,
                            
                            'pit_car_reg':pit_car_reg,'pit_car_adv':pit_car_adv,
                            'pit_ssn_reg':pit_ssn_reg,'pit_ssn_adv':pit_ssn_adv,
                            'pit_yby_reg':pit_yby_reg,'pit_yby_adv':pit_yby_adv,
                            
                            'fld_car_reg':fld_car_reg,
                            'fld_ssn_reg':fld_ssn_reg,
                            'fld_yby_reg':fld_yby_reg,
                            })
        
        self.__all_stats_dict = {'hitting':{'career':[hit_car_reg,hit_car_adv],
                                            'yby':[hit_yby_reg,hit_yby_adv],
                                            'season':[hit_ssn_reg,hit_ssn_adv]
                                            },
                                 'pitching':{'career':[pit_car_reg,pit_car_adv],
                                            'yby':[pit_yby_reg,pit_yby_adv],
                                            'season':[pit_ssn_reg,pit_ssn_adv]
                                            },
                                 'fielding':{'career':[fld_car_reg,fld_car_reg],
                                            'yby':[fld_yby_reg,fld_yby_reg],
                                            'season':[fld_ssn_reg,fld_ssn_reg]
                                            },
                                 }
    
        
        self.__hitting = stat_collection('hitting',
            career_regular  = hit_car_reg,career_advanced = hit_car_adv,
            season_regular  = hit_ssn_reg,season_advanced = hit_ssn_adv,
            yby_regular     = hit_yby_reg,yby_advanced    = hit_yby_adv
        )
        self.__pitching = stat_collection('pitching',
            career_regular  = pit_car_reg,career_advanced = pit_car_adv,
            season_regular  = pit_ssn_reg,season_advanced = pit_ssn_adv,
            yby_regular     = pit_yby_reg,yby_advanced    = pit_yby_adv
        )
        self.__fielding = stat_collection('fielding',
            career_regular  = fld_car_reg,
            season_regular  = fld_ssn_reg,
            yby_regular     = fld_yby_reg,
        )
        
    def __call__(self,stat_group:str,stat_type:str,filter_by:str|None=None,filter_val:str|None=None,advanced:bool|str=False,**kwargs):
        """Get stats data through class call
        
        Parameters:
        -----------
        stat_group : str (required)
            specify a stat group ('hitting','pitching' or 'fielding')
        
        stat_type : str (required)
            specify a stat type ('career', 'yby', 'season')
        
        advanced : bool (required, Default is False)
        
        filter_by : str | None
        
        """
        try:
            group_dict = self.__all_stats_dict[stat_group]
            stat_selection = group_dict[stat_type]
            idx = 0
            if kwargs.get('adv') is not None:
                advanced = kwargs['adv']
            if (advanced is True) or ('adv' in str(advanced)):
                idx = 1
            df = stat_selection[idx]
            
            if filter_by is not None:
                df = df[df[filter_by]==filter_val]
            
            return df
        except:
            if kwargs.get('exception_val') is not None:
                return kwargs['exception_val']
            else:
                return None
    
    def get(self,stat_group:str,stat_type:str,filter_by:str|None=None,filter_val:str|None=None,advanced:bool|str=False,**kwargs):
        """Get stats data through class call
        
        Parameters:
        -----------
        stat_group : str (required)
            specify a stat group ('hitting','pitching' or 'fielding')
        
        stat_type : str (required)
            specify a stat type ('career', 'yby', 'season')
        
        advanced : bool (required, Default is False)
        
        filter_by : str | None
        
        """
        try:
            group_dict = self.__all_stats_dict[stat_group]
            stat_selection = group_dict[stat_type]
            idx = 0
            if kwargs.get('adv') is not None:
                advanced = kwargs['adv']
            if (advanced is True) or ('adv' in str(advanced)):
                idx = 1
            df = stat_selection[idx]
            
            if filter_by is not None:
                df = df[df[filter_by]==filter_val]
            
            return df
        except:
            if kwargs.get('exception_val') is not None:
                return kwargs['exception_val']
            else:
                return None
    
    @property
    def hitting(self):
        """Player's hitting stats (regular or advanced)
        """
        return self.__hitting
    
    @property
    def pitching(self):
        """Player's pitching stats (regular or advanced)
        """
        return self.__pitching
    
    @property
    def fielding(self):
        """Player's fielding stats
        """
        return self.__fielding
    
    
class team_stats(mlb_data_wrapper):
    def __init__(self,
                 hit_car_reg:pd.DataFrame|None=None,hit_car_adv:pd.DataFrame|None=None,
                 hit_ssn_reg:pd.DataFrame|None=None,hit_ssn_adv:pd.DataFrame|None=None,
                 hit_yby_reg:pd.DataFrame|None=None,hit_yby_adv:pd.DataFrame|None=None,
                 
                 pit_car_reg:pd.DataFrame|None=None,pit_car_adv:pd.DataFrame|None=None,
                 pit_ssn_reg:pd.DataFrame|None=None,pit_ssn_adv:pd.DataFrame|None=None,
                 pit_yby_reg:pd.DataFrame|None=None,pit_yby_adv:pd.DataFrame|None=None,
                 
                 fld_car_reg:pd.DataFrame|None=None,
                 fld_ssn_reg:pd.DataFrame|None=None,
                 fld_yby_reg:pd.DataFrame|None=None
                 ):
        super().__init__(**{'hit_car_reg':hit_car_reg,'hit_car_adv':hit_car_adv,
                            'hit_ssn_reg':hit_ssn_reg,'hit_ssn_adv':hit_ssn_adv,
                            'hit_yby_reg':hit_yby_reg,'hit_yby_adv':hit_yby_adv,
                            
                            'pit_car_reg':pit_car_reg,'pit_car_adv':pit_car_adv,
                            'pit_ssn_reg':pit_ssn_reg,'pit_ssn_adv':pit_ssn_adv,
                            'pit_yby_reg':pit_yby_reg,'pit_yby_adv':pit_yby_adv,
                            
                            'fld_car_reg':fld_car_reg,
                            'fld_ssn_reg':fld_ssn_reg,
                            'fld_yby_reg':fld_yby_reg,
                            })
        
        self.__hitting = stat_collection('hitting',
            career_regular  = hit_car_reg,career_advanced = hit_car_adv,
            season_regular  = hit_ssn_reg,season_advanced = hit_ssn_adv,
            yby_regular     = hit_yby_reg,yby_advanced    = hit_yby_adv
        )
        self.__pitching = stat_collection('pitching',
            career_regular  = pit_car_reg,career_advanced = pit_car_adv,
            season_regular  = pit_ssn_reg,season_advanced = pit_ssn_adv,
            yby_regular     = pit_yby_reg,yby_advanced    = pit_yby_adv
        )
        self.__fielding = stat_collection('fielding',
            career_regular  = fld_car_reg,
            season_regular  = fld_ssn_reg,
            yby_regular     = fld_yby_reg,
        )
    
    @property
    def hitting(self):
        """Player's hitting stats (regular or advanced)
        """
        return self.__hitting
    
    @property
    def pitching(self):
        """Player's pitching stats (regular or advanced)
        """
        return self.__pitching
    
    @property
    def fielding(self):
        """Player's fielding stats
        """
        return self.__fielding
    
    
    