from .utils import dt
from .utils import pd
from .utils import keys


class _mlb_date:
    """datetime.date wrapper for mlb date representations

    """
    def __init__(self,_date_string):
        if _date_string != "-":
            d = dt.datetime.strptime(_date_string,r"%Y-%m-%d").date()
            self.__date_obj = d
            self.__date_str    = self.__date_obj.strftime(r"%Y-%m-%d")
            self.__month_name  = d.strftime(r"%B")
            self.__dow         = d.strftime(r"%A")
            self.__day         = d.strftime(r"%d")
            self.__month       = d.strftime(r"%m")
            self.__year        = d.strftime(r"%Y")
            
        else:
            self.__date_obj = None
            self.__date_str    = '-'
            self.__month_name  = '-'
            self.__dow         = '-'
            self.__month       = 0
            self.__day         = 0
            self.__year        = 0

    def __repr__(self):
        return self._date_str

    def __str__(self):
        return self._date_str
    
    def __call__(self) -> dt.date | None:
        """Return datetime.date object"""
        return self._date_obj

    @property
    def month(self) -> int:
        return self.__month
    
    @property
    def day(self) -> int:
        return self.__day

    @property
    def year(self) -> int:
        return self.__year

    @property
    def day_of_week(self) -> str:
        return self.__dow
    
    @property
    def month_name(self) -> str:
        return self.__month_name

    @property
    def _date_obj(self) -> dt.date | None:
        return self.__date_obj

    @property
    def _date_str(self) -> str:
        return self.__date_str

class _mlb_data_wrapper:
    """### wrapper
    Namespace/subscriptable object for interacting with nested data
    
    """
    def __init__(self,**kwargs):
        self.__dict__.update(kwargs)
    
    def __getitem__(self, __name: str):
        return getattr(self,__name)
        
    def __call__(self,_attr):
        return getattr(self,_attr)

class _venue_data(_mlb_data_wrapper):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        # might add "historic" properties for venue
    
    @property
    def name(self):
        """The name of the venue"""
        return self._name
    
    @property
    def mlbam(self):
        """The venue's official MLB ID"""
        return self._mlbam

class _league_data(_mlb_data_wrapper):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
    
    @property
    def mlbam(self):
        """Official League MLB ID"""
        return self._mlbam

    @property
    def name(self):
        """League's full name"""
        return self._name

    @property
    def short(self):
        """League short name"""
        return self._short

    @property
    def abbrv(self):
        """League abbreviation"""
        return self._abbrv

    @property
    def abbreviation(self):
        """League abbreviation"""
        return self._abbrv

class _person_name_data(_mlb_data_wrapper):
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
        """Person's given name (typically FML)"""
        return self._given
    
    @property
    def first(self):
        """Person's first name"""
        return self._first
    
    @property
    def middle(self):
        """Person's middle name"""
        return self._middle
    
    @property
    def last(self):
        """Person's middle name
        
        (may sometimes just be the middle initial)"""
        return self._last
    
    @property
    def nick(self):
        """Person's nick name"""
        return self._nick
    
    @property
    def pronunciation(self):
        """Pronunciation of person's LAST name"""
        return self._pronunciation


    
class _stat_collection(_mlb_data_wrapper):
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
            
        self.__regular = _mlb_data_wrapper(
            career=career_regular,
            season=season_regular,
            yby=yby_regular
        )
        
        self.__advanced = _mlb_data_wrapper(
            career=career_advanced,
            season=season_advanced,
            yby=yby_advanced
        )
        
        self.__career = _mlb_data_wrapper(
            regular=career_regular,
            advanced=career_advanced
        )
        self.__season = _mlb_data_wrapper(
            regular=season_regular,
            advanced=season_advanced
        )
        self.__yby = _mlb_data_wrapper(
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

class player_stats(_mlb_data_wrapper):
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
        super().__init__(**{'hit_car_reg':hit_car_reg,
                            'hit_car_adv':hit_car_adv,
                            'hit_ssn_reg':hit_ssn_reg,
                            'hit_ssn_adv':hit_ssn_adv,
                            'hit_yby_reg':hit_yby_reg,
                            'hit_yby_adv':hit_yby_adv,
                            
                            'pit_car_reg':pit_car_reg,
                            'pit_car_adv':pit_car_adv,
                            'pit_ssn_reg':pit_ssn_reg,
                            'pit_ssn_adv':pit_ssn_adv,
                            'pit_yby_reg':pit_yby_reg,
                            'pit_yby_adv':pit_yby_adv,
                            
                            'fld_car_reg':fld_car_reg,
                            'fld_ssn_reg':fld_ssn_reg,
                            'fld_yby_reg':fld_yby_reg,
                            })
        

        
        self.__hitting = _stat_collection('hitting',
            career_regular  = hit_car_reg,
            career_advanced = hit_car_adv,
            season_regular  = hit_ssn_reg,
            season_advanced = hit_ssn_adv,
            yby_regular     = hit_yby_reg,
            yby_advanced    = hit_yby_adv
        )
        self.__pitching = _stat_collection('pitching',
            career_regular  = pit_car_reg,
            career_advanced = pit_car_adv,
            season_regular  = pit_ssn_reg,
            season_advanced = pit_ssn_adv,
            yby_regular     = pit_yby_reg,
            yby_advanced    = pit_yby_adv
        )
        self.__fielding = _stat_collection('fielding',
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