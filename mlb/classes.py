import datetime as dt

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
    
    def __setattr__(self,_attr,_value):
        raise AttributeError("Can't do that")
        
    def __call__(self,_attr):
        return getattr(self,_attr)

class _venue_data(_mlb_data_wrapper):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        # might add "historic" properties for venue
    
    @property
    def name(self):
        return self._name
    
    @property
    def mlbam(self):
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

class stats_data:
    def __init__(
        self,
        hit_ssn_reg=None,
        hit_ssn_adv=None,
        pit_ssn_reg=None,
        pit_ssn_adv=None,
        
        **kwargs):
        pass
