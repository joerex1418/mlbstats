import datetime as dt
import pandas as pd
from typing import Union, Optional
from dataclasses import dataclass
from dateutil.parser import parse as parse_dt

from . import parsing
from . import constants as c
from . import mlb_dataclasses as dclass

class ExtendedDict(dict):
    def __init_subclass__(cls) -> None:
        return super().__init_subclass__()
    
    def dget(self,*args,default=None):
        for i in args:
            try:
                return self[i]
            except:
                pass
        return None


@dataclass
class _MlbDate:
    # __slots__ = ['_date']
    _date: Union[dt.date,dt.datetime,str]
    def __post_init__(self):
        if type(self._date) is str:
            self._date = parse_dt(self._date).date()
        elif type(self._date) is dt.datetime:
            self._date = self._date.date()
        elif type(self._date) is dt.date:
            pass
    
    def __repr__(self):
        return self._date
    
    def __str__(self):
        return self._date
    
    def __call__(self,fmt:str=None):
        if fmt is None:
            fmt = r'%Y-%m-%d'
        return dt.datetime.strptime(self._date,fmt).date()
    

@dataclass(frozen=True)
class _Standings:
    __slots__ = ['records','splits']
    records: pd.DataFrame
    splits: Optional[pd.DataFrame]

        
class AppObjects:
    @dataclass(frozen=True)
    class TeamPageContent:
        __slots__ = ['players','stats','rosters','team','draft','transactions','date']
        players: dclass.PlayerDirectory
        stats: dclass.TeamStats
        rosters: dclass.TeamRosters
        team: dclass.TeamInfo
        draft: dict
        transactions: pd.DataFrame
        date: dt.date
        
        def __repr__(self) -> str:
            return f'<TeamPageContent: {self.team.name} ({self.team.mlbam})>'
        
        @property
        def trx(self):
            """Short-hand accessor for 'transactions' attribute"""
            return self.transactions