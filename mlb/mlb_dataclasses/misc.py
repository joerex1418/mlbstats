import datetime as dt
from dataclasses import dataclass
from typing import Union, Optional, List, Dict

@dataclass
class MlbDate:
    __date: Union[dt.datetime,dt.date,str] = None
    def __post_init__(self):
        d = self.__date
        t = type(d)
        if t is str:
            self.__date_obj = dt.datetime.strptime(d,r'%Y-%m-%d').date()
        elif t is dt.datetime:
            self.__date_obj = d.date()
        elif t is dt.date:
            self.__date_obj = d
        else:
            self.__date_obj = ''

    def __call__(self,fmt:str):
        return self.__date_obj.strftime(fmt)

    def __str__(self):
        try:
            return self.__date_obj.strftime(r'%Y-%m-%d')
        except:
            return self.__date_obj

    def __repr__(self):
        try:
            return self.__date_obj.strftime(r'%Y-%m-%d')
        except:
            return self.__date_obj
    
    @property
    def year(self):
        return self.__date_obj.year
    
    @property
    def month(self):
        return self.__date_obj.month
    
    @property
    def day(self):
        return self.__date_obj.day
        

@dataclass(frozen=True)
class Position:
    __slots__ = ['code','name','type','abbreviation']
    code: Optional[str]
    name: Optional[str]
    type: Optional[str]
    abbreviation: Optional[str]

    @property
    def abbrv(self):
        return self.abbreviation

    def __str__(self):
        return str(self.abbreviation)

    def __repr__(self):
        return f'<{self.abbreviation}, {self.name}>'

@dataclass(frozen=True)
class Dexterity:
    code: Union[str,None] = None
    description: Union[str,None] = None
    
    def __str__(self):
        return self.code
    
    def __repr__(self):
        return self.code
    
