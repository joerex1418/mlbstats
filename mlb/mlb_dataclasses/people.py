import datetime as dt
from dataclasses import dataclass, asdict
from typing import Union, Optional, List, Dict

import pandas as pd

from .. import constants as c
from .misc import Position, Dexterity, MlbDate

_JSON = Union[Dict,List[Dict]]

def parse_date(date_string:str):
    if date_string is None:
        return None
    try:
        return dt.datetime.strptime(date_string,r'%Y-%m-%d').date()
    except:
        return None

@dataclass(frozen=True)
class PersonName:
    __slots__ = ['mlbam','full','given','first','middle','last','nick','pronunciation']
    mlbam: int
    full: Optional[str]
    given: Optional[str]
    first: Optional[str]
    middle: Optional[str]
    last: Optional[str]
    nick: Optional[str]
    pronunciation: Optional[str]
    
    def __str__(self) -> str:
        return str(self.full)
    
    def __repr__(self) -> str:
        return str(self.full)
    
    @property
    def id(self):
        return self.mlbam
    
    def asdict(self):
        return asdict(self)
    

@dataclass(frozen=True)
class PersonEvent:
    city: Union[str,None] = None
    country: Union[str,None] = None
    state: Union[str,None] = None
    date: Union[MlbDate,None] = None


@dataclass(frozen=True)
class Person:
    # __slots__ = []
    mlbam: int
    name: Union[PersonName,str] = None
    age: Union[int,None] = None
    birth: Union[PersonEvent,None] = None
    bat_side: Union[Dexterity,str] = None
    pitch_hand: Union[Dexterity,str] = None
    position: Union[Position,str] = None
    is_player: Union[bool,None] = None
    active: Union[bool,None] = None
    draft_year: Union[int,None] = None
    height: Union[str,None] = None
    weight: Union[int,None] = None
    jersey_number: Union[str,None] = None
    
    @property
    def id(self):
        return self.mlbam
    
    def asdict(self):
        d = asdict(self)
        
        d = {'name':d['name']['full'],
             'pos':self.position.abbreviation,
             'number':self.jersey_number,
             'birth_city':d['birth'].get('city'),
             'birth_state':d['birth'].get('state'),
             'birth_country':d['birth'].get('country')
             }
        
        return d
    
    @staticmethod
    def from_json(_json:_JSON):
        pos: dict = _json.get('position',_json.get('primaryPosition',{}))
        return Person(
            mlbam=_json.get('id',0),
            name=PersonName(
                    mlbam=_json.get("mlbam"),
                    full=_json.get("fullName"),
                    given=_json.get("fullFMLName"),
                    first=_json.get("firstName"),
                    middle=_json.get("middleName"),
                    last=_json.get("lastName"),
                    nick=_json.get("nickName"),
                    pronunciation=_json.get(c.PRON),
                ),
            age=_json.get('currentAge',_json.get('age')),
            birth=PersonEvent(
                city=_json.get('birthCity'),
                country=_json.get('birthCountry'),
                state=_json.get('birthStateProvince'),
                date=MlbDate(_json.get('birthDate'))
                ),
            bat_side=Dexterity(
                _json.get('batSide',{}).get('code'),
                _json.get('batSide',{}).get('description')),
            pitch_hand=Dexterity(
                _json.get('batSide',{}).get('code'),
                _json.get('batSide',{}).get('description')),
            position=Position(pos.get('code'),pos.get('name'),
                              pos.get('type'),pos.get(c.ABBRV)
                              ),
            is_player=_json.get('isPlayer'),
            active=_json.get('active'),
            draft_year=_json.get('draftYear',0),
            height=_json.get('height',''),
            weight=_json.get('weight',0),
            jersey_number=_json.get('primaryNumber',_json.get('jerseyNumber'))
        )
    

@dataclass(frozen=True)
class PlayerDirectory:
    __slots__ = ['__dict','df']
    __dict: Dict[str,Person]
    
    def __repr__(self):
        df = pd.DataFrame([p.asdict() for p in self.__dict.values()])
        return df.to_string()
    
    def __call__(self, _player_id):
        return self.__dict[f'ID{str(_player_id)}']
    
    
    @staticmethod
    def from_json(_json:_JSON):
        _dict = {}
        for p in _json:
            new = Person.from_json(p)
            _dict[f'ID{new.mlbam}'] = new
        return PlayerDirectory(_dict)