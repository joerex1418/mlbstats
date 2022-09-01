from dataclasses import dataclass
from typing import Union, Optional, List, Dict

import pandas as pd

from .. import constants as c
from .venue import Venue
from .league import Leagues
from .stats import StatTypeCollection

_pdf = pd.DataFrame

@dataclass(frozen=True)
class TeamName:
    __slots__ = ['mlbam','full','location','franchise','club','short','abbreviation']
    mlbam: int
    full: str
    location: str
    franchise: str
    club: str
    short: str
    abbreviation: str
    
    def __str__(self):
        return self.full

    def __repr__(self):
        return f'<{self.full}: ({self.mlbam})>'
    
    @property
    def abbrv(self):
        """Shorthand property for 'abbreviation' attribute"""
        return self.abbreviation
    
    @property
    def id(self):
        """Alias for 'mlbam' attribute"""
        return self.mlbam

    
@dataclass(frozen=True)
class TeamStats:
    __slots__ = ['players','totals']
    players: StatTypeCollection
    totals: StatTypeCollection
        
    def __repr__(self) -> str:
        players_status = "NOT AVAIL"
        totals_status = "NOT AVAIL"
        if self.players is not None:
            players_status = "AVAIL"
        if self.totals is not None:
            totals_status = "AVAIL"
        return f'<_TeamStats: Roster: {players_status} ; Totals: {totals_status}>'
    
    def __call__(self,*args) -> Union[pd.DataFrame,None]:
        _group = ''
        _get_adv = False
        if len(args) >= 1:
            _group = args[0]
            if len(args) == 2:
                if args[1] == 'adv' or args[1] == 'advanced':
                    _get_adv = True
        
        if _group == 'hitting':
            if _get_adv:
                df: _pdf = (pd.concat([self.players.hitting_adv,self.totals.hitting_adv]))
            else:
                df: _pdf = (pd.concat([self.players.hitting,self.totals.hitting]))
            df[['player_mlbam','team_mlbam','player_name']] = (
                (df[['player_mlbam','team_mlbam','player_name']]
                .fillna({'player_mlbam':0,'team_mlbam':0,'player_name':'-'})
                .astype({'player_mlbam':int,'team_mlbam':int,'player_name':str}))
                )
            return df
        
        elif _group == 'pitching':
            if _get_adv:
                df: _pdf = (pd.concat([self.players.pitching_adv,self.totals.pitching_adv]))
            else:
                df: _pdf = (pd.concat([self.players.pitching,self.totals.pitching]))
            df[['player_mlbam','team_mlbam','player_name']] = (
                (df[['player_mlbam','team_mlbam','player_name']]
                .fillna({'player_mlbam':0,'team_mlbam':0,'player_name':'-'})
                .astype({'player_mlbam':int,'team_mlbam':int,'player_name':str}))
                )
            return df
        
        elif _group == 'fielding':
            df: _pdf = (pd.concat([self.players._fielding,self.totals._fielding]))
            df[['player_mlbam','team_mlbam','player_name']] = (
                (df[['player_mlbam','team_mlbam','player_name']]
                .fillna({'player_mlbam':0,'team_mlbam':0,'player_name':'-'})
                .astype({'player_mlbam':int,'team_mlbam':int,'player_name':str}))
                )
            df = df.rename(columns=c.STATDICT)
            pos_col = df['pos']
            df = df.drop(columns='pos')
            df.insert(3,'pos',pos_col)
            df[['pos','SB%','cERA','CS','SB','PB','CI','WP','PK']] = (
                (df[['pos','SB%','cERA','CS','SB','PB','CI','WP','PK']]
                 .fillna({
                     'pos':'-',
                     'SB%':'-',
                     'cERA':'-',
                     'CS':'-',
                     'SB':'-',
                     'PB':'-',
                     'CI':'-',
                     'WP':'-',
                     'PK':'-'})
                 .astype(str))
            )
            
            return df
                

@dataclass(frozen=True)
class TeamRosters:
    __slots__ = ['full','fortyman','active']
    full: pd.DataFrame
    fortyman: pd.DataFrame
    active: pd.DataFrame


@dataclass(frozen=True)
class TeamInfo:
    __slots__ = ['name','league','division','venue','first_year','season']
    name: TeamName
    league: Leagues.League
    division: Optional[Leagues.Division]
    venue: Venue
    first_year: str
    season: str
    
    def __str__(self):
        return str(self.name)
    
    def __repr__(self):
        return f'<_TeamInfo: {self.name}; LG: {self.league.id}; DIV: {self.division.id}>'
    
    @property
    def id(self):
        return self.name.mlbam
    
    @property
    def mlbam(self):
        return self.name.mlbam
