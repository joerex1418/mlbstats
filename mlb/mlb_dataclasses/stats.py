from typing import Optional, Union, List, Dict
from dataclasses import dataclass

import pandas as pd

from .. import parsing
from .. import constants as c

@dataclass(frozen=True)
class StatTypeCollection:
    __slots__ = ['hitting','pitching','_fielding','hitting_adv','pitching_adv']
    hitting: Optional[pd.DataFrame]
    pitching: Optional[pd.DataFrame]
    _fielding: Optional[pd.DataFrame]
    hitting_adv: Optional[pd.DataFrame]
    pitching_adv: Optional[pd.DataFrame]
    
    @property
    def fielding(self):
        return self._fielding.rename(columns=c.STATDICT)
    
    @staticmethod
    def from_json(_json,**kwargs):
        dfs = {}
        
        for s in _json.get('stats',[{}]):
            if s.get('type',{}).get('displayName').lower().find('advanced') == -1:
                if s.get('group',{}).get('displayName') == 'hitting':
                    dfs['hitting'] = parsing._parse_league_stats(s['splits'])
                elif s.get('group',{}).get('displayName') == 'pitching':
                    dfs['pitching'] = parsing._parse_league_stats(s['splits'])
                elif s.get('group',{}).get('displayName') == 'fielding':
                    dfs['fielding'] = parsing._parse_league_stats(s['splits'])
            else:
                if s.get('group',{}).get('displayName') == 'hitting':
                    dfs['hitting_adv'] = parsing._parse_league_stats(s['splits'])
                elif s.get('group',{}).get('displayName') == 'pitching':
                    dfs['pitching_adv'] = parsing._parse_league_stats(s['splits'])
                    
        if kwargs.get('dfs_only'):
            return dfs
        
        return StatTypeCollection(
            dfs['hitting'],dfs['pitching'],dfs['fielding'],
            dfs['hitting_adv'],dfs['pitching_adv']
        )

