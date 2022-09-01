from typing import Optional, Union, List, Dict
from dataclasses import dataclass

class Leagues:
    @dataclass(frozen=True)
    class League:
        __slots__ = ['id','name','name','abbreviation','short_name','children_ids']
        id: int
        name: str
        abbreviation: str
        short_name: str
        children_ids: list[int]
        
        @property
        def abbrv(self):
            """Shorthand property for 'abbreviation' attribute"""
            return self.abbreviation
        
        def __str__(self):
            return self.name

        def __repr__(self):
            return f'<League: {self.name} - ({self.id})>'

    
    @dataclass(frozen=True)
    class Division(League):
        __slots__ = ['parent_id','parent_name','parent_name','parent_abbreviation','parent_short_name']
        parent_id: Optional[int]
        parent_name: Optional[str]
        parent_abbreviation: Optional[str]
        parent_short_name: Optional[str]
        
        def __repr__(self):
            return f'<Division: {self.name} - {self.id} (Lg: {self.parent_id})>'
        
    AMERICAN   = League(103,"American League","AL","American",[200,201,202])
    NATIONAL   = League(104,"National League","NL","National",[203,204,205])
    AL_WEST    = Division(200,"American League West","ALW","AL West",[],103,"American League","AL","American")
    AL_EAST    = Division(201,"American League East","ALE","AL East",[],103,"American League","AL","American")
    AL_CENTRAL = Division(202,"American League Central","ALC","AL Central",[],103,"American League","AL","American")
    NL_WEST    = Division(203,"National League West","NLW","NL West",[],104,"National League","NL","National")
    NL_EAST    = Division(204,"National League East","NLE","NL East",[],104,"National League","NL","National")
    NL_CENTRAL = Division(205,"National League Central","NLC","NL Central",[],104,"National League","NL","National")
    EMPTY      = None

    @staticmethod
    def get(league_id):
        if str(league_id) == "103":
            return Leagues.AMERICAN
        elif str(league_id) == "104":
            return Leagues.NATIONAL
        if str(league_id) == "200":
            return Leagues.AL_WEST
        elif str(league_id) == "201":
            return Leagues.AL_EAST
        elif str(league_id) == "202":
            return Leagues.AL_CENTRAL
        elif str(league_id) == "203":
            return Leagues.NL_WEST
        elif str(league_id) == "204":
            return Leagues.NL_CENTRAL
        elif str(league_id) == "205":
            return Leagues.NL_EAST
        else:
            return Leagues.EMPTY
    
    @staticmethod
    def get_id(league_query:str):
        """Get league ID by search query (for convenience)"""
        query = str(league_query.lower())

        if league_query.isdigit():
            return Leagues.get(league_query)

        if query in ['al','american','american league']:
            return 103
        elif query in ['nl','national','national league']:
            return 104
        elif query in ['alw','al west','american league west']:
            return 200
        elif query in ['ale','al east','american league east']:
            return 201
        elif query in ['alc','al central','american league central']:
            return 202
        elif query in ['nlw','nl west','national league west']:
            return 203
        elif query in ['nle','nl east','national league east']:
            return 204
        elif query in ['nlc','nl central','national league central']:
            return 205
        
