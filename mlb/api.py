import lxml
import requests
import pandas as pd



class api:
    def __init__(self):
        self.__base = "https://statsapi.mlb.com/api/v1"

    def __call__(self, *args, **kwargs):
        all_input = []
        for arg in args:
            all_input.append(arg)
        for val in kwargs.values():
            all_input.append(val)
        return all_input
    
    @property
    def base(self):
        return self.__base
    
    class get:
        def __init__(self,*args,**kwargs):
            pass

    class person:
        """Person Rest Object (statsapi.mlb.com)"""
        def __init__(self,PersonRestObject:dict = None,obj=None,mlbam:int = None):
            if obj is not None:
                PersonRestObject = obj
            data = PersonRestObject

            # Attributes
            for k,v in data.items():
                if type(v) is str or type(v) is int:
                    setattr(self,f'{k}',data[k])
                
                elif k == 'primaryPosition':
                    self.position   = self.position(v)

                elif k == 'batSide':
                    self.bats       = self.dominantHand(v)

                elif k == 'pitchHand':
                    self.throws     = self.dominantHand(data['pitchHand'])

            stats : list[dict] = data.get('stats',[{}])
            stat_dict = {
                'hitting':{
                    'season':{},
                    'seasonAdvanced':{}
                },
                'pitching':{
                    'season':{},
                    'seasonAdvanced':{}
                },
                'fielding':{
                    'season':{},
                    'seasonAdvanced':{}
                },
            }
            for stat_item in stats:
                statType: str       = stat_item.get('type',{}).get('displayName','-')
                statGroup : str     = stat_item.get('group',{}).get('displayName','-')
                splits : list[dict] = stat_item.get('splits',[{}])

                _stat_obj = self.stat(statType,statGroup,splits)

                stat_dict[statGroup] = {
                    statType: splits
                }
            self.stats = self.statsCollection(stat_dict)
        
        def __getattribute__(self, __name: str):
            return getattr(self,__name)



        class position:
            """Player's primary position"""
            def __init__(self,PrimaryPositionObject:dict=None):
                if PrimaryPositionObject is not None:
                    for k,v in PrimaryPositionObject.items():
                        setattr(self,k,str(v))


        class dominantHand:
            """Player's batting side or throwing hand"""
            def __init__(self,dominantHandObject:dict=None):
                if dominantHandObject is not None:
                    for k,v in dominantHandObject.items():
                        setattr(self,k,str(v))


        class statsCollection:
            """made up of multiple 'stat' instances"""
            def __init__(self,stat_data):
                pass


        class stat:
            """Player stat object representing a specific "statGroup" and "statType"
            
            """
            def __init__(self,statsObject:dict=None):
                pass





