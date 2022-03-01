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

    class stats:
        pass

    class people:
        pass

    class teams:
        pass

    class season:
        pass

    class standings:
        pass

    class game:
        pass




