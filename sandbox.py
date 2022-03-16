#%%
import lxml
import time
import mlb
import requests
import pandas as pd
import datetime as dt
from mlb.utils import keys
from IPython.display import display



# %%
p = mlb.person(547989)


# %%
p.birth.date.full
# %%
