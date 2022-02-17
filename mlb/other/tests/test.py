#%%
import requests

start = 1876
end = 2021

for i in range(start,end+1):
    BASE = "https://statsapi.mlb.com/api/v1"
    url = BASE + "/"