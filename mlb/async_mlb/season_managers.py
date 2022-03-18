# import asyncio
# import aiohttp
# import datetime as dt
# # import time

# endpoints = {
#     "lastXGames":"/people/{mlbam}?hydrate=stats(group=[hitting],type=[lastXGames],limit={limit})" # limit=[5,15,30,60]
# }

# sitCodes_str = "h,a,d,n,g,t,l,r,vl,vr,vt,val,vnl,2,3,4,5,6,7,8,9,10,twn,tls,taw,tal,b1,b2,b3,b4,b5,b6,b7,b8,b9,lo,fip,i01,i02,i03,i04,i05,i06,i07,i08,i09,ix,e,r0,r1,r2,r3,r12,r23,r123,ron,ron2,risp,risp2,o0,o1,o2,fp,ac,bc,ec,2s,fc,c00,c01,c02,c10,c11,c12,c20,c21,c22,c30,c31,c32,p1,p2,p3,p4,p5,p6,p7,p8,p9,pO,pD,pH"
# sitCodes = sitCodes_str.split(',')




# def get_tasks(session,season):
#     startDate = dt.date(season,1,1)
#     endDate = dt.date(season,12,1)
#     delta = dt.timedelta(days=1)
#     dates = []
#     while startDate < endDate:
#         date_str = dt.date.strftime(startDate,"%m/%d/%Y")
#         # print(date_str)
#         dates.append(date_str)
#         startDate+=delta
#     tasks = []
#     for d in dates:
#         tasks.append(session.get(f"https://statsapi.mlb.com/api/v1/teams/145/coaches?date={d}",ssl=False))
#     return tasks



# async def get_season_managers(season):
#     retrieved = []
#     async with aiohttp.ClientSession() as session:
#         tasks = get_tasks(session,season)
#         responses = await asyncio.gather(*tasks)
#         for response in responses:
#             resp = await response.json()
#             for entry in resp["roster"]:
#                 if entry["jobId"] == "MNGR":
#                     try:
#                         print(entry["person"]["fullName"])
#                         retrieved.append(entry["person"])
#                     except:
#                         print('----manager not found')
#                         pass
#     return retrieved

# def runit(season):
#     # start = time.time()
#     retrieved = asyncio.run(get_season_managers(season))
#     # print("--- {} seconds ---".format(time.time()-start))
#     return retrieved


# # if __name__ == "__main__":
# #     season=2021
# #     asyncio.run(get_season_managers(season))