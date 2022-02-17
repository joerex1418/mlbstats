import asyncio
import aiohttp


sitCodes_str = "h,a,d,n,g,t,l,r,vl,vr,vt,val,vnl,2,3,4,5,6,7,8,9,10,twn,tls,taw,tal,b1,b2,b3,b4,b5,b6,b7,b8,b9,lo,fip,i01,i02,i03,i04,i05,i06,i07,i08,i09,ix,e,r0,r1,r2,r3,r12,r23,r123,ron,ron2,risp,risp2,o0,o1,o2,fp,ac,bc,ec,2s,fc,c00,c01,c02,c10,c11,c12,c20,c21,c22,c30,c31,c32,p1,p2,p3,p4,p5,p6,p7,p8,p9,pO,pD,pH"

sitCodes = sitCodes_str.split(',')

def get_tasks(session,sitCodes):
    tasks = []
    for s in sitCodes:
        # print(url.format(bbrefID[0],bbrefID))
        tasks.append(session.get(f"http://statsapi.mlb.com/api/v1/people/547989?hydrate=stats(type=statSplits,group=hitting,season=2021,sitCodes={s})",ssl=False))
    return tasks

async def async_call(sitCodes):
    retrieved = []
    async with aiohttp.ClientSession() as session:
        tasks = get_tasks(session,sitCodes)
        responses = await asyncio.gather(*tasks)
        for response in responses:
            resp = await response.json()
            try:
                print(resp['people'][0]['stats'][0]['splits'][0]['split']['description'])
                retrieved.append(resp)
            except:
                print(f"sitCode, not found")


