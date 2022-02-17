import asyncio
import aiohttp
import pandas as pd
# import time

BASE = "https://statsapi.mlb.com/api/v1"

start = 1901
end = 1920
seasons = []
seasons_ints = list(range(start,end+1))
for s in seasons_ints:
    seasons.append(str(s))
seasons = ",".join(seasons)

url = BASE + f"/schedule?&hydrate=&season={seasons}&sportId=1"

async def parse_schedule(response):
        og_cols = [
            'gamePk',
            'Date',
            'mlbam',
            '-',
            'opp_mlbam',
            'venue_mlbam',
            'Result',
            'Record',
            'SrsGm#',
            'R','H',
            'E',
            'LOB',
            'Venue',
            'Attend.',
            'D/N',
            'Sky',
            'Temp',
            'Wind',
            '#Inns',
            'Elasped',
            '',
            'Notes']
        
        columns = [
            'gamePk',
            'away_mlbam',
            'away_record',
            'home_mlbam',
            'home_record',
            'playdate',
            'venue_mlbam',
            'result',
            'SrsGm#',
            'runs',
            'hits',
            'errors',
            'leftOnBase',
            'venue_name',
            'attendance',
            'dayNight',
            'sky',
            'temp',
            'wind',
            'scheduled_innings',
            'duration',
            'delay_status',
            'notes'
        ]
        all_dates = response["dates"]
        all_games = []
        for date in all_dates:
            playdate = date["date"]

            for game in date["games"]:
                gamePk = game["gamePk"]
                if "rescheduledFromDate" in game.keys():
                    rescheduledFrom = game["rescheduledFromDate"]
                else:rescheduledFrom = ""
                if "rescheduledGameDate" in game.keys():
                    rescheduledTo = game["rescheduledGameDate"]
                else:rescheduledTo = ""
                try:
                    delay_status = f'{game["status"]["detailedState"]} ({game["status"]["reason"]})'
                except:
                    delay_status = ""
                venue_mlbam = game["venue"]["id"]
                venue_name = game["venue"]["name"]
                away = game["teams"]["away"]["team"]
                home = game["teams"]["home"]["team"]

                away_mlbam = away["id"]
                home_mlbam = home["id"]

                # if int(mlbam) == int(away_mlbam):
                #     isHome = False
                #     vsSymbol = "@"
                #     # team_name = away_name
                #     # opponent_name = home_name
                #     opponent_mlbam = home_mlbam
                # else:
                #     isHome = True
                #     vsSymbol = "vs"
                #     # team_name = home_name
                #     # opponent_name = away_name
                #     opponent_mlbam = away_mlbam

                runs = 0
                hits = 0
                errors = 0
                leftOnBase = 0
                # if isHome is True:
                leagueRecord = game["teams"]["home"]["leagueRecord"]
                home_wins = leagueRecord["wins"]
                home_losses = leagueRecord["losses"]
                home_record = f"{home_wins}-{home_losses}"
                try:
                    if game["teams"]["home"]["isWinner"] is True:
                        result = "W"
                    else:result = "L"
                except: result = "--"
                try:
                    for inning in game["linescore"]["innings"]:
                        try:runs += inning["home"]["runs"]
                        except:pass
                        try:hits += inning["home"]["hits"]
                        except:pass
                        try:errors += inning["home"]["errors"]
                        except:pass
                        try:leftOnBase += inning["home"]["leftOnBase"]
                        except:pass
                except:
                    pass
                # else:
                leagueRecord = game["teams"]["away"]["leagueRecord"]
                away_wins = leagueRecord["wins"]
                away_losses = leagueRecord["losses"]
                away_record = f"{away_wins}-{away_losses}"
                try:
                    if game["teams"]["away"]["isWinner"] is True:
                        result = "W"
                    else:result = "L"
                except: result = "--"
                try:
                    for inning in game["linescore"]["innings"]:
                        try:runs += inning["away"]["runs"]
                        except:pass
                        try:hits += inning["away"]["hits"]
                        except:pass
                        try:errors += inning["away"]["errors"]
                        except:pass
                        try:leftOnBase += inning["away"]["leftOnBase"]
                        except:pass
                except:
                    pass
                    
                try:
                    sky = game["weather"]["condition"]
                    if sky == "Unknown": sky = "--"
                except:
                    sky = "--"
                try:temp = game["weather"]["temp"]
                except:temp = "--"
                try:wind = game["weather"]["wind"]
                except:wind = "--"
                try:duration = game["gameInfo"]["gameDurationMinutes"]
                except:duration = "--"
                try:attendance = game["gameInfo"]["attendance"]
                except:attendance = "--"
                
                dayNight = game.get("dayNight",'-').capitalize()
                scheduled_innings = game.get("scheduledInnings","-")
                games_in_series = game.get("gamesInSeries",'-')
                series_game_number = game.get("seriesGameNumber",'-')

                if rescheduledTo == "" and rescheduledFrom == "":
                    notes = ""
                elif rescheduledFrom == "":
                    notes = f"PP Date {rescheduledTo}"
                elif rescheduledTo == "":
                    notes = f"Makeup {rescheduledFrom}"

                # try:
                #     media_items = game["content"]["media"]["epgAlternate"]
                #     for media in media_items:
                #         if media["title"] == "Extended Highlights":
                #             playbackId = media["items"][0]["mediaPlaybackId"]
                #             url_extended_highlights = f"https://mlb-cuts-diamond.mlb.com/FORGE/{season}/{season}-{playdate[5:7]}/{playdate[8:]}/{playbackId}_1280x720_59_4000K.mp4"
                #         elif media["title"] == "Daily Recap":
                #             playbackId = media["items"][0]["mediaPlaybackId"]
                #             url_highlights = f"https://mlb-cuts-diamond.mlb.com/FORGE/{season}/{season}-{playdate[5:7]}/{playdate[8:]}/{playbackId}_1280x720_59_4000K.mp4"
                # except:
                #     url_extended_highlights = ""
                #     url_highlights = ""
                # print(url_extended_highlights)
                # print(url_highlights)

                single_game = [
                    gamePk,
                    away_mlbam,
                    away_record,
                    home_mlbam,
                    home_record,
                    playdate,
                    venue_mlbam,
                    result,
                    f"{series_game_number} of {games_in_series}",
                    runs,
                    hits,
                    errors,
                    leftOnBase,
                    venue_name,
                    attendance,
                    dayNight,
                    sky,
                    temp,
                    wind,
                    scheduled_innings,
                    duration,
                    delay_status,
                    notes]
                all_games.append(single_game)

        season_games_df = pd.DataFrame(all_games,columns=columns)

        return season_games_df


async def update_game_logs(start,end,save_as):
    all_records = []
    hydrations = "decisions,gameInfo,venue,linescore,weather,series"
    async with aiohttp.ClientSession() as session:
        tasks = []
        for season in range(start,end+1):
            url = BASE + f"/schedule?&hydrate={hydrations}&season={season}&sportId=1"
            tasks.append(session.get(url, ssl=False))

        responses = await asyncio.gather(*tasks)
        
        for response in responses:
            resp = await response.json()
            parsed = await parse_schedule(resp)
            all_records.append(parsed)
            
    updated_df = pd.concat(all_records)
    save_as = save_as.replace(".csv","")
    updated_df.to_csv(f"{save_as}.csv",index=False)
    return updated_df

if __name__ == "__main__":
    # start = time.time()
    df1 = asyncio.run(update_game_logs(1901,1950,save_as="schedules_1901-1950"))
    df2 = asyncio.run(update_game_logs(1951,2000,save_as="schedules_1951-2000"))
    df3 = asyncio.run(update_game_logs(2001,2021,save_as="schedules_2001-2021"))
    combined_df = pd.concat([df1,df2,df3])
    combined_df.to_csv("schedules.csv",index=False)
    print(combined_df)
    # print(f"--- {time.time()-start} seconds ---")


