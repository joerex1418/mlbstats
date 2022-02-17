import os

import pandas as pd
import datetime as dt
from numpy import NAN
# from sqlalchemy import create_engine

from .paths import *

def get(df_title) -> pd.DataFrame:
    return pd.read_csv(DATA_DIR + f"{df_title}.csv",index_col=False)

def get_teams_df(year=None) -> pd.DataFrame:
    # engine = create_engine(f"sqlite:///{os.path.abspath('SimpleStatsMLB/baseball.db')}")
    # engine = create_engine(f"sqlite:///{os.path.abspath('simplestats/baseball.db')}")
    # engine = create_engine(BASEBALL_DB)
    try:
        # print("opening connection")
        # conn = engine.connect()
        # teams_df = pd.read_sql_table(table_name='teams',con=conn,index_col=False)
        # conn.close()
        # engine.dispose()
        # print("returning df")

        teams_df = pd.read_csv(TEAMS_CSV,index_col=False)
        
        if year is None:
            return teams_df
        else:
            df = teams_df
            df = df[df["yearID"]==year]
            return df
    except Exception as e:
        try:
            pass
            # conn.close()
            # engine.dispose()
        except:
            pass

def get_franchise_df(teamID) -> pd.DataFrame:
    try:
        teamID = int(teamID)
        search_by = "mlbam"
    except:
        search_by = "bbrefID"
    df = get_teams_df()
    if search_by == "mlbam":
        return df[df["mlbam"]==teamID].sort_values(by="yearID",ascending=False)
    elif search_by == "bbrefID":
        return df[df["bbrefID"]==teamID].sort_values(by="yearID",ascending=False)

def get_standings_df() -> pd.DataFrame:
    try:
        # engine = create_engine(f"sqlite:///{os.path.abspath('SimpleStatsMLB/baseball.db')}")
        # engine = create_engine(f"sqlite:///{os.path.abspath('simplestats/baseball.db')}")
        # engine = create_engine(BASEBALL_DB)
        # conn = engine.connect()
        # df = pd.read_sql_table(table_name='ybyStandings',con=conn,index_col=False,coerce_float=False).drop(columns=['index'])
        # conn.close()

        df = pd.read_csv(YBY_STANDINGS_CSV,index_col=False)

        return df

    except Exception as e:
        print(e)
        try:
            pass
            # conn.close()
        except:
            pass
        return None

def get_yby_records(raw=False) -> pd.DataFrame:
    try:
        # engine = create_engine(f"sqlite:///{os.path.abspath('simplestats/baseball.db')}")
        # engine = create_engine(BASEBALL_DB)
        # conn = engine.connect()
        # df = pd.read_sql_table(table_name='ybyRecords',con=conn,index_col=False).drop(columns=['index'])

        # conn.close()
        # engine.dispose()


        df = pd.read_csv(YBY_RECORDS_CSV,index_col=False)
        return df

        if raw is True:
            pass
        else:
            df = df.astype("str")
            df = df.astype({"tm_mlbam":"int","season":"int"})
            df["vAL"] = df.al_W + "-" + df.al_L
            df["vNL"] = df.nl_W + "-" + df.nl_L
            df["vWest"] = df.west_W + "-" + df.west_L
            df["vEast"] = df.east_W + "-" + df.east_L
            df["vCentral"] = df.central_W + "-" + df.central_L
            df["Home"] = df.hm_W + "-" + df.hm_L
            df["Away"] = df.aw_W + "-" + df.aw_L
            df["Last10"] = df.lt_W + "-" + df.lt_L
            df["ExInns"] = df.xInn_W + "-" + df.xInn_L
            df["OneRun"] = df["1R_W"] + "-" + df["1R_L"]
            df["Winners"] = df.win_W + "-" + df.win_L
            df["Day"] = df.dy_W + "-" + df.dy_L
            df["Night"] = df.nt_W + "-" + df.nt_L
            df["Grass"] = df.g_W + "-" + df.g_L
            df["Turf"] = df.t_W + "-" + df.t_L
            df["vRHP"] = df.rhp_W + "-" + df.rhp_L
            df["vLHP"] = df.lhp_W + "-" + df.lhp_L

        return df[[   
                'season',
                'tm_mlbam',
                'tm_name',
                'tm_bbrefID',
                'lg_mlbam',
                'lg_abbrv',
                'div_mlbam',
                'div_short',
                'v_mlbam',
                'G',
                'W',
                'L',
                'W%',
                'R',
                'RA',
                'RunDiff',
                'vAL',
                'vNL',
                'vWest',
                'vEast',
                'vCentral',
                'Home',
                'Away',
                'vRHP',
                'vLHP',
                'Last10',
                'ExInns',
                'OneRun',
                'Winners',
                'Day',
                'Night',
                'Grass',
                'Turf'
                ]]
    
    
    except Exception as e:
        try:
            print(e)
            # print("FAILED - closing connection")
            # conn.close()
            # engine.dispose()
        except:
            pass

def get_people_df() -> pd.DataFrame:
    try:
        # engine = create_engine(f"sqlite:///{os.path.abspath('SimpleStatsMLB/baseball.db')}")
        # engine = create_engine(f"sqlite:///{os.path.abspath('simplestats/baseball.db')}")
        # engine = create_engine(BASEBALL_DB)
        # conn = engine.connect()
        # df = pd.read_sql_table(table_name='people',con=conn,index_col=False,coerce_float=False).drop(columns=['index'])
        # conn.close()
        
        df = pd.read_csv(PEOPLE_CSV,index_col=False)

        return df

    except Exception as e:
        print(e)
        try:
            pass
            # conn.close()
        except:
            pass

def get_bios_df() -> pd.DataFrame:
    try:
        # engine = create_engine(f"sqlite:///{os.path.abspath('SimpleStatsMLB/baseball.db')}")
        # engine = create_engine(f"sqlite:///{os.path.abspath('simplestats/baseball.db')}")

        # engine = create_engine(BASEBALL_DB)
        # conn = engine.connect()
        # df = pd.read_sql_table(table_name='bios',con=conn,index_col=False,coerce_float=False).drop(columns=['index'])
        # conn.close()

        df = pd.read_csv(BIOS_CSV,dtype={'isPlayer':'str','isActive':'str','primaryNumber':'str'},index_col=False)
        df.replace('-',NAN,inplace=True)
        df['name'] = df['name'].astype('str')
        df[['birthDate','deathDate','mlbDebutDate','lastGame']] = df[['birthDate','deathDate','mlbDebutDate','lastGame']].apply(pd.to_datetime,format=r"%Y-%m-%d")

        return df
    except Exception as e:
        print(e)
        try:
            pass
            # conn.close()
        except:
            pass

def get_seasons_df() -> pd.DataFrame:
    try:
        # engine = create_engine(f"sqlite:///{os.path.abspath('SimpleStatsMLB/baseball.db')}")
        # engine = create_engine(f"sqlite:///{os.path.abspath('simplestats/baseball.db')}")

        # engine = create_engine(BASEBALL_DB)
        # conn = engine.connect()
        # df = pd.read_sql_table(table_name='seasons',con=conn,index_col=False,coerce_float=False).drop(columns=['index'])
        # conn.close()
        
        cols = ['preSeasonStartDate','preSeasonEndDate','seasonStartDate','seasonEndDate','springStartDate','springEndDate','regularSeasonStartDate','regularSeasonEndDate','allStarDate','postSeasonStartDate','postSeasonEndDate','offSeasonStartDate','offSeasonEndDate']

        df = pd.read_csv(SEASONS_CSV,index_col=False)

        df[cols] = df[cols].apply(pd.to_datetime,format=r"%Y-%m-%d")
        return df
    except Exception as e:
        print(e)
        try:
            pass
            # conn.close()
        except:
            pass

def get_venues_df(active_only=True) -> pd.DataFrame:
    try:
        # engine = create_engine(f"sqlite:///{os.path.abspath('SimpleStatsMLB/baseball.db')}")
        # engine = create_engine(f"sqlite:///{os.path.abspath('simplestats/baseball.db')}")
        # engine = create_engine(BASEBALL_DB)
        # conn = engine.connect()
        # people = pd.read_sql_table(table_name='parks',con=conn,index_col=False,coerce_float=False).drop(columns=['index'])
        # conn.close()
        
        df = pd.read_csv(VENUES_CSV,index_col=False)

        if active_only is True:
            df = df[df["active"]==True].reset_index(drop=True)
        
        return df
    except Exception as e:
        print(e)
        try:
            pass
            # conn.close()
        except:
            pass

def get_playoff_history(mlbam):
    """Needs work. Use StatsAPI"""
    return {'do not use':'this function'} # postseason appearance function is WIP

def get_season_info(date=None) -> tuple:
    """
    Get current season in-progress and most recently completed season, given a specified date

    Parameters:
    -------------
    date: date string, (format: `mm/dd/yyyy`)\n\t\tDefault: current date
    """
    if date is None:
        current_date = dt.datetime.today()
        using_pretend = False
    else:
        using_pretend = True
        pretend_current = dt.datetime.strptime(date,r"%m/%d/%Y")
        current_date = pretend_current
    try:
        # engine = create_engine(f"sqlite:///{os.path.abspath('SimpleStatsMLB/baseball.db')}")
        # engine = create_engine(f"sqlite:///{os.path.abspath('simplestats/baseball.db')}")
        # engine = create_engine(BASEBALL_DB)
        # conn = engine.connect()
        # df = pd.read_sql_table(
        #     table_name='seasons',
        #     con=conn,
        #     index_col=False,
        #     columns=["season","seasonStart","seasonEnd"],
        #     coerce_float=False)

        # conn.close()

        df = get_seasons_df()
        if using_pretend is False:
            df = df.head(4)

        # dates = []
        # zipped = zip(df.season,df.seasonStartDate,df.seasonEndDate)

        # for tup in zipped:
            # startDate = dt.datetime.strptime(f"{tup[1]}, {tup[0]}",r'%B %d, %Y')
            # endDate = dt.datetime.strptime(f"{tup[2]}, {tup[0]}",r'%B %d, %Y')

            # startDate = tup[1]
            # endDate = tup[2]

            # dates.append((startDate,endDate))

        # df_dates = pd.DataFrame(data=dates,columns=["start","end"])

        df = df.rename(columns={'seasonStartDate':'start','seasonEndDate':'end'})
        # df['start'] = pd.to_datetime(df['start'],format=r"%Y-%m-%d")
        # df['end'] = pd.to_datetime(df['end'],format=r"%Y-%m-%d")

        df_dates = df
        # return df_dates

        for row in range(len(df_dates)):
            # if df_dates.iloc[row].start.date() <= current_date <= df_dates.iloc[row].end.date():
            if df_dates.iloc[row].start <= current_date <= df_dates.iloc[row].end:
                seasonInProgress = current_date.year
                seasonCompleted = seasonInProgress - 1
                # print(f"On date: {current_date}:")
                # print(f"InProgress: {seasonInProgress}")
                # print(f"Completed: {seasonCompleted}")
                season_info = {'in_progress':seasonInProgress,'last_completed':seasonCompleted}
                return season_info
                # return seasonInProgress, seasonCompleted

        seasonInProgress = None

        for row in range(len(df_dates)):
            # if df_dates.iloc[row].start.date().year == current_date.year:
            if df_dates.iloc[row].start.year == current_date.year:
                curr_year_row = df_dates.iloc[row]
                # if current_date < curr_year_row.start.date():
                if current_date < curr_year_row.start:
                    seasonCompleted = current_date.year - 1
                    # print(f"On date: {current_date}:")
                    # print(f"InProgress: {seasonInProgress}")
                    # print(f"Completed: {seasonCompleted}")
                    season_info = {'in_progress':seasonInProgress,'last_completed':seasonCompleted}
                    return season_info
                    # return seasonInProgress, seasonCompleted

                # elif current_date > curr_year_row.end.date():
                elif current_date > curr_year_row.end:
                    seasonCompleted = current_date.year
                    # print(f"On date: {current_date}:")
                    # print(f"InProgress: {seasonInProgress}")
                    # print(f"Completed: {seasonCompleted}")
                    season_info = {'in_progress':seasonInProgress,'last_completed':seasonCompleted}
                    return season_info
                    # return seasonInProgress, seasonCompleted

                else:
                    print("Hmm...didn't work. Here is the dataframe again\n")
                    return df_dates

    except Exception as e:
        print(e)
        try:
            pass
            # conn.close()
        except Exception as e:
            print(e)

def get_hall_of_fame() -> pd.DataFrame:
    return pd.read_csv(HALL_OF_FAME_CSV,index_col=False)

def get_broadcasts_df() -> pd.DataFrame:
    return pd.read_csv(BROADCASTS_CSV,index_col=False)

def save_teams():
    try:
        df = get_teams_df()
        # df.to_csv(os.path.abspath('simplestats/baseball/teams_master.csv'),index=False)
        df.to_csv(TEAMS_CSV,index=False)
        print("Successfully saved TEAMS dataframe")
    except:
        print("ERROR: could not save TEAMS dataframe")

def save_yby_records():
    try:
        df = get_yby_records()
        # df.to_csv(os.path.abspath('simplestats/baseball/yby_records_master.csv'),index=False)
        df.to_csv(YBY_RECORDS_CSV,index=False)
        print("Successfully saved RECORDS dataframe")
    except:
        print("ERROR: could not save RECORDS dataframe")

def save_people():
    try:
        df = get_people_df()
        # df.to_csv(os.path.abspath('simplestats/baseball/people_master.csv'),index=False)
        df.to_csv(PEOPLE_CSV,index=False)
        print("Successfully saved PEOPLE dataframe")
    except:
        print("ERROR: could not save PEOPLE dataframe")

def save_bios():
    try:
        df = get_bios_df()
        # df.to_csv(os.path.abspath('simplestats/baseball/bios_master.csv'),index=False)
        df.to_csv(BIOS_CSV,index=False)
        print("Successfully saved BIOS dataframe")
    except:
        print("ERROR: could not save BIOS dataframe")

def save_seasons():
    try:
        df = get_seasons_df()
        # df.to_csv(os.path.abspath('simplestats/baseball/seasons_master.csv'),index=False)
        df.to_csv(SEASONS_CSV,index=False)
        print("Successfully saved SEASONS dataframe")
    except:
        print("ERROR: could not save SEASONS dataframe")

def save_venues():
    try:
        df = get_venues_df()
        # df.to_csv(os.path.abspath('simplestats/baseball/venues_master.csv'),index=False)
        df.to_csv(VENUES_CSV,index=False)
        print("Successfully saved VENUES dataframe")
    except:
        print("ERROR: could not save VENUES dataframe")

def save_standings():
    try:
        df = get_standings_df()
        # df.to_csv(os.path.abspath('simplestats/baseball/yby_standings_master.csv'),index=False)
        df.to_csv(YBY_STANDINGS_CSV,index=False)
        print("Successfully saved STANDINGS dataframe")
    except:
        print("ERROR: could not save STANDINGS dataframe")

def save_all():
    for s in (save_teams,save_yby_records,save_standings,save_people,save_bios,save_seasons,save_venues):
        s()