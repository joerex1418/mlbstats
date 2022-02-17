# import lxml
import requests
import pandas as pd
import datetime as dt
# from bs4 import BeautifulSoup as bs

from . import player
from . import team
from . import franchise
from . import league

from .async_mlb import get_leaders
from .async_mlb import get_team_responses

from .mlbdata import get_bios_df
from .mlbdata import get_people_df
from .mlbdata import get_season_info
from .mlbdata import get_teams_df
from .mlbdata import get_seasons_df
from .mlbdata import get_yby_records

from .constants import BASE
from .constants import BBREF_ENDPOINTS
from .constants import BBREF_BASE
from .constants import TEAMS
from .constants import BAT_FIELDS
from .constants import BAT_FIELDS_ADV
from .constants import PITCH_FIELDS
from .constants import PITCH_FIELDS_ADV
from .constants import FIELD_FIELDS
from .constants import STATDICT
from .constants import POSITION_DICT

from .utils import curr_year
from .utils import curr_date
from .utils import iso_format
from .utils import iso_format_ms
from .utils import utc_zone
from .utils import simplify_time
from .utils import prettify_time
from .utils import default_season
from .utils import game_str_display

__BASE = ""
 
class Player:
    """
    # Player

    playerID : str | int
        player's official MLB ID
    """
    def __init__(self,playerID):
        _player = player.player(playerID)
        self.mlbam              = _player["mlbam"]
        self.bbrefID            = _player["bbrefID"]
        self.retroID            = _player["retroID"]
        self.bbrefIDminors      = _player["bbrefIDminors"]
        self.primaryPosition    = _player["primaryPosition"]
        self.fullName           = _player["fullName"]
        self.firstName          = _player["firstName"]
        self.lastName           = _player["lastName"]
        self.nickName           = _player["nickName"]
        self.primaryNumber      = _player["primaryNumber"]
        self.currentAge         = _player["currentAge"]
        self.birthDate          = _player["birthDate"]
        self.birthCity          = _player["birthCity"]
        self.birthState         = _player["birthState"]
        self.birthCountry       = _player["birthCountry"]
        self.deathDate          = _player["deathDate"]
        self.deathCity          = _player["deathCity"]
        self.deathState         = _player["deathState"]
        self.deathCountry       = _player["deathCountry"]
        self.weight             = _player["weight"]
        self.height             = _player["height"]
        self.bats               = _player["bats"]
        self.throws             = _player["throws"]
        self.education          = _player["education"]
        self.firstYear          = _player["firstYear"]
        self.lastYear           = _player["lastYear"]
        self.zoneBot            = _player["zoneBot"]
        self.zoneTop            = _player["zoneTop"]
        self.isActive           = _player["isActive"]
        
        self.__roster_entries   = _player["rosterEntries"]
        self.__transactions     = _player["transactions"]
        self.__drafts           = _player["drafts"]
        self.__awards           = _player["awards"]


        try:
            self.birth_dt = dt.datetime.strptime(_player['birthDate'],r"%Y-%m-%d")
        except:
            self.birth_dt = None

        try:
            self.death_dt = dt.datetime.strptime(_player['deathDate'],r"%Y-%m-%d")
        except:
            self.death_dt = None
        

        # Stats Data
        hitting =  _player["hitting"]
        pitching = _player["pitching"]
        fielding = _player["fielding"]

        self.__hitting_recent = hitting["recentSeason"]
        self.__hitting_yby = hitting["yby"]
        self.__hitting_yby_adv = hitting["ybyAdv"]
        self.__hitting_career_reg = hitting["careerReg"]
        self.__hitting_caree_reg_adv = hitting["careerAdvReg"]
        self.__hitting_career_post = hitting["careerPost"]
        self.__hitting_career_post_adv = hitting["careerAdvPost"]

        self.__pitching_recent = pitching["recentSeason"]
        self.__pitching_yby = pitching["yby"]
        self.__pitching_yby_adv = pitching["ybyAdv"]
        self.__pitching_career_reg = pitching["careerReg"]
        self.__pitching_career_reg_adv = pitching["careerAdvReg"]
        self.__pitching_career_post = pitching["careerPost"]
        self.__pitching_career_post_adv = pitching["careerAdvPost"]

        self.__fielding_recent = fielding["recentSeason"]
        self.__fielding_career_reg = fielding["careerReg"]
        self.__fielding_career_post = fielding["careerPost"]
        self.__fielding_yby = fielding["yby"]

    def __repr__(self) -> str:
        if self.isActive:
            status = "active"
        else:
            status = "inactive"
        return f"""<SimpleStats Player - '{self.fullName}' | mlbam ID: '{self.mlbam}' | baseball-reference ID: '{self.bbrefID}' | status: '{status}'>"""

    def __getitem__(self,item):
        return getattr(self,item)
    
    def __setitem__(self,key,value):
        setattr(self,key,value)

    def career_stats(self,group,advanced=False):
        """
        # Career Stats
        Returns `pandas.Dataframe` of player's CAREER stats:

        - Valid values for `group` parameter are "hitting", "pitching", or "fielding"
        - If `advanced` is `True`, class method will return a dataframe of player's advanced stats"""
        if group == "hitting":
            if advanced is True:
                return self.__hitting_caree_reg_adv
            return self.__hitting_career_reg
        elif group == "pitching":
            if advanced is True:
                return self.__pitching_career_reg_adv
            return self.__pitching_career_reg
        elif group == "fielding":
            return self.__fielding_career_reg

    def yby_stats(self,group,advanced=False):
        """
        # Year-by-Year Stats
        Returns `pandas.Dataframe` of player's YEAR-BY-YEAR stats:
        - Valid values for `group` parameter are "hitting", "pitching", or "fielding"
        - If `advanced` is `True`, class method will return a dataframe of player's advanced stats
        """
        if group == "hitting":
            if advanced is True:
                return self.__hitting_yby_adv.sort_values(by="Year",ascending=False)
            return self.__hitting_yby.sort_values(by="Year",ascending=False)
        elif group == "pitching":
            if advanced is True:
                return self.__pitching_yby_adv.sort_values(by="Year",ascending=False)
            return self.__pitching_yby.sort_values(by="Year",ascending=False)
        elif group == "fielding":
            return self.__fielding_yby.sort_values(by="Year",ascending=False)

    def summary_stats(self,group):
        """
        # Recent Season Stats
        Returns `pandas.Dataframe` of player's most RECENT SEASON stats:
        - Valid values for `group` parameter are "hitting", "pitching", or "fielding"
        - If `advanced` is `True`, class method will return a dataframe of player's advanced stats
        """

        if group == "hitting":
            try:
                recentYear = self.__hitting_yby.sort_values(by="Year",ascending=False)
                recentYear = recentYear.iloc[[0]]
                year_value = recentYear["Year"].item()
                combined = pd.concat([self.__hitting_career_reg,recentYear]).drop(columns=["Year","Team"])
                combined.insert(0,"",["Career",year_value])
                return combined
            except:
                return None
        elif group == "pitching":
            try:
                recentYear = self.__pitching_yby.sort_values(by="Year",ascending=False)
                recentYear = recentYear.iloc[[0]]
                year_value = recentYear["Year"].item()
                combined = pd.concat([self.__pitching_career_reg,recentYear]).drop(columns=["Year","Team"])
                combined.insert(0,"",["Career",year_value])
                return combined
            except:
                return None
        elif group == "fielding":
            try:
                recentYear = self.__fielding_yby.sort_values(by="Year",ascending=False)
                year_value = recentYear.iloc[0]["Year"]
                recentYear = recentYear[(recentYear["Year"]==year_value) & (recentYear["Pos"]!="DH")]
                recentYear.insert(0,"",year_value)
                recentYear.sort_values(by="G",ascending=False,inplace=True)

                career = self.__fielding_career_reg
                career = career[career["Pos"]!="DH"]
                career.insert(0,"","Career")

                combined = pd.concat([career,recentYear]).drop(columns=["Year","Team"]).fillna("--")
                return combined
            except:
                return None

    def game_logs(self,group,year=None):
        """
        Returns `pandas.Dataframe` of player's stats for each game in a given season:
        """
        if year is None: year = curr_year
        if group == "hitting":
            log = player.hittingLog(self.bbrefID,year)
        elif group == "pitching":
            log = player.pitchingLog(self.bbrefID,year)
        elif group == "fielding":
            log = player.fieldingLog(self.bbrefID,year)
        return log

    def metricLog(self):
        """NOT YET CONFIGURED"""
        pass

    def play_log(self):
        """NOT YET CONFIGURED"""
        pass

    def hitting_splits(self,year=curr_year):
        bat_splits = player.bbrefSplits(self.bbrefID,year,"b")
        return bat_splits

    def pitching_splits(self,year=curr_year):
        _pitch_splits = player.bbrefSplits(self.bbrefID,year,"p")
        return _pitch_splits

    def roster_entries(self):
        return self.__roster_entries

    def arsenal(self):
        pass

    def draft(self):
        return self.__drafts

    def awards(self):
        return self.__awards

    def transactions(self):
        return self.__transactions


class Team:
    """
    Team
    ====

    teamID : str | int
        Team's official MLB ID
    season : str | int
        specific season to retrieve data for
    """
    def __init__(self,teamID,season=None) -> None:
        if season is None:
            season = default_season()

        self.__info             = team.team(teamID,season)
        self.__year             = season
        self.__team_df          = self.__info["team_df"]
        self.__franchise_df     = self.__info["franchise_df"]
        self.__mlbam            = self.__info["mlbam"]
        self.__bbrefID          = self.__info["bbrefID"]
        self.__franchID         = self.__info["franchID"]
        self.__retroID          = self.__info["retroID"]
        self.__mlbID            = self.__info["mlbID"]
        self.__fullName         = self.__info["fullName"]
        self.__lgAbbrv          = self.__info["lgAbbrv"]
        self.__locationName     = self.__info["locationName"]
        self.__clubName         = self.__info["clubName"]
        self.__venueName        = self.__info["venueName"]
        self.__venue_mlbam      = self.__info["venue_mlbam"]
        self.__firstYear        = self.__info["firstYear"]

        async_response = get_team_responses(self.__mlbam,season)
        self.__roster_stats        = async_response["roster_stats"]
        self.__game_log          = async_response["game_log"]
        self.__game_stats        = async_response["game_stats"]
        self.__team_stats        = async_response["team_stats"]
        self.__leaders_hitting   = async_response["leaders_hitting"]
        self.__leaders_pitching  = async_response["leaders_pitching"]
        self.__leaders_fielding  = async_response["leaders_fielding"]
        self.__transactions      = async_response["transactions"]
        self.__draft             = async_response["draft"]

        self.__hitting = self.__team_stats["hitting"]
        self.__hittingAdv = self.__team_stats["hittingAdvanced"]
        self.__pitching = self.__team_stats["pitching"]
        self.__pitchingAdv = self.__team_stats["pitchingAdvanced"]
        self.__fielding = self.__team_stats["fielding"]

        ybyRecords = get_yby_records()
        self.__yby_records = ybyRecords[(ybyRecords["season"]==season) & (ybyRecords["tm_mlbam"]==self.__mlbam)]

        self.__div_mlbam = self.__yby_records.div_mlbam.item()
        self.__div_standings = ybyRecords[(ybyRecords["season"]==season) & (ybyRecords["div_mlbam"]==self.__div_mlbam)]

        self.__wins      = self.__yby_records.W.item()
        self.__losses    = self.__yby_records.L.item()

        # self.__records_dict = self.__team_stats["records_dict"]

        # stats = team.stats(self.__mlbam)
        # self.__hitting = stats["hitting"]
        # self.__hittingAdv = stats["hittingAdvanced"]
        # self.__pitching = stats["pitching"]
        # self.__pitchingAdv = stats["pitchingAdvanced"]
        # self.__fielding = stats["fielding"]
        # self.__records_dict = stats["records_dict"]

        # self.__roster_stats = team.roster_stats(self.__mlbam,season=self.__year)
        # self.game_log = team.game_log(self.__mlbam,season=self.__year)

        self.__set_team_attrs()        

    def __repr__(self) -> str:
        return f"""< SimpleStats Team - '{self.__fullName}' | mlbam ID: '{self.__mlbam}' | baseball-reference ID: '{self.__bbrefID}' >"""

    def __getitem__(self,item):
        return self.__attrs[item]

    def __set_team_attrs(self):
        self.__attrs = {
            "mlbam":        self.__mlbam,
            "bbrefID":      self.__bbrefID,
            "franchID":     self.__franchID,
            "fullName":     self.__fullName,
            "wins":         self.__wins,
            "losses":       self.__losses
        }


    def full_name(self):
        """Return the team's full name"""
        return self.__fullName
    
    def club_name(self):
        """Returns the team's club name"""
        return self.__clubName 

    def loc_name(self):
        """Return CITY that team is named after"""
        return self.__locationName

    def season_record(self):
        return f"{self.__wins}-{self.__losses}"

    def standings_division(self):
        cols = ["tm_mlbam","tm_name","G","W","L","R","RA","RunDiff"]
        df = self.__div_standings[cols]
        return df

    def franchise_df(self):
        return self.__franchise_df

    def leaders(self,group,return_type="dict"):
        """
        Returns a dictionary (or df -- COMING SOON) of top players in different categories
        
        Required:
        --------
        - group:    'hitting', 'pitching', 'fielding' ('h', 'p', 'f' for shorthand)
        
        - return_type:  'dict' or 'df'
        """
        if group == "hitting" or group == "h":
            categories = {}
            rows = []
            leaders = self.__leaders_hitting
            for cat,leaders in leaders.items():
                rows = []
                for leader in leaders:
                    row = []
                    for val in leader.values():
                        row.append(val)
                    rows.append(row)
                df = pd.DataFrame(data=rows,columns=list(leaders[0].keys()))
                categories[cat] = df
            return categories


        elif group == "pitching" or group == "p":
            return self.__leaders_pitching
        elif group == "fielding" or group == "f":
            return self.__leaders_fielding

    def season_records(self):
        return self.__yby_records

    def venue(self):
        return self.__venueName

    def hitting(self,*args):
        totals = False
        if len(args) == 1 and args[0] == 'totals':
            totals = True
        if totals is False:
            df = self.__roster_stats["hitting"]
            all_p = df[df["primaryPosition"]=="P"]
            all_non_p = df[df["primaryPosition"]!="P"]
            df = pd.concat([all_non_p,all_p])
        else:
            df = self.__hitting

        return df

    def hitting_advanced(self,*args):
        totals = False
        if len(args) == 1 and args[0] == 'totals':
            totals = True
        if totals is False:
            df = self.__roster_stats["hittingAdvanced"]
            all_p = df[df["primaryPosition"]=="P"]
            all_non_p = df[df["primaryPosition"]!="P"]
            df = pd.concat([all_non_p,all_p])
        else:
            df = self.__hittingAdv

        return df

    def pitching(self,*args):
        totals = False
        if len(args) == 1 and args[0] == 'totals':
            totals = True
        if totals is False:
            df = self.__roster_stats["pitching"]
            all_p = df[df["primaryPosition"]=="P"]
            all_non_p = df[df["primaryPosition"]!="P"]
            df = pd.concat([all_p,all_non_p])
        else:
            df = self.__pitching

        return df

    def pitching_advanced(self,*args):
        totals = False
        if len(args) == 1 and args[0] == 'totals':
            totals = True
        if totals is False:
            df = self.__roster_stats["pitchingAdvanced"]
            all_p = df[df["primaryPosition"]=="P"]
            all_non_p = df[df["primaryPosition"]!="P"]
            df = pd.concat([all_p,all_non_p])
        else:
            df = self.__pitchingAdv

        return df

    def fielding(self,*args):
        totals = False
        if len(args) == 1 and args[0] == 'totals':
            totals = True
        if totals is False:
            df = self.__roster_stats["fielding"]
        else:
            df = self.__fielding

        return df

    def game_stats(self,group=None):
        """
        Get hitting, pitching, and fielding stats for each game for the given year

        (Not fully developed yet)
        ADD FUNCTIONALITY TO GROUP BY OPPOSING TEAM
        """
        stats = self.__game_stats
        hitting = stats["hitting"].drop(columns=["G#"]) # column may be misleading to users but is helpful to ensure correct order of games when it comes to double headers
        pitching = stats["pitching"].drop(columns=["G#"])
        fielding = stats["fielding"].drop(columns=["G#"])
        if group is None:
            return {"hitting":hitting,"pitching":pitching,"fielding":fielding}
        else:
            return stats[group].drop(columns=["G#"])

    def game_log(self):
        stats = self.__game_log
        return stats

    def team_splits(self,season=None,s_type="b"):
        if season is None:
            season = self.__year
        return team.bbrefSplits(self.__bbrefID,season=season,s_type=s_type)

    def transactions(self):
        df = self.__transactions
        df.sort_values(by=["date"],ascending=False,inplace=True)
        colmap = {
            "player":"Player",
            "f_team":"From Team",
            "t_team":"To Team",
            "date":"Date",
            "eff_date":"Effective Date",
            "trx_code":"Code",
            "trx_type":"Transaction",
            "description":"Description"
        }
        df = df[["player","f_team","t_team","date","eff_date","trx_code","trx_type","description"]]
        return df.rename(columns=colmap)

    def draft_picks(self):
        d = self.__draft.to_dict("records")
        return self.__draft


    def team_hitting(self):
        """
        Get ALL team hitting stat totals/avgs
        """
        reg = self.__hitting
        adv = self.__hittingAdv
        df = reg.merge(adv)

        return df

    def team_hitting_reg(self):
        """
        Get REGULAR team hitting stat totals/avgs
        """
        df = self.__hitting

        return df

    def team_hitting_adv(self):
        """
        Get ADVANCED team hitting stat totals/avgs
        """
        df = self.__hittingAdv
        return df

    def team_pitching(self):
        """
        Get ALL team pitching stat totals/avgs
        """
        reg = self.__pitching
        adv = self.__pitchingAdv
        df = reg.merge(adv)
        return df

    def team_pitching_reg(self):
        """
        Get REGULAR team pitching stat totals/avgs
        """
        cols = ['GP','GS','GF','W','L','W%','AB','ERA','H','R','ER','2B','3B','HR','SO','BB','IBB','WHIP','IP','P','TB','K','K%','BF','HBP','CG','ShO','SV', 'SVO','BS','HLD']
        df = self.__pitching.merge(self.__pitchingAdv)
        return df[cols]

    def team_pitching_adv(self):
        """
        Get REGULAR team pitching stat totals/avgs
        """
        cols = ['AVG','OBP','SLG','OPS','CS','SB','SB%','O','GO','AO', 'GO/AO','GIDP','BK','WP','PK','sB','sF','P/Inn','SO:BB','SO/9','BB/9','H/9','R/9','HR/9','CI','IR','IRS','BABIP','QS','GIDPO','TS','Whiffs','BIP','RS','P/PA','BB/PA','SO/PA','HR/PA','BB/SO','ISO','FO','PO','LO','GH','FH','PH','LH']
        df = self.__pitching.merge(self.__pitchingAdv)
        return df[cols]

    def team_fielding(self):
        """Get team fielding stat totals/avgs"""
        df = self.__fielding
        return df

    def player_hitting(self):
        """
        Get hitting stats for all players on team
        """
        reg = self.__roster_stats["hitting"]
        adv = self.__roster_stats["hittingAdvanced"]
        df = reg.merge(adv)
        allPs = df[df["primaryPosition"]=="P"]
        nonPs = df[df["primaryPosition"]!="P"]
        df = pd.concat([nonPs,allPs])
        return df

    def player_hitting_reg(self):
        """
        Get REGULAR hitting stats for all players on team
        """

        df = self.__roster_stats["hitting"]
        return df

    def player_hitting_adv(self):
        """
        Get ADVANCED hitting stats for all players on team
        """
        df = self.__roster_stats["hittingAdvanced"]
        return df

    def player_pitching(self):
        """
        Get pitchings stats for all pitchers on team
        """
        reg = self.__roster_stats["pitching"]
        adv = self.__roster_stats["pitchingAdvanced"]
        df = reg.merge(adv)
        return df

    def player_pitching_reg(self):
        """
        Get REGULAR pitching stats for all pitchers on team
        """
        df = self.__roster_stats["pitching"]
        return df

    def player_pitching_adv(self):
        """
        Get ADVANCED pitching stats for all pitchers on team
        """
        df = self.__roster_stats["pitchingAdvanced"]
        return df

    def player_fielding(self):
        """
        Get fielding stats for all players on team
        """
        df = self.__roster_stats["fielding"]
        active = df[df["status"]=="Active"]
        non_active = df[df["status"]!="Active"]
        df = pd.concat([active,non_active])
        return df


class Franchise:
    """
    # Franchise

    teamID : str | int
        Team's official MLB ID
    """
    def __init__(self,teamID):
        _info = franchise.franchise_info(teamID)

        self.__franchise_df      = _info["franchise_df"]
        self.firstYear           = _info["firstYear"]
        self.recentYear          = _info["recentYear"]
        self.fullName            = _info["fullName"]
        self.locationName        = _info["locationName"]
        self.clubName            = _info["clubName"]
        self.mlbam               = _info["mlbam"]
        self.franchID            = _info["franchID"]
        self.bbrefID             = _info["bbrefID"]
        self.retroID             = _info["retroID"]
        self.fileCode            = _info["fileCode"]
        self.__lgDiv             = _info["lgDiv"]
        self.curr_venue_mlbam  = _info["venueCurrent_mlbam"]
        self.__venueList         = _info["venueList"]
        self.venues            = _info["venueDict"]

        self.__ybyRecords        = franchise.franchise_records(teamID)
        self.__ybyStats          = franchise.franchise_stats(self.mlbam)

        self.__total_GP          = self.__ybyRecords.G.astype("int").sum()
        self.__total_wins        = self.__ybyRecords.W.astype("int").sum()
        self.__total_losses      = self.__ybyRecords.L.astype("int").sum()
        self.gp                  = self.__total_GP
        self.w                   = self.__total_wins
        self.l                   = self.__total_losses

        # self.__postseason_appearances = _info["postseason_appearances"] # postseason appearance function is WIP

        self.__attrs = {
            "franchise":        self.__franchise_df,
            "firstyear":        self.firstYear,
            "recentyear":       self.recentYear,
            "fullname":         self.fullName,
            "locationname":     self.locationName,
            "clubname":         self.clubName,
            "mlbam":            self.mlbam,
            "franchid":         self.franchID,
            "bbrefid":          self.bbrefID,
            "retroid":          self.retroID,
            "filecode":         self.fileCode,
            "league":           self.__lgDiv,
            "venue_mlbam":      self.curr_venue_mlbam,
            "venues":           self.venues,
            "gp":               self.__total_GP,
            "gamesplayed":      self.__total_GP,
            "games_played":     self.__total_GP,
            "wins":             self.__total_wins,
            "w":                self.__total_wins,
            "losses":           self.__total_losses,
            "l":                self.__total_losses,
        }
    
    def __repr__(self) -> str:
        return f"""<SimpleStats Franchise - '{self.fullName}' | mlbam ID: '{self.mlbam}' | baseball-reference ID: '{self.bbrefID}' | years: '{self.firstYear} - {self.recentYear}'>"""

    def __getitem__(self,item):
        return getattr(item)

    def info(self):
        numYears = self.recentYear - self.firstYear+1
        activeYears = f"{self.firstYear} - {self.recentYear}"
        games_played = self.__total_GP
        wins = self.__total_wins
        losses = self.__total_losses
        venues = self.venues
        return {"numYears":numYears,"activeYears":activeYears,"gamesPlayed":games_played,"totalWins":wins,"totalLosses":losses,"venues":venues}

    def ybyRecords(self,return_type="df"):
        if return_type == "df":
            return self.__ybyRecords.reset_index(drop=True)
        elif return_type == "prefmt":
            # return tabulate(self.__ybyRecords.reset_index(drop=False),"keys","simple",numalign="center",stralign="left")
            pass

    def ybyStats(self,group,combined=True,return_type="df"):
        """Doc string needed"""
        if group.lower() not in ("hitting","hittingadv","pitching","pitchingadv","fielding"):
            print(f"get value, '{group}' not a valid parameter")
            return None

        if combined is False:
            return self.__ybyStats[group]

        if "hitting" in group.lower():
            reg_df = self.__ybyStats["hitting"].set_index("Season",drop=False)
            adv_df = self.__ybyStats["hittingAdv"].set_index("Season")
            combined = reg_df.join(adv_df)
            combined = combined.reset_index(drop=True)
        elif "pitching" in group.lower():
            reg_df = self.__ybyStats["pitching"].set_index("Season",drop=False)
            adv_df = self.__ybyStats["pitchingAdv"].set_index("Season")
            combined = reg_df.join(adv_df)
            combined = combined.reset_index(drop=True)
        elif "fielding" in group.lower():
            combined = self.__ybyStats["fielding"]
        else:
            print("Options are 'hitting', 'pitching' or 'fielding'")
            return None
        if return_type == "df":
            return combined
        elif return_type == "prefmt":
            # return tabulate(combined,"keys","simple",numalign="center",stralign="left",showindex=False)
            pass
        elif return_type == "html":
            return combined

    def playoffHistory(self):
        pass # postseason appearance function is WIP
        # return self.__postseason_appearances

    def mlbam(self):
        return self.mlbam

    def franchID(self):
        return self.franchID

    def fullName(self,year=None):
        if year is None:
            return self.fullName
        else:
            return self.__franchise_df[self.__franchise_df["yearID"]==year]["fullName"].item()

    def locationName(self,year=None):
        if year is None:
            return self.locationName
        else:
            return self.__franchise_df[self.__franchise_df["yearID"]==year]["locationName"].item()

    def clubName(self,year=None):
        if year is None:
            return self.clubName
        else:
            return self.__franchise_df[self.__franchise_df["yearID"]==year]["clubName"].item()

    def bbrefID(self,year=None):
        if year is None:
            return self.bbrefID
        else:
            return self.__franchise_df[self.__franchise_df["yearID"]==year]["bbrefID"].item()

    def retroID(self,year=None):
        if year is None:
            return self.retroID
        else:
            return self.__franchise_df[self.__franchise_df["yearID"]==year]["retroID"].item()


class League:
    """Represents an instance of MLB for given season (default is the current year)
    
    Params
    ------
    season : str or int
        season (year) to retrieve league stats/data for
    
    Methods
    -------
    team_hitting() - get league hitting stats by team
    
    team_pitching() - get league pitching stats by team
    
    team_fielding() - get league fielding stats by team
    
    player_hitting() - get league hitting stats by player
    
    player_pitching() - get league pitching stats by player
    
    player_fielding() - get league fielding stats by player
        
    leaders() - not configured
    
    team_splits() - not configured
    

    """
    def __init__(self,season=None):
        if season is None:
            self.season = curr_year
        else:
            self.season = int(season)

        with requests.Session() as sesh:
            self._teamstats = league.team_stats(self.season,sesh=sesh)
            self._playerstats = league.player_stats(self.season,sesh=sesh)
            # self._standings = league.standings(self.season,sesh=sesh)
        
        self._teamHitting = self._teamstats['hitting']
        self._teamPitching = self._teamstats['pitching']
        self._teamFielding = self._teamstats['fielding']
        self._teamHittingAdvanced = self._teamstats['hittingAdvanced']
        self._teamPitchingAdvanced = self._teamstats['pitchingAdvanced']

        self._playerHitting = self._playerstats['hitting']
        self._playerPitching = self._playerstats['pitching']
        self._playerFielding = self._playerstats['fielding']
        self._playerHittingAdvanced = self._playerstats['hittingAdvanced']
        self._playerPitchingAdvanced = self._playerstats['pitchingAdvanced']

        yby_records = get_yby_records()
        self._mlb_standings = yby_records[yby_records["season"]==self.season]
        
    def standings(self,*args):
        """Get Standings for a given season
        
        Return specified league or division standings by specifying `kwargs`
        
        Accepted args:
        -----------
        league: specify by league abbrv or league name

        leagueID: specify by leagueID (mlbam)

        division: specify by division abbrv or division name
                NOTE: must be specific, like `division="AL West"` (NOT `division = "West"`)

        divisionID: specify by divisionID

        """
        df = self._mlb_standings
        if len(args) == 0:
            return df

        lg_mlbams = []
        div_mlbams = []
        for arg in args:
            if type(arg) is int:
                if arg in [103,104]:
                    lg_mlbams.append(str(arg))
                if arg in [200,201,202,203,204,205]:
                    div_mlbams.append(str(arg))
            elif type(arg) is str:
                if arg.lower() in ["al","american","american league"]:
                    lg_mlbams.append('103')
                elif arg.lower() in ["nl","national","national league"]:
                    lg_mlbams.append('104')
                elif arg.lower() in ["alw","al w","alwest","al west"]:
                    div_mlbams.append('200')
                elif arg.lower() in ["ale","al e","aleast","al east"]:
                    div_mlbams.append('201')
                elif arg.lower() in ["alc","al c","alcentral","al central","alcent","al cent"]:
                    div_mlbams.append('202')
                elif arg.lower() in ["nlw","nl w","nlwest","nl west"]:
                    div_mlbams.append('203')
                elif arg.lower() in ["nle","nl e","nleast","nl east"]:
                    div_mlbams.append('204')
                elif arg.lower() in ["nlc","nl c","nlcentral","nl central","nlcent","nl cent"]:
                    div_mlbams.append('205')

        if lg_mlbams != []:
            df = df[df["lg_mlbam"].isin(lg_mlbams)]
        if div_mlbams != []:
            df = df[df["div_mlbam"].isin(div_mlbams)]

        return df.sort_values(by="W%",ascending=False)

    def team_hitting(self,league='all',division='all'):
        if type(division) is int:
            reg = self._teamHitting
            adv = self._teamHittingAdvanced
            df = reg.merge(adv)
            df = df[df['div_mlbam']==division]
            return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

        elif division == 'all':
            if league.lower() == 'all':
                reg = self._teamHitting
                adv = self._teamHittingAdvanced
                df = reg.merge(adv)
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._teamHitting
                adv = self._teamHittingAdvanced
                df = reg.merge(adv)
                df = df[df['Lg']=='AL']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._teamHitting
                adv = self._teamHittingAdvanced
                df = reg.merge(adv)
                df = df[df['Lg']=='NL']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

        elif 'west' in division.lower():
            if league.lower() == 'all':
                reg = self._teamHitting
                adv = self._teamHittingAdvanced
                df = reg.merge(adv)
                df = df[df['Div']=='West']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._teamHitting
                adv = self._teamHittingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='AL') & (df['Div']=='West')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._teamHitting
                adv = self._teamHittingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='NL') & (df['Div']=='West')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

        elif 'east' in division.lower():
            if league.lower() == 'all':
                reg = self._teamHitting
                adv = self._teamHittingAdvanced
                df = reg.merge(adv)
                df = df[df['Div']=='East']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._teamHitting
                adv = self._teamHittingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='AL') & (df['Div']=='East')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._teamHitting
                adv = self._teamHittingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='NL') & (df['Div']=='East')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

        elif 'central' in division.lower():
            if league.lower() == 'all':
                reg = self._teamHitting
                adv = self._teamHittingAdvanced
                df = reg.merge(adv)
                df = df[df['Div']=='Central']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._teamHitting
                adv = self._teamHittingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='AL') & (df['Div']=='Central')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._teamHitting
                adv = self._teamHittingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='NL') & (df['Div']=='Central')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

    def team_pitching(self,league='all',division='all'):
        if type(division) is int:
            reg = self._teamPitching
            adv = self._teamPitchingAdvanced
            df = reg.merge(adv)
            df = df[df['div_mlbam']==division]
            return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

        elif division == 'all':
            if league.lower() == 'all':
                reg = self._teamPitching
                adv = self._teamPitchingAdvanced
                df = reg.merge(adv)
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._teamPitching
                adv = self._teamPitchingAdvanced
                df = reg.merge(adv)
                df = df[df['Lg']=='AL']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._teamPitching
                adv = self._teamPitchingAdvanced
                df = reg.merge(adv)
                df = df[df['Lg']=='NL']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

        elif 'west' in division.lower():
            if league.lower() == 'all':
                reg = self._teamPitching
                adv = self._teamPitchingAdvanced
                df = reg.merge(adv)
                df = df[df['Div']=='West']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._teamPitching
                adv = self._teamPitchingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='AL') & (df['Div']=='West')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._teamPitching
                adv = self._teamPitchingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='NL') & (df['Div']=='West')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

        elif 'east' in division.lower():
            if league.lower() == 'all':
                reg = self._teamPitching
                adv = self._teamPitchingAdvanced
                df = reg.merge(adv)
                df = df[df['Div']=='East']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._teamPitching
                adv = self._teamPitchingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='AL') & (df['Div']=='East')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._teamPitching
                adv = self._teamPitchingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='NL') & (df['Div']=='East')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

        elif 'central' in division.lower():
            if league.lower() == 'all':
                reg = self._teamPitching
                adv = self._teamPitchingAdvanced
                df = reg.merge(adv)
                df = df[df['Div']=='Central']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._teamPitching
                adv = self._teamPitchingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='AL') & (df['Div']=='Central')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._teamPitching
                adv = self._teamPitchingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='NL') & (df['Div']=='Central')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

    def team_fielding(self,league='all',division='all'):

        if type(division) is int:
            df = self._teamFielding
            df = df[df['div_mlbam']==division]
            return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

        elif division == 'all':
            if league.lower() == 'all':
                df = self._teamFielding
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                df = self._teamFielding
                df = df[df['Lg']=='AL']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                df = self._teamFielding
                df = df[df['Lg']=='NL']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

        elif 'west' in division.lower():
            if league.lower() == 'all':
                df = self._teamFielding
                df = df[df['Div']=='West']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                df = self._teamFielding
                df = df[(df['Lg']=='AL') & (df['Div']=='West')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                df = self._teamFielding
                df = df[(df['Lg']=='NL') & (df['Div']=='West')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

        elif 'east' in division.lower():
            if league.lower() == 'all':
                df = self._teamFielding
                df = df[df['Div']=='East']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                df = self._teamFielding
                df = df[(df['Lg']=='AL') & (df['Div']=='East')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                df = self._teamFielding
                df = df[(df['Lg']=='NL') & (df['Div']=='East')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

        elif 'central' in division.lower():
            if league.lower() == 'all':
                df = self._teamFielding
                df = df[df['Div']=='Central']
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                df = self._teamFielding
                df = df[(df['Lg']=='AL') & (df['Div']=='Central')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                df = self._teamFielding
                df = df[(df['Lg']=='NL') & (df['Div']=='Central')]
                return df.sort_values(by='Team',ascending=True).reset_index(drop=True)

    def player_hitting(self,league='all',division='all'):
        if type(division) is int:
            reg = self._playerHitting
            adv = self._playerHittingAdvanced
            df = reg.merge(adv)
            df = df[df['div_mlbam']==division]
            return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

        elif division == 'all':
            if league.lower() == 'all':
                reg = self._playerHitting
                adv = self._playerHittingAdvanced
                df = reg.merge(adv)
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._playerHitting
                adv = self._playerHittingAdvanced
                df = reg.merge(adv)
                df = df[df['Lg']=='AL']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._playerHitting
                adv = self._playerHittingAdvanced
                df = reg.merge(adv)
                df = df[df['Lg']=='NL']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

        elif 'west' in division.lower():
            if league.lower() == 'all':
                reg = self._playerHitting
                adv = self._playerHittingAdvanced
                df = reg.merge(adv)
                df = df[df['Div']=='West']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._playerHitting
                adv = self._playerHittingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='AL') & (df['Div']=='West')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._playerHitting
                adv = self._playerHittingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='NL') & (df['Div']=='West')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

        elif 'east' in division.lower():
            if league.lower() == 'all':
                reg = self._playerHitting
                adv = self._playerHittingAdvanced
                df = reg.merge(adv)
                df = df[df['Div']=='East']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._playerHitting
                adv = self._playerHittingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='AL') & (df['Div']=='East')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._playerHitting
                adv = self._playerHittingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='NL') & (df['Div']=='East')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

        elif 'central' in division.lower():
            if league.lower() == 'all':
                reg = self._playerHitting
                adv = self._playerHittingAdvanced
                df = reg.merge(adv)
                df = df[df['Div']=='Central']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._playerHitting
                adv = self._playerHittingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='AL') & (df['Div']=='Central')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._playerHitting
                adv = self._playerHittingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='NL') & (df['Div']=='Central')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

    def player_pitching(self,league='all',division='all'):
        if type(division) is int:
            reg = self._playerPitching
            adv = self._playerPitchingAdvanced
            df = reg.merge(adv)
            df = df[df['div_mlbam']==division]
            return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

        elif division == 'all':
            if league.lower() == 'all':
                reg = self._playerPitching
                adv = self._playerPitchingAdvanced
                df = reg.merge(adv)
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._playerPitching
                adv = self._playerPitchingAdvanced
                df = reg.merge(adv)
                df = df[df['Lg']=='AL']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._playerPitching
                adv = self._playerPitchingAdvanced
                df = reg.merge(adv)
                df = df[df['Lg']=='NL']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

        elif 'west' in division.lower():
            if league.lower() == 'all':
                reg = self._playerPitching
                adv = self._playerPitchingAdvanced
                df = reg.merge(adv)
                df = df[df['Div']=='West']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._playerPitching
                adv = self._playerPitchingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='AL') & (df['Div']=='West')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._playerPitching
                adv = self._playerPitchingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='NL') & (df['Div']=='West')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

        elif 'east' in division.lower():
            if league.lower() == 'all':
                reg = self._playerPitching
                adv = self._playerPitchingAdvanced
                df = reg.merge(adv)
                df = df[df['Div']=='East']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._playerPitching
                adv = self._playerPitchingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='AL') & (df['Div']=='East')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._playerPitching
                adv = self._playerPitchingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='NL') & (df['Div']=='East')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

        elif 'central' in division.lower():
            if league.lower() == 'all':
                reg = self._playerPitching
                adv = self._playerPitchingAdvanced
                df = reg.merge(adv)
                df = df[df['Div']=='Central']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                reg = self._playerPitching
                adv = self._playerPitchingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='AL') & (df['Div']=='Central')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                reg = self._playerPitching
                adv = self._playerPitchingAdvanced
                df = reg.merge(adv)
                df = df[(df['Lg']=='NL') & (df['Div']=='Central')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

    def player_fielding(self,league='all',division='all'):

        if type(division) is int:
            df = self._playerFielding
            df = df[df['div_mlbam']==division]
            return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

        elif division == 'all':
            if league.lower() == 'all':
                df = self._playerFielding
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                df = self._playerFielding
                df = df[df['Lg']=='AL']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                df = self._playerFielding
                df = df[df['Lg']=='NL']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

        elif 'west' in division.lower():
            if league.lower() == 'all':
                df = self._playerFielding
                df = df[df['Div']=='West']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                df = self._playerFielding
                df = df[(df['Lg']=='AL') & (df['Div']=='West')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                df = self._playerFielding
                df = df[(df['Lg']=='NL') & (df['Div']=='West')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

        elif 'east' in division.lower():
            if league.lower() == 'all':
                df = self._playerFielding
                df = df[df['Div']=='East']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                df = self._playerFielding
                df = df[(df['Lg']=='AL') & (df['Div']=='East')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                df = self._playerFielding
                df = df[(df['Lg']=='NL') & (df['Div']=='East')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

        elif 'central' in division.lower():
            if league.lower() == 'all':
                df = self._playerFielding
                df = df[df['Div']=='Central']
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'al':
                df = self._playerFielding
                df = df[(df['Lg']=='AL') & (df['Div']=='Central')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)
            elif league.lower() == 'nl':
                df = self._playerFielding
                df = df[(df['Lg']=='NL') & (df['Div']=='Central')]
                return df.sort_values(by='Player',ascending=True).reset_index(drop=True)

    def leaders(self):
        pass

    def team_splits(self):
        pass


class Game:
    """Represents a single MLB game
    
    Paramaters
    ----------
    gameID : int or str
        numeric ID for the game
    
    timecode : str
        specify a value to retrieve a "snapshot" of the game at a specific point in time
        
        Format = "YYYYmmdd_HHMMDD"
    
    tz : str
        preferred timezone to view datetime values ("ct","et","mt", or "pt") 

    Methods:
    --------
    overview() -> str
        prints a boxscore-like visual as text

    boxscore() -> dict
        returns a python dictionary of boxscore information

    linescore() -> dict
        returns a python dictionary of the linescore in various formats

    situation() -> dict
        get a dict of information for the current situation

    diamond() -> dict
        returns a python dictionary detailing the current defensive lineup

    matchup_info() -> dict
        returns a python dictionary detailing information for the current matchup

    matchup_event_log() -> DataFrame
        returns a dataframe of events that have taken place for the current matchup/at-bat

    away_batting_stats() -> DataFrame
        returns a dataframe of game batting stats for the away team

    away_pitching_stats() -> DataFrame
        returns a dataframe of game pitching stats for the away team

    away_fielding_stats() -> DataFrame
        returns a dataframe of game fielding stats for the away team

    home_batting_stats() -> DataFrame
        returns a dataframe of game batting stats for the home team

    home_pitching_stats() -> DataFrame
        returns a dataframe of game pitching stats for the home team

    home_fielding_stats() -> DataFrame
        returns a dataframe of game fielding stats for the home team

    event_log() -> DataFrame
        returns a dataframe of every pitching event for every plate appearance thus far
    
    pa_log() -> DataFrame
        returns a dataframe of all plate appearance results

    get_content() -> dict
        returns a dictionary of broadcast information, highlight/recap video urls, and editorial data

    flags() -> dict
        returns a dictionary of notable attributes about the game
    """
    
    def __init__(self,gamePk,timecode=None,tz="ct"):
        game_id = gamePk
        self.__people = get_people_df()
        self.__tz = tz
        self.game_id = game_id
        self.gamePk  = game_id

        if timecode is None:
            timecodeQuery = ""
        else:
            timecodeQuery = f"&timecode={timecode}"

        BASE = "https://statsapi.mlb.com/api"
        game_url = BASE + f"/v1.1/game/{self.game_id}/feed/live?hydrate=venue,flags,preState{timecodeQuery}"

        gm = requests.get(game_url).json()

        gameData = gm["gameData"]
        liveData = gm["liveData"]

     # GAME Information
        self.__linescore    = liveData["linescore"]
        self.__boxscore     = liveData["boxscore"]
        self.__flags        = gameData["flags"]

        self.gameState      = gameData["status"]["abstractGameState"]
        self.game_state     = self.gameState
        self.detailedState  = gameData["status"]["detailedState"]
        self.detailed_state = self.detailedState
        self.info           = self.__boxscore["info"]
        self.sky            = gameData["weather"].get("condition","-")
        self.temp           = gameData["weather"].get("temp","-")
        self.wind           = gameData["weather"].get("wind","-")
        self.first_pitch    = gameData.get("gameInfo",{}).get("firstPitch","-")
        self.attendance     = gameData.get("gameInfo",{}).get("attendance","-")
        self.start          = gameData.get("datetime",{}).get("time","-")
        self.start_iso      = gameData.get("datetime",{}).get("dateTime","-")

        datetime = gameData["datetime"]
        self.game_date      = datetime["officialDate"]
        self.gameDate       = self.game_date
        self.daynight       = datetime["dayNight"]

        self.__venue        = gameData["venue"]

        self.__officials    = self.__boxscore.get("officials",[{},{},{},{}])

        self.__ump_home     = self.__officials[0].get("official",{})
        self.__ump_first    = self.__officials[1].get("official",{})
        self.__ump_second   = self.__officials[2].get("official",{})
        self.__ump_third    = self.__officials[3].get("official",{})
        self.__umpires      = {"home":self.__ump_home,"first":self.__ump_first,"second":self.__ump_second,"third":self.__ump_third}
     
     # ALL PLAYERS IN GAME
        self.__players = gameData["players"]

     # AWAY Team Data
        away = gameData["teams"]["away"]
        self.away_id                = away["id"]
        self.__away_team_full       = away["name"]
        self.__away_team            = away["clubName"]
        self.__away_team_abbrv      = away["abbreviation"]
        self.__away_record          = f'({away["record"]["wins"]},{away["record"]["losses"]})'
        self.__away_stats           = self.__boxscore["teams"]["away"]["teamStats"]
        self.__away_player_data     = self.__boxscore["teams"]["away"]["players"]
        self.__away_lineup          = self.__boxscore["teams"]["away"]["batters"]
        self.__away_starting_order  = self.__boxscore["teams"]["away"]["battingOrder"]
        self.__away_pitcher_lineup  = self.__boxscore["teams"]["away"]["pitchers"]
        self.__away_bullpen         = self.__boxscore["teams"]["away"]["bullpen"]
        self.__away_rhe             = self.__linescore["teams"]["away"]
        self.__away_bench           = self.__boxscore["teams"]["away"]["bench"]
        self.away_full              = self.__away_team_full
        self.away_club              = self.__away_team
        self.away_abbrv             = self.__away_team_abbrv

     # HOME Team Data
        home = gameData["teams"]["home"]
        self.home_id                = home["id"]
        self.__home_team_full       = home["name"]
        self.__home_team            = home["clubName"]
        self.__home_team_abbrv      = home["abbreviation"]
        self.__home_record          = f'({home["record"]["wins"]},{home["record"]["losses"]})'
        self.__home_stats           = self.__boxscore["teams"]["home"]["teamStats"]
        self.__home_player_data     = self.__boxscore["teams"]["home"]["players"]
        self.__home_lineup          = self.__boxscore["teams"]["home"]["batters"]
        self.__home_starting_order  = self.__boxscore["teams"]["home"]["battingOrder"]
        self.__home_pitcher_lineup  = self.__boxscore["teams"]["home"]["pitchers"]
        self.__home_bullpen         = self.__boxscore["teams"]["home"]["bullpen"]
        self.__home_rhe             = self.__linescore["teams"]["home"]
        self.__home_bench           = self.__boxscore["teams"]["home"]["bench"]
        self.home_full              = self.__home_team_full
        self.home_club              = self.__home_team
        self.home_abbrv             = self.__home_team_abbrv

        self.__curr_defense         = self.__linescore["defense"]
        self.__curr_offense         = self.__linescore["offense"]
        self.__curr_play            = liveData["plays"].get("currentPlay",{})

        self.balls                  = self.__linescore.get("balls","-")
        self.strikes                = self.__linescore.get("strikes","-")
        self.outs                   = self.__linescore.get("outs","-")
        self.inning                 = self.__linescore.get("currentInning","-")
        self.inning_ordinal         = self.__linescore.get("currentInningOrdinal","-")
        self.inning_state           = self.__linescore.get("inningState","-")

        self.__inn_half             = self.__linescore.get("inningHalf","-")
        self.__inn_label            = f"{self.__inn_half} of the {self.inning_ordinal}"

     # PLAYS and EVENTS
        self.__all_plays = liveData["plays"]["allPlays"]
        self.__scoring_plays = []

        self.__all_events = []
        self.__pitch_events = []
        self.__bip_events = []
        for play in self.__all_plays:
            for event in play["playEvents"]:
                self.__all_events.append(event)
                if event["isPitch"] == True:
                    self.__pitch_events.append(event)
                    if event["details"]["isInPlay"] == True:
                        self.__bip_events.append(event)
            try:
                if play["about"]["isScoringPlay"] == True:
                    self.__scoring_plays.append(play)
            except:
                pass

        self.__str_display = game_str_display(self)

    def __str__(self):
        return f"mlb.Game | gamePk {self.game_id} | {self.__away_team_abbrv} ({self.__away_rhe.get('runs',0)}) @ {self.__home_team_abbrv} ({self.__home_rhe.get('runs',0)})"

    def __repr__(self):
        return f"mlb.Game | gamePk {self.game_id} | {self.__away_team_abbrv} ({self.__away_rhe.get('runs',0)}) @ {self.__home_team_abbrv} ({self.__home_rhe.get('runs',0)})"

    def __getitem__(self,key):
        return getattr(self,key)

    def __call__(self, *args, **kwds):
        pass

    def overview(self):
        print(self.__str_display)

    def umpires(self,base=None):
        """
        Get info for umpires. If 'base' is None, all umpire data are returned
        """
        if base is None:
            return self.__umpires
        return self.__umpires[base]

    def boxscore(self,tz=None) -> dict:
        if tz is None:
            tz = self.__tz
        # compiles score, batting lineups, players on field, current matchup, count, outs, runners on base, etc.
        away = {"full":self.__away_team_full,"club":self.__away_team,"mlbam":self.away_id}
        home = {"full":self.__home_team_full,"club":self.__home_team,"mlbam":self.home_id}

        if self.__inn_half == "Top": # might have to adjust to using "inning state"
            team_batting = away
            team_fielding = home
        else:
            team_batting = home
            team_fielding = away

        diamond = self.diamond()
        situation = self.situation()
        matchup = self.matchup_info()
        scoreAway = self.__away_rhe.get("runs",0)
        scoreHome = self.__home_rhe.get("runs",0)
        firstPitch = prettify_time(self.first_pitch,tz=tz)
        scheduledStart = prettify_time(self.start_iso,tz=tz)
        umps = {"home":self.__ump_home,"first":self.__ump_first,"second":self.__ump_second,"third":self.__ump_third}

        return {
            "away":away,
            "home":home,
            "batting":team_batting,
            "fielding":team_fielding,
            "diamond":diamond,
            "situation":situation,
            "matchup":matchup,
            "score":{"away":scoreAway,"home":scoreHome},
            "gameState":self.gameState,
            "firstPitch":firstPitch,
            "scheduledStart":scheduledStart,
            "umpires":umps
            }
          
    def linescore(self) -> dict:
        """
        Returns a tuple of game's current linescore
        """

        ls = self.__linescore
        ls_total = {
            "away":{
                "runs":ls.get("teams",{}).get("away",{}).get("runs","-"),
                "hits":ls.get("teams",{}).get("away",{}).get("hits","-"),
                "errors":ls.get("teams",{}).get("away",{}).get("errors","-")
                },
            "home":{
                "runs":ls.get("teams",{}).get("home",{}).get("runs","-"),
                "hits":ls.get("teams",{}).get("home",{}).get("hits","-"),
                "errors":ls.get("teams",{}).get("home",{}).get("errors","-")
                }
        }

        ls_inns = []

        for inn in ls["innings"]:
            ls_inns.append({
            "away":{
                "runs":inn.get("away",{}).get("runs","-"),
                "hits":inn.get("away",{}).get("hits","-"),
                "errors":inn.get("away",{}).get("errors","-")
                },
            "home":{
                "runs":inn.get("home",{}).get("runs","-"),
                "hits":inn.get("home",{}).get("hits","-"),
                "errors":inn.get("home",{}).get("errors","-")
                },
            "inning":inn.get("num",""),
            "inningOrdinal":inn.get("ordinalNum","")
            
            })
        return {
            "total":ls_total,
            "innings":ls_inns,
            "away":{},
            "home":{}
        }
    
        rhe = self.__linescore["teams"]
        away_rhe = (rhe["away"]["runs"],rhe["away"]["hits"],rhe["away"]["errors"])
        home_rhe = (rhe["home"]["runs"],rhe["home"]["hits"],rhe["home"]["errors"])
        curr_inning = self.__linescore["currentInning"]
        headers = [""]
        for i in range(1,curr_inning+1):headers.append(i)
        for i in ["R","H","E"]:headers.append(i)
        away_row = [self.__away_team]
        home_row = [self.__home_team]
        inn_dfs = {}
        for inn in self.__linescore["innings"]:
            try:away_r = inn["away"]["runs"]
            except:away_r = "-"
            away_h = inn["away"]["hits"]
            away_e = inn["away"]["errors"]

            try:home_r = inn["home"]["runs"]
            except:home_r = "-"
            home_h = inn["home"]["hits"]
            home_e = inn["home"]["errors"]

            away_row_inn = [self.__away_team,away_r,away_h,away_e]
            home_row_inn = [self.__home_team,home_r,home_h,home_e]
            inn_df = pd.DataFrame(data=[away_row_inn,home_row_inn],columns=["","R","H","E"])
            inn_dfs[inn["num"]] = inn_df

            away_row.append(away_r)
            home_row.append(home_r)
        for i in away_rhe:away_row.append(i)
        for i in home_rhe:home_row.append(i)
        df = pd.DataFrame(data=[away_row,home_row],columns=headers)
        # return df,inn_dfs
        return inn_dfs

    def situation(self) -> dict:
        """Returns a python dictionary detailing the current game situation (count, outs, men-on, batting queue):

        outs : 
            number of current outs (int)
        balls : 
            number of current balls (int)
        strikes : 
            number of current strikes (int)
        runnersOn : dict
            dictionary with bases as keys and bools as values
        basesOccupied : 
            a list of integers representing currently occupied bases
        queue : 
            dictionary of batting team's next two batters (onDeck,inHole)
        """
        # outs, balls, strikes, runnersOn, batting queue
        try:
            onDeck = self.__curr_offense["onDeck"]
            inHole = self.__curr_offense["inHole"]
        except:
            return {
                    "outs":self.outs,
                    "balls":self.balls,
                    "strikes":self.strikes,
                    "runnersOn":{},
                    "basesOccupied":[],
                    "queue":{
                        "onDeck":{
                            "id":"",
                            "name":""
                            },
                        "inHole":{
                            "id":"",
                            "name":""
                            }
                        }
                    }

        matchup = self.__curr_play["matchup"]

        basesOccupied = []
        runnersOn = {}
        if "first" in self.__curr_offense.keys():
            basesOccupied.append(1)
            runnersOn["first"] = {
                "id":self.__curr_offense["first"]["id"],
                "name":self.__curr_offense["first"]["fullName"],
                "isOccuppied":True
                }
        else:runnersOn["first"] = {"isOccuppied":False}

        if "second" in self.__curr_offense.keys():
            basesOccupied.append(2)
            runnersOn["second"] = {
                "id":self.__curr_offense["second"]["id"],
                "name":self.__curr_offense["second"]["fullName"],
                "isOccuppied":True
                }
        else:runnersOn["second"] = {"isOccuppied":False}

        if "third" in self.__curr_offense.keys():
            basesOccupied.append(3)
            runnersOn["third"] = {
                "id":self.__curr_offense["third"]["id"],
                "name":self.__curr_offense["third"]["fullName"],
                "isOccuppied":True
                }
        else:runnersOn["third"] = {"isOccuppied":False}

        return {
            "outs":self.outs,
            "balls":self.balls,
            "strikes":self.strikes,
            "runnersOn":runnersOn,
            "basesOccupied":basesOccupied,
            "queue":{
                "onDeck":{
                    "id":onDeck["id"],
                    "name":onDeck["fullName"]
                    },
                "inHole":{
                    "id":inHole["id"],
                    "name":inHole["fullName"]
                    }
                }}

    def venue(self) -> dict:
        v = self.__venue
        venue_name = v["name"]
        venue_mlbam = v["id"]
        fieldInfo = v["fieldInfo"]
        capacity = fieldInfo["capacity"]
        roof = fieldInfo["roofType"]
        turf = fieldInfo["turfType"]
        try:
            dimensions = {
                "leftLine":fieldInfo["leftLine"],
                "leftCenter":fieldInfo["leftCenter"],
                "center":fieldInfo["center"],
                "rightCenter":fieldInfo["rightCenter"],
                "rightLine":fieldInfo["rightLine"]
            }
        except:
            dimensions = {
                "leftLine":None,
                "leftCenter":None,
                "center":None,
                "rightCenter":None,
                "rightLine":None
            }
        loc = v["location"]
        latitude = loc.get("defaultCoordinates",{}).get("latitude",None)
        longitude = loc.get("defaultCoordinates",{}).get("longitude",None)
        address1 = loc.get("address1",None)
        address2 = loc.get("address2",None)
        city = loc.get("city",None)
        state = loc.get("state",None)
        stateAbbrev = loc.get("stateAbbrev",None)
        zipCode = loc.get("postalCode",None)
        phone = loc.get("phone",None)

        return {
            "name":venue_name,
            "mlbam":venue_mlbam,
            "capacity":capacity,
            "roof":roof,
            "turf":turf,
            "dimensions":dimensions,
            "lat":latitude,
            "long":longitude,
            "address1":address1,
            "address2":address2,
            "city":city,
            "state":state,
            "stateAbbrev":stateAbbrev,
            "zipCode":zipCode,
            "phone":phone
        }

    def diamond(self,print_as_df=True):
        """
        Returns current defensive team's roster
        `print_as_df`: whether or not method will return pandas.Dataframe (return python dict if False)
                        default:`True`
        """
        try:
            diamond = {
                1:self.__curr_defense["pitcher"],
                2:self.__curr_defense["catcher"],
                3:self.__curr_defense["first"],
                4:self.__curr_defense["second"],
                5:self.__curr_defense["third"],
                6:self.__curr_defense["shortstop"],
                7:self.__curr_defense["left"],
                8:self.__curr_defense["center"],
                9:self.__curr_defense["right"],
            }
        except:
            return {}
        curr_diamond = []
        for key, value in diamond.items():
            curr_diamond.append([POSITION_DICT[key],value["fullName"]])

        df = pd.DataFrame(curr_diamond)

        diamond = {
            "pitcher":{"name":self.__curr_defense["pitcher"]["fullName"],"id":self.__curr_defense["pitcher"]["id"]},
            "catcher":{"name":self.__curr_defense["catcher"]["fullName"],"id":self.__curr_defense["catcher"]["id"]},
            "first":{"name":self.__curr_defense["first"]["fullName"],"id":self.__curr_defense["first"]["id"]},
            "second":{"name":self.__curr_defense["second"]["fullName"],"id":self.__curr_defense["second"]["id"]},
            "third":{"name":self.__curr_defense["third"]["fullName"],"id":self.__curr_defense["third"]["id"]},
            "shortstop":{"name":self.__curr_defense["shortstop"]["fullName"],"id":self.__curr_defense["shortstop"]["id"]},
            "left":{"name":self.__curr_defense["left"]["fullName"],"id":self.__curr_defense["left"]["id"]},
            "center":{"name":self.__curr_defense["center"]["fullName"],"id":self.__curr_defense["center"]["id"]},
            "right":{"name":self.__curr_defense["right"]["fullName"],"id":self.__curr_defense["right"]["id"]},
        }

        return diamond

    def matchup_info(self):
        """
        Gets current matchup info in form of python dictionary:

        Returned dict keys:
        * `at_bat`: current batter (dict)
        * `pitching`: current pitcher (dict)
        * `zone`: current batter's strike zone metrics (tuple)
        """


        try:
            matchup = self.__curr_play["matchup"]
            zoneTop = self.__curr_play["playEvents"][-1]["pitchData"]["strikeZoneTop"]
            zoneBot = self.__curr_play["playEvents"][-1]["pitchData"]["strikeZoneBottom"]
        except:
            return {"atBat":{},"pitching":{},"zone":(3.5,1.5)}
        atBat = {
            "name":matchup["batter"]["fullName"],
            "id":matchup["batter"]["id"],
            "bats":matchup["batSide"]["code"],
            "zoneTop":self.__players[f'ID{matchup["batter"]["id"]}']["strikeZoneTop"],
            "zoneBottom":self.__players[f'ID{matchup["batter"]["id"]}']["strikeZoneBottom"],
            "stands":matchup["batSide"]["code"]
            }
        pitching = {
            "name":matchup["pitcher"]["fullName"],
            "id":matchup["pitcher"]["id"],
            "throws":matchup["pitchHand"]["code"]}
        
        return {"atBat":atBat,"pitching":pitching,"zone":(zoneTop,zoneBot)}

    def matchup_event_log(self) -> pd.DataFrame:
        """
        Gets a pitch-by-pitch log of the current batter-pitcher matchup:

        Column labels:
        * `Pitch #`: pitch number for current matchup (current event included)
        * `Details`: details on pitch result (e.g. 'Ball', 'Called Strike', 'Ball In Dirt', 'Foul', etc.)
        * `Pitch`: pitch type (e.g. 'Curveball', 'Four-Seam Fastball')
        * `Release Speed`: pitch speed when released (in MPH)
        * `End Speed`: pitch speed when ball crosses plate (in MPH)
        * `Zone`: strike zone
        * `Spin`: ball spin rate (in RPM)
        * `pX`: x-coordinate (horizontal) of pitch (in feet)
        * `pZ`: z-coordinate (vertical) of pitch relative to batter's strike zone (in feet)
        * `hX`: x-coordinate 
        * `hY`: y-coordinate
        * `Hit Location`: location the ball was hit to, if applicable (field pos: 1-9)

        """
        
        headers = [
            "pitch_num",
            "details",
            "zoneTop",
            "zoneBottom",
            "zoneTopInitial",
            "zoneBottomInitial",
            "pitch_type",
            "pitch_code",
            "release_speed",
            "end_speed",
            "spin",
            "zone",
            "pX",
            "pZ",
            "hit_location",
            "hX",
            "hY",
            "play_id"]
        try:
            pa_events = self.__curr_play["playEvents"]
        except:
            empty_df = pd.DataFrame(columns=headers)
            return empty_df

        events_data = []

        for ab_log in pa_events:
            play_id = ab_log.get("playId")
            if ab_log["isPitch"] == False:
                pass
            else:
                event = []

                pitchNumber = ab_log["pitchNumber"]

                details = ab_log["details"]["description"]

                try:
                    pitchType = ab_log["details"]["type"]["description"]
                    pitchCode = ab_log["details"]["type"]["code"]
                except:
                    pitchType = "unknown"
                    pitchCode = "unknown"

                try:start_vel = ab_log["pitchData"]["startSpeed"]
                except:start_vel = "--"

                try:end_vel = ab_log["pitchData"]["endSpeed"]
                except:end_vel = "--"

                try:pX_coord = ab_log["pitchData"]["coordinates"]["pX"]
                except:pX_coord = "--"
                try:pZ_coord = ab_log["pitchData"]["coordinates"]["pZ"]
                except:pZ_coord = "--"

                try:
                    zoneTopInitial = pa_events[0]["pitchData"]["strikeZoneTop"]
                    zoneBottomInitial = pa_events[0]["pitchData"]["strikeZoneBottom"]
                except:
                    try:
                        zoneTopInitial = pa_events[0]["pitchData"]["strikeZoneTop"]
                        zoneBottomInitial = pa_events[0]["pitchData"]["strikeZoneBottom"]
                    except:
                        zoneTopInitial = 3.5
                        zoneBottomInitial = 1.5
                try:
                    zoneTop = ab_log["pitchData"]["strikeZoneTop"]
                    zoneBottom = ab_log["pitchData"]["strikeZoneBottom"]
                except:
                    zoneTop = 3.5
                    zoneBottom = 1.5

                try:spin = ab_log["pitchData"]["breaks"]["spinRate"]
                except:spin = ""
                try:zone = ab_log["pitchData"]["zone"]
                except:zone = ""
                try:hit_location = ab_log["hitData"]["location"]
                except:hit_location = ""
                try:
                    hX = ab_log["hitData"]["coordinates"]["coordX"]
                    hY = ab_log["hitData"]["coordinates"]["coordY"]
                except:
                    hX = ""
                    hY = ""

                event.append(pitchNumber)
                event.append(details)
                event.append(zoneTop)
                event.append(zoneBottom)
                event.append(zoneTopInitial)
                event.append(zoneBottomInitial)
                event.append(pitchType)
                event.append(pitchCode)
                event.append(start_vel)
                event.append(end_vel)
                event.append(spin)
                event.append(zone)
                event.append(pX_coord)
                event.append(pZ_coord)
                event.append(hit_location)
                event.append(hX)
                event.append(hY)
                event.append(play_id)

                events_data.append(event)

        matchup_df = pd.DataFrame(data=events_data,columns=headers)
        matchup_df.sort_values(by=["pitch_num"],inplace=True)

        return matchup_df

    def pa_log(self) -> pd.DataFrame:
        """
        Get detailed log of each plate appearance in game

        Note:
        ----------
            Dataframe begins with most recent plate appearance
        """
        headers = [
            "bat_teamID",
            "bat_team",
            "pa",
            "inning",
            "batter",
            "bat_side",
            "pitcher",
            "pa_pitch_count",
            "event",
            "event_type",
            "details",
            "pitch_type",
            "pitch_code",
            "release_velocity",
            "end_velocity",
            "spin_rate",
            "zone",
            "exit_velocity",
            "launch_angle",
            "distance",
            "location",
            "hit_trajectory",
            "hX",
            "hY",
            "category",
            "timeElasped",
            "timeStart",
            "timeEnd",
            "batterID",
            "pitcherID",
            "isHome",
            "play_id"
            ]

        events_data = []
        for play in self.__all_plays:
            for e in play["playEvents"]:
                if "game_advisory" in e.get("details",{}).get("eventType","").lower():
                    pass
                else:
                    firstEvent = e
                    break
            play_id = play["playEvents"][-1].get("playId","-")
            lastEvent = play["playEvents"][-1]
            pitchData = lastEvent["pitchData"]
            try:ab_num = play["atBatIndex"] + 1
            except:ab_num = "--"
            try:bat_side = play["matchup"]["batSide"]["code"]
            except:bat_side = "--"

            try:innNum = play["about"]["inning"]
            except:innNum = "--"
            try:innHalf = play["about"]["halfInning"]
            except:innHalf = "--"
            if innHalf == "bottom":
                inning = f"Bot {innNum}"
            else: inning = f"Top {innNum}"

            try:
                batter = play["matchup"]["batter"]
                batterName = batter["fullName"]
                batterID = batter["id"]
            except:
                batterName = "--"
                batterID = "--"
            try:
                pitcher = play["matchup"]["pitcher"]
                pitcherName = pitcher["fullName"]
                pitcherID = pitcher["id"]
            except:
                pitcherName = "--"
                pitcherID = "--"
            try:pitchNum = lastEvent["pitchNumber"]
            except:pitchNum = "--"
            try:
                event = play["result"]["event"]
                event_type = play["result"]["eventType"]
            except:
                event = "--"
                event_type = "--"
            try:details = play["result"]["description"]
            except:details = "--"
            try:
                pitchType = lastEvent["details"]["type"]["description"]
                pitchCode = lastEvent["details"]["type"]["code"]
            except:
                pitchType = "--"
                pitchCode = "--"
            try:releaseSpeed = pitchData["startSpeed"]
            except:releaseSpeed = "--"
            try:endSpeed = pitchData["endSpeed"]
            except:endSpeed = "--"
            try:spinRate = pitchData["breaks"]["spinRate"]
            except:spinRate = "--"
            try:zone = pitchData["zone"]
            except:zone = "--"


            # Hit Data (and Trajectory - if ball is in play)
            try:hitData = lastEvent["hitData"]
            except:pass
            try:launchSpeed = hitData["launchSpeed"]
            except:launchSpeed = "--"
            try: launchAngle = hitData["launchAngle"]
            except:launchAngle = "--"
            try: distance = hitData["totalDistance"]
            except:distance = "--"
            try:hitLocation = hitData["location"]
            except:hitLocation = "--"
            try:
                hX = hitData["coordinates"]["coordX"]
                hY = hitData["coordinates"]["coordY"]
            except:
                hX = "--"
                hY = "--"

            try:
                hitTrajectory = hitData["trajectory"]
            except:
                hitTrajectory = "--"

            # Event Category/Type
            try:category = lastEvent["type"]
            except:category = "--"

            # Time information
            try:
                startTime = firstEvent["startTime"]
                startTime_obj = dt.datetime.strptime(startTime,iso_format_ms).replace(tzinfo=utc_zone)
                startTime = dt.datetime.strftime(startTime_obj,iso_format_ms)
                
            except:
                startTime = "--"
            try:
                endTime = lastEvent["endTime"]
                endTime_obj = dt.datetime.strptime(endTime,iso_format_ms).replace(tzinfo=utc_zone)
                endTime = dt.datetime.strftime(endTime_obj,iso_format_ms)
                # endTime = dt.datetime.strptime(play["playEvents"][-1]["endTime"],iso_format_ms).replace(tzinfo=utc_zone)
                
                elasped = endTime_obj - startTime_obj
                

            except Exception as e:
                endTime = "--"
                elasped = "--"
                print(f"ERROR: -- {e} --")

            # Is Home Team Batting?
            if f"ID{batterID}" in self.__home_player_data.keys():
                homeBatting = True
                batTeamID = self.home_id
                battingTeam = self.__home_team
            else:
                homeBatting = False
                batTeamID = self.away_id
                battingTeam = self.__away_team

            event_data = [
                batTeamID,
                battingTeam,
                ab_num,
                inning,
                batterName,
                bat_side,
                pitcherName,
                pitchNum,
                event,
                event_type,
                details,
                pitchType,
                pitchCode,
                releaseSpeed,
                endSpeed,
                spinRate,
                zone,
                launchSpeed,
                launchAngle,
                distance,
                hitLocation,
                hitTrajectory,
                hX,
                hY,
                category,
                prettify_time(elasped),
                prettify_time(startTime),
                prettify_time(endTime),
                batterID,
                pitcherID,
                homeBatting,
                play_id]

            events_data.append(event_data)

        df = pd.DataFrame(data=events_data,columns=headers).sort_values(by=["pa"],ascending=False)

        return df

    def event_log(self) -> pd.DataFrame:
        """Get detailed log of every pitch event

        NOTE: Dataframe begins with most recent pitch event
        """
        headers = [
            "bat_teamID",
            "bat_team",
            "pa",
            "event",
            "event_type",
            "inning",
            "pitch_idx",
            "batter",
            "batter_mlbam",
            "bat_side",
            "batter_zoneTop",
            "batter_zoneBottom",
            "pitcher",
            "pitcher_mlbam",
            "details",
            "pitch_num",
            "pitch_type",
            "pitch_code",
            "call",
            "count",
            "outs",
            "zoneTop",
            "zoneBottom",
            "release_velocity",
            "end_velocity",
            "spin_rate",
            "zone",
            'pX',
            'pZ',
            "exit_velocity",
            "launch_angle",
            "distance",
            "location",
            "hX",
            "hY",
            "category",
            "endTime",
            "isHome",
            "play_id"]

        events_data = []
        for play in self.__all_plays:
            try:ab_num = play["about"]["atBatIndex"]+1
            except:ab_num = "--"
            try:batter = play["matchup"]["batter"]
            except:batter = "--"
            try:pitcher = play["matchup"]["pitcher"]
            except:pitcher = "--"
            try:
                batter_zoneTop = self.__players[f'ID{batter["id"]}']["strikeZoneTop"]
                batter_zoneBottom = self.__players[f'ID{batter["id"]}']["strikeZoneBottom"]
            except:
                batter_zoneTop = "-"
                batter_zoneBottom = "-"
            try:bat_side = play["matchup"]["batSide"]["code"]
            except:bat_side = "-"

            last_idx = play["playEvents"][-1]["index"]

            for event_idx,event in enumerate(play["playEvents"]):
                play_id = event.get("playId")
                try:
                    if event_idx != last_idx:
                        desc = "--"
                    else:
                        desc = play["result"]["description"]
                except:
                    desc = "--"
                
                try:
                    event_label = play["result"]["event"]
                    event_type = play["result"]["eventType"]
                except:
                    event_label = "--"
                    event_type = "--"

                try:pitchNumber = event["pitchNumber"]
                except:pitchNumber = 0

               # Times
                try:
                    startTime = dt.datetime.strptime(event["startTime"],iso_format_ms).replace(tzinfo=utc_zone)
                    # startTimeStr = startTime.strftime(r"%H:%M:%S (%Y-%m-%d)")
                except:startTime = "--"
                try:
                    endTime = dt.datetime.strptime(event["endTime"],iso_format_ms).replace(tzinfo=utc_zone)
                    # endTimeStr = endTime.strftime(r"%H:%M:%S (%Y-%m-%d)")
                except:
                    endTime = "--"
                    # endTimeStr = "--"
                try:
                    elapsed = endTime - startTime
                    elapsed = prettify_time(elapsed)
                except:elapsed = "--"

               # Call and Pitch Type and Count/Outs
                try:pitchType = event["details"]["type"]["description"]
                except:pitchType = "--"
                try:pitchCode = event["details"]["type"]["code"]
                except:pitchCode = "--"
                try:call = event["details"]["description"]
                except:call = "--"
                try:count = f'{event["count"]["balls"]}-{event["count"]["strikes"]}'
                except:count = "--"
                try:outs = event["count"]["outs"]
                except: outs = "--"

               # Pitch Data
                try:pitchData = event["pitchData"]
                except:pass
                try:startSpeed = pitchData["startSpeed"]
                except:startSpeed = "--"
                try:endSpeed = pitchData["endSpeed"]
                except:endSpeed = "--"
                try:spinRate = pitchData["breaks"]["spinRate"]
                except:spinRate = "--"
                try:zone = pitchData["zone"]
                except:zone = "--"
                try:
                    pX = pitchData["coordinates"]["pX"]
                    pZ = pitchData["coordinates"]["pZ"]
                except:
                    pX = "--"
                    pZ = "--"
                try:
                    zoneTop = pitchData["strikeZoneTop"]
                    zoneBottom = pitchData["strikeZoneBottom"]
                except:
                    zoneTop = 3.5
                    zoneBottom = 1.5

               # Hit Data
                if "hitData" in event.keys():
                    try:hitData = event["hitData"]
                    except:pass
                    try:launchSpeed = hitData["launchSpeed"]
                    except:launchSpeed = "--"
                    try: launchAngle = hitData["launchAngle"]
                    except:launchAngle = "--"
                    try: distance = hitData["totalDistance"]
                    except:distance = "--"
                    try:hitLocation = hitData["location"]
                    except:hitLocation = "--"
                    try:
                        hX = hitData["coordinates"]["coordX"]
                        hY = hitData["coordinates"]["coordY"]
                    except:
                        hX = "--"
                        hY = "--"
                else:
                    launchSpeed = "--"
                    launchAngle = "--"
                    distance = "--"
                    hitLocation = "--"
                    hX = "--"
                    hY = "--"

               #type
                try:category = event["type"]
                except:category = "--"

               # Is Home Team Batting?
                if f'ID{batter["id"]}' in self.__home_player_data.keys():
                    homeBatting = True
                    batTeamID = self.home_id
                    battingTeam = self.__home_team
                else:
                    homeBatting = False
                    batTeamID = self.away_id
                    battingTeam = self.__away_team

               #
                event_data = [
                    batTeamID,
                    battingTeam,
                    ab_num,
                    event_label,
                    event_type,
                    play["about"]["inning"],
                    event_idx,
                    batter["fullName"],
                    batter["id"],
                    bat_side,
                    batter_zoneTop,
                    batter_zoneBottom,
                    pitcher["fullName"],
                    pitcher["id"],
                    desc,
                    pitchNumber,
                    pitchType,
                    pitchCode,
                    call,
                    count,
                    outs,
                    zoneTop,
                    zoneBottom,
                    startSpeed,
                    endSpeed,
                    spinRate,
                    zone,
                    pX,
                    pZ,
                    launchSpeed,
                    launchAngle,
                    distance,
                    hitLocation,
                    hX,
                    hY,
                    category,
                    endTime,
                    homeBatting,
                    play_id]

                events_data.append(event_data)

        df = pd.DataFrame(data=events_data,columns=headers).sort_values(by=["pa","pitch_idx"],ascending=False)
       #
        return df

    def scoring_event_log(self) -> pd.DataFrame:
        """Get detailed log of every scoring play pitch event

        NOTE: Dataframe begins with most recent pitch event
        """
        headers = [
            "bat_teamID",
            "bat_team",
            "pa",
            "event",
            "event_type",
            "inning",
            "pitch_idx",
            "batter",
            "batter_mlbam",
            "pitcher",
            "pitcher_mlbam",
            "details",
            "pitch_num",
            "pitch_type",
            "pitch_code",
            "call",
            "count",
            "outs",
            "release_velocity",
            "end_velocity",
            "spin_rate",
            "zone",
            'pX',
            'pZ',
            "exit_velocity",
            "launch_angle",
            "distance",
            "location",
            "hX",
            "hY",
            "category",
            "endTime",
            "isHome"]

        events_data = []
        for play in self.__scoring_plays:
            try:ab_num = play["about"]["atBatIndex"]+1
            except:ab_num = "--"
            try:batter = play["matchup"]["batter"]
            except:batter = "--"
            try:pitcher = play["matchup"]["pitcher"]
            except:pitcher = "--"

            last_idx = play["playEvents"][-1]["index"]

            for event_idx,event in enumerate(play["playEvents"]):
                try:
                    if event_idx != last_idx:desc = "--"
                    else:desc = play["result"]["description"]
                except:desc = "--"
                
                try:
                    event_label = play["result"]["event"]
                    event_type = play["result"]["eventType"]
                except:
                    event_label = "--"
                    event_type = "--"
               # Times

                try:pitchNumber = event["pitchNumber"]
                except:pitchNumber = 0
                try:
                    # startTime = dt.datetime.strptime(event["startTime"],iso_format_ms).replace(tzinfo=utc_zone)
                    startTime = dt.datetime.strptime(event["startTime"],iso_format_ms)
                    startTimeStr = startTime.strftime(r"%H:%M:%S (%Y-%m-%d)")
                except:
                    startTime = "--"
                    # startTimeStr = "--"
                try:
                    # endTime = dt.datetime.strptime(event["endTime"],iso_format_ms).replace(tzinfo=utc_zone)
                    endTime = dt.datetime.strptime(event["endTime"],iso_format_ms)
                    # endTimeStr = endTime.strftime(r"%H:%M:%S (%Y-%m-%d)")
                except:
                    endTime = "--"
                    # endTimeStr = "--"
                try:
                    elapsed = endTime - startTime
                    elapsed = prettify_time(elapsed)
                except:elapsed = "--"


               # Call and Pitch Type and Count/Outs
                try:pitchType = event["details"]["type"]["description"]
                except:pitchType = "--"
                try:pitchCode = event["details"]["type"]["code"]
                except:pitchCode = "--"
                try:call = event["details"]["description"]
                except:call = "--"
                try:count = f'{event["count"]["balls"]}-{event["count"]["strikes"]}'
                except:count = "--"
                try:outs = event["count"]["outs"]
                except: outs = "--"

               # Pitch Data
                try:pitchData = event["pitchData"]
                except:pass
                try:startSpeed = pitchData["startSpeed"]
                except:startSpeed = "--"
                try:endSpeed = pitchData["endSpeed"]
                except:endSpeed = "--"
                try:spinRate = pitchData["breaks"]["spinRate"]
                except:spinRate = "--"
                try:zone = pitchData["zone"]
                except:zone = "--"
                try:
                    pX = pitchData["coordinates"]["pX"]
                    pZ = pitchData["coordinates"]["pZ"]
                except:
                    pX = "--"
                    pZ = "--"

               # Hit Data
                if "hitData" in event.keys():
                    try:hitData = event["hitData"]
                    except:pass
                    try:launchSpeed = hitData["launchSpeed"]
                    except:launchSpeed = "--"
                    try: launchAngle = hitData["launchAngle"]
                    except:launchAngle = "--"
                    try: distance = hitData["totalDistance"]
                    except:distance = "--"
                    try:hitLocation = hitData["location"]
                    except:hitLocation = "--"
                    try:
                        hX = hitData["coordinates"]["coordX"]
                        hY = hitData["coordinates"]["coordY"]
                    except:
                        hX = "--"
                        hY = "--"
                else:
                    launchSpeed = "--"
                    launchAngle = "--"
                    distance = "--"
                    hitLocation = "--"
                    hX = "--"
                    hY = "--"

               #type
                try:category = event["type"]
                except:category = "--"

               # Is Home Team Batting?
                if f'ID{batter["id"]}' in self.__home_player_data.keys():
                    homeBatting = True
                    batTeamID = self.home_id
                    battingTeam = self.__home_team
                else:
                    homeBatting = False
                    batTeamID = self.away_id
                    battingTeam = self.__away_team

               #
                event_data = [
                    batTeamID,
                    battingTeam,
                    ab_num,
                    event_label,
                    event_type,
                    play["about"]["inning"],
                    event_idx,
                    batter["fullName"],
                    batter["id"],
                    pitcher["fullName"],
                    pitcher["id"],
                    desc,
                    pitchNumber,
                    pitchType,
                    pitchCode,
                    call,
                    count,
                    outs,
                    startSpeed,
                    endSpeed,
                    spinRate,
                    zone,
                    pX,
                    pZ,
                    launchSpeed,
                    launchAngle,
                    distance,
                    hitLocation,
                    hX,
                    hY,
                    category,
                    endTime,
                    homeBatting]

                events_data.append(event_data)

        df = pd.DataFrame(data=events_data,columns=headers).sort_values(by=["pa","pitch_idx"],ascending=False)
       #
        return df

    # TEAMS' INDIVIDUAL BATTER STATS (should also be used to display as batting order tables)
    def away_batting_stats(self) -> pd.DataFrame:
        """
        Get current game batting stats for players on AWAY team

        Returns:
        ----------
            `pandas.Dataframe`


        Example:
        ----------
        >>> stats = away_batting_stats()
        >>> print(stats["Player","Pos","AB","R","H"])
                       Player Pos  AB  R   H
        0       Leody Taveras  CF   5  0   1
        1  Isiah Kiner-Falefa  SS   4  2   3
        2       Adolis Garcia  RF   4  1   1
        3      Nathaniel Lowe  1B   4  1   0
        4           DJ Peters  DH   5  2   4
        5          Jonah Heim   C   4  0   0
        6          Nick Solak  2B   4  0   0
        7        Jason Martin  LF   4  0   0
        8     Yonny Hernandez  3B   4  1   2
        9             Summary      38  7  11

        
        """
        # these stats will be only for this CURRENT GAME (with the exception of a player's batting average stat)
        if self.gameState == "Live" or self.gameState == "Final":
            tm = self.__away_stats["batting"]
            players = self.__away_player_data
            # headers = ["Player","Pos","AB","R","H","RBI","SO","BB","AVG","HR","2B","3B","FO","GO","IBB","SacBunts","SacFlies","GIDP","batting","substitute","bbrefID","mlbam"]
            headers = ["Player","Pos","AB","R","H","RBI","SO","BB","AVG","HR","2B","3B","FO","GO","IBB","SacBunts","SacFlies","GIDP","batting","substitute","mlbam"]
            rows = []
            for playerid in self.__away_lineup:
                player = players[f"ID{playerid}"]
                name = player["person"]["fullName"]
                position = player["position"]["abbreviation"]
                stats = player["stats"]["batting"]
                if len(stats) == 0:pass
                else:
                    at_bats = stats["atBats"]
                    # pas = stats["plateAppearances"]
                    runs = stats["runs"]
                    hits = stats["hits"]
                    dbls = stats["doubles"]
                    trpls = stats["triples"]
                    hrs = stats["homeRuns"]
                    rbis = stats["rbi"]
                    sos = stats["strikeOuts"]
                    bbs = stats["baseOnBalls"]
                    flyouts = stats["flyOuts"]
                    groundouts = stats["groundOuts"]
                    ibbs = stats["intentionalWalks"]
                    sacbunts = stats["sacBunts"]
                    sacflies = stats["sacFlies"]
                    gidp = stats["groundIntoDoublePlay"]
                    avg = player["seasonStats"]["batting"]["avg"]
                    isCurrentBatter = player["gameStatus"]["isCurrentBatter"]
                    isSubstitute = player["gameStatus"]["isSubstitute"]
                    # try:
                    #     bbrefID = self.__people[self.__people["mlbam"]==playerid].bbrefID.item()
                    # except:
                    #     search_url = f"https://www.baseball-reference.com/redirect.fcgi?player=1&mlb_ID={playerid}"
                    #     req = requests.get(search_url)
                    #     resp = req.url
                    #     bbrefID = resp[resp.rfind("/")+1:resp.rfind(".")]

                    # row_data = [name,position,at_bats,runs,hits,rbis,sos,bbs,avg,hrs,dbls,trpls,flyouts,groundouts,ibbs,sacbunts,sacflies,gidp,isCurrentBatter,isSubstitute,bbrefID,playerid]
                    row_data = [name,position,at_bats,runs,hits,rbis,sos,bbs,avg,hrs,dbls,trpls,flyouts,groundouts,ibbs,sacbunts,sacflies,gidp,isCurrentBatter,isSubstitute,playerid]
                    rows.append(row_data)
            summary = [
                "Summary",
                " ",
                tm["atBats"],
                tm["runs"],
                tm["hits"],
                tm["rbi"],
                tm["strikeOuts"],
                tm["baseOnBalls"],
                tm["avg"],
                tm["homeRuns"],
                tm["doubles"],
                tm["triples"],
                tm["flyOuts"],
                tm["groundOuts"],
                tm["intentionalWalks"],
                tm["sacBunts"],
                tm["sacFlies"],
                tm["groundIntoDoublePlay"],
                " ",
                " ",
                " "
                ]
            rows.append(summary)
            df = pd.DataFrame(data=rows,columns=headers)
            return df
        else:
            print('error. check API for game\'s state')
            return pd.DataFrame()

    def home_batting_stats(self) -> pd.DataFrame:
        """
        Get current game batting stats for players on HOME team

        * Note: Last entry in dataframe is "Summary"

        Returns:
        ----------
            `pandas.Dataframe`

        Example:
        ----------
        >>> stats = home_batting_stats()
        >>> print(stats["Player","Pos","AB","R","H"])
                     Player Pos  AB  R  H
        0      Tim Anderson  SS   4  0  2
        1       Luis Robert  CF   4  0  0
        2        Jose Abreu  1B   4  0  0
        3   Yasmani Grandal   C   4  1  1
        4      Eloy Jimenez  LF   3  0  0
        5    Billy Hamilton  LF   0  1  0
        6      Yoan Moncada  3B   4  2  3
        7      Gavin Sheets  DH   4  1  1
        8        Adam Engel  RF   2  0  0
        9      Leury Garcia  2B   3  0  1
        10          Summary      32  5  8

        >>>
        """
        # these stats will be only for this CURRENT GAME (with the exception of a player's batting average stat)
        if self.gameState == "Live" or self.gameState == "Final":
            tm = self.__home_stats["batting"]
            players = self.__home_player_data
            headers = ["Player","Pos","AB","R","H","RBI","SO","BB","AVG","HR","2B","3B","FO","GO","IBB","SacBunts","SacFlies","GIDP","batting","substitute","mlbam"]
            rows = []
            for playerid in self.__home_lineup:
                player = players[f"ID{playerid}"]
                name = player["person"]["fullName"]
                position = player["position"]["abbreviation"]
                stats = player["stats"]["batting"]
                if len(stats) == 0:pass
                else:
                    at_bats = stats["atBats"]
                    # pas = stats["plateAppearances"]
                    runs = stats["runs"]
                    hits = stats["hits"]
                    dbls = stats["doubles"]
                    trpls = stats["triples"]
                    hrs = stats["homeRuns"]
                    rbis = stats["rbi"]
                    sos = stats["strikeOuts"]
                    bbs = stats["baseOnBalls"]
                    flyouts = stats["flyOuts"]
                    groundouts = stats["groundOuts"]
                    ibbs = stats["intentionalWalks"]
                    sacbunts = stats["sacBunts"]
                    sacflies = stats["sacFlies"]
                    gidp = stats["groundIntoDoublePlay"]
                    avg = player["seasonStats"]["batting"]["avg"]
                    isCurrentBatter = player["gameStatus"]["isCurrentBatter"]
                    isSubstitute = player["gameStatus"]["isSubstitute"]
                    # try:bbrefID = self.__people[self.__people["mlbam"]==playerid].bbrefID.item()
                    # except:
                    #     search_url = f"https://www.baseball-reference.com/redirect.fcgi?player=1&mlb_ID={playerid}"
                    #     req = requests.get(search_url)
                    #     resp = req.url
                    #     bbrefID = resp[resp.rfind("/")+1:resp.rfind(".")]

                    row_data = [name,position,at_bats,runs,hits,rbis,sos,bbs,avg,hrs,dbls,trpls,flyouts,groundouts,ibbs,sacbunts,sacflies,gidp,isCurrentBatter,isSubstitute,playerid]
                    rows.append(row_data)
            summary = [
                "Summary",
                " ",
                tm["atBats"],
                tm["runs"],
                tm["hits"],
                tm["rbi"],
                tm["strikeOuts"],
                tm["baseOnBalls"],
                tm["avg"],
                tm["homeRuns"],
                tm["doubles"],
                tm["triples"],
                tm["flyOuts"],
                tm["groundOuts"],
                tm["intentionalWalks"],
                tm["sacBunts"],
                tm["sacFlies"],
                tm["groundIntoDoublePlay"],
                " ",
                " ",
                " "
                ]
            rows.append(summary)
            df = pd.DataFrame(data=rows,columns=headers)
            return df
        else:
            print('error. check API for game\'s state')
            return pd.DataFrame()

    # TEAMS' INDIVIDUAL PITCHER STATS
    def away_pitching_stats(self) -> pd.DataFrame:
        """
        Get current game pitching stats for players on AWAY team

        * Note: Last entry in dataframe is "Summary"

        Returns:
        ----------
            `pandas.Dataframe`

        Example:
        ----------
        >>> stats = away_pitching_stats()
        >>> print(stats[["Player","IP","R","H","ER","SO"]])
                    Player   IP  R  H  ER  SO
        0     Matt Manning  5.0  0  2   0   7
        1       Jose Urena  1.2  3  3   3   2
        2       Alex Lange  0.1  0  2   0   1
        3  Kyle Funkhouser  1.0  2  1   2   2
        0          Summary  7.3  5  8   5  12

        >>>
        """
        # these stats will be only for this CURRENT GAME (with the exception of a player's ERA stat)
        players = self.__away_player_data
        headers = ["Player","Ct","IP","R","H","ER","SO","BB","K","B",f"ERA ({self.game_date[:4]})","Strike %","HR","2B","3B","PkOffs","Outs","IBB","HBP","SB","WP","BF","mlbam"]
        rows = []
        for playerid in self.__away_pitcher_lineup:
            player = players[f"ID{playerid}"]
            stats = player["stats"]["pitching"]

            name = player["person"]["fullName"]
            # pos = player["position"]["abbreviation"]
            pitch_ct = stats.get("pitchesThrown","")
            ip = float(stats.get("inningsPitched",0))
            sos = stats.get("strikeOuts",0)
            bbs = stats.get("baseOnBalls",0)
            strikes = stats.get("strikes",0)
            balls = stats.get("balls",0)
            try:strike_perc = float(stats["strikePercentage"])
            except:strike_perc = "--"
            runs = stats["runs"]
            ers = stats["earnedRuns"]
            hits = stats["hits"]
            hrs = stats["homeRuns"]
            dbls = stats["doubles"]
            trpls = stats["triples"]
            pkoffs = stats["pickoffs"]
            outs = stats["outs"]
            ibbs = stats["intentionalWalks"]
            hbp = stats["hitByPitch"]
            sbs = stats["stolenBases"]
            wps = stats["wildPitches"]
            batters_faced = stats["battersFaced"]
            era = player["seasonStats"]["pitching"]["era"]
            # try:bbrefID = self.__people[self.__people["mlbam"]==playerid].bbrefID.item()
            # except: # retrieves player's bbrefID if not in current registry
            #     search_url = f"https://www.baseball-reference.com/redirect.fcgi?player=1&mlb_ID={playerid}"
            #     req = requests.get(search_url)
            #     resp = req.url
            #     bbrefID = resp[resp.rfind("/")+1:resp.rfind(".")]

            row_data = [name,pitch_ct,ip,runs,hits,ers,sos,bbs,strikes,balls,era,strike_perc,hrs,dbls,trpls,pkoffs,outs,ibbs,hbp,sbs,wps,batters_faced,playerid]
            rows.append(row_data)

        df = pd.DataFrame(data=rows,columns=headers)

        sum_of_K_percs = 0
        for num in list(df["Strike %"]):
            try:sum_of_K_percs+=num
            except:pass
        try:rounded_strike_perc = round((sum_of_K_percs/len(df)),3)
        except:rounded_strike_perc = ""
        sum_df = pd.DataFrame(data=[[
            "Summary",
            df["Ct"].sum(),
            df["IP"].sum(),
            df["R"].sum(),
            df["H"].sum(),
            df["ER"].sum(),
            df["SO"].sum(),
            df["BB"].sum(),
            df["K"].sum(),
            df["B"].sum(),
            self.__away_stats["pitching"]["era"],
            rounded_strike_perc,
            df["HR"].sum(),
            df["2B"].sum(),
            df["3B"].sum(),
            df["PkOffs"].sum(),
            df["Outs"].sum(),
            df["IBB"].sum(),
            df["HBP"].sum(),
            df["SB"].sum(),
            df["WP"].sum(),
            df["BF"].sum(),
            " "
        ]],columns=headers)

        return pd.concat([df,sum_df])

    def home_pitching_stats(self) -> pd.DataFrame:
        """
        Get current game pitching stats for players on HOME team

        * Note: Last entry in dataframe is "Summary"

        Returns:
        ----------
            `pandas.Dataframe`

        Example:
        ----------
        >>> stats = home_pitching_stats()
        >>> print(stats[["Player","IP","R","H","ER","SO"]])
                   Player   IP  R  H  ER  SO
        0   Lucas Giolito  5.0  1  2   1   3
        1       Ryan Burr  1.0  0  1   0   1
        2  Dallas Keuchel  0.2  3  4   3   1
        3     Matt Foster  0.1  0  0   0   0
        4    Aaron Bummer  1.0  0  0   0   0
        5   Liam Hendriks  1.0  0  0   0   2
        0         Summary  8.3  4  7   4   7

        >>>
        """
        # these stats will be only for this CURRENT GAME (with the exception of a player's ERA stat)
        players = self.__home_player_data
        headers = ["Player","Ct","IP","R","H","ER","SO","BB","K","B",f"ERA ({self.game_date[:4]})","Strike %","HR","2B","3B","PkOffs","Outs","IBB","HBP","SB","WP","BF","mlbam"]
        rows = []
        for playerid in self.__home_pitcher_lineup:
            player = players[f"ID{playerid}"]
            stats = player["stats"]["pitching"]

            name = player["person"]["fullName"]
            # pos = player["position"]["abbreviation"]
            pitch_ct = stats.get("pitchesThrown","")
            ip = float(stats.get("inningsPitched",0))
            sos = stats.get("strikeOuts",0)
            bbs = stats.get("baseOnBalls",0)
            strikes = stats.get("strikes",0)
            balls = stats.get("balls",0)
            try:strike_perc = float(stats["strikePercentage"])
            except:strike_perc = "--"
            runs = stats["runs"]
            ers = stats["earnedRuns"]
            hits = stats["hits"]
            hrs = stats["homeRuns"]
            dbls = stats["doubles"]
            trpls = stats["triples"]
            pkoffs = stats["pickoffs"]
            outs = stats["outs"]
            ibbs = stats["intentionalWalks"]
            hbp = stats["hitByPitch"]
            sbs = stats["stolenBases"]
            wps = stats["wildPitches"]
            batters_faced = stats["battersFaced"]
            era = player["seasonStats"]["pitching"]["era"]

            row_data = [name,pitch_ct,ip,runs,hits,ers,sos,bbs,strikes,balls,era,strike_perc,hrs,dbls,trpls,pkoffs,outs,ibbs,hbp,sbs,wps,batters_faced,playerid]
            rows.append(row_data)

        df = pd.DataFrame(data=rows,columns=headers)

        sum_of_K_percs = 0
        for num in list(df["Strike %"]):
            try:sum_of_K_percs+=num
            except:pass
        try:rounded_strike_perc = round((sum_of_K_percs/len(df)),3)
        except:rounded_strike_perc = ""
        sum_df = pd.DataFrame(data=[[
            "Summary",
            df["Ct"].sum(),
            df["IP"].sum(),
            df["R"].sum(),
            df["H"].sum(),
            df["ER"].sum(),
            df["SO"].sum(),
            df["BB"].sum(),
            df["K"].sum(),
            df["B"].sum(),
            self.__home_stats["pitching"]["era"],
            rounded_strike_perc,
            df["HR"].sum(),
            df["2B"].sum(),
            df["3B"].sum(),
            df["PkOffs"].sum(),
            df["Outs"].sum(),
            df["IBB"].sum(),
            df["HBP"].sum(),
            df["SB"].sum(),
            df["WP"].sum(),
            df["BF"].sum(),
            " "
        ]],columns=headers)

        return pd.concat([df,sum_df])

    # TEAMS' INDIVIDUAL FIELDER STATS
    def away_fielding_stats(self) -> pd.DataFrame:
        """
        Get current game fielding stats for players on AWAY team

        * Note: Last entry in dataframe is "Summary"

        Returns:
        ----------
            `pandas.Dataframe`

        Example:
        ----------
        >>> stats = away_fielding_stats()
        >>> print(stats[["Player","Pos","Putouts","Errors"]].head())
                       Player Pos  Putouts  Errors
        0         Akil Baddoo  CF        2       0
        1     Robbie Grossman  LF        1       0
        2     Jonathan Schoop  DH        0       0
        3      Miguel Cabrera  1B        6       0
        4   Jeimer Candelario  3B        0       0
        """
        # these stats will be only for this CURRENT GAME (with the exception of a player's fielding average stat)
        tm = self.__away_stats["fielding"]
        players = self.__away_player_data
        headers = ["Player","Pos","Putouts","Assists","Errors","Chances","Stolen Bases","Pickoffs","Passed Balls",f"Fld % ({self.game_date[:4]})","mlbam"]
        rows = []
        for playerid in (self.__away_lineup+self.__away_pitcher_lineup):
            player = players[f"ID{playerid}"]
            stats = player["stats"]["fielding"]

            name = player["person"]["fullName"]
            pos = player["position"]["abbreviation"]
            putouts = stats["putOuts"]
            assists = stats["assists"]
            errors = stats["errors"]
            chances = stats["chances"]
            sbs = stats["stolenBases"]
            pkoffs = stats["pickoffs"]
            passedballs = stats["passedBall"]
            field_perc = player["seasonStats"]["fielding"]["fielding"]

            row_data = [name,pos,putouts,assists,errors,chances,sbs,pkoffs,passedballs,field_perc,playerid]
            rows.append(row_data)
        summary = [
            "Summary",
            " ",
            tm["putOuts"],
            tm["assists"],
            tm["errors"],
            tm["chances"],
            tm["stolenBases"],
            tm["pickoffs"],
            tm["passedBall"],
            " ",
            " "
        ]
        rows.append(summary)
        df = pd.DataFrame(data=rows,columns=headers)
        return df

    def home_fielding_stats(self) -> pd.DataFrame:
        """
        Get current game fielding stats for players on HOME team

        * Note: Last entry in dataframe is "Summary"

        Returns:
        ----------
            `pandas.Dataframe`

        Example:
        ----------
        >>> stats = home_fielding_stats()
        >>> print(stats[["Player","Pos","Putouts","Errors"]].head())
                     Player Pos  Putouts  Errors
        0      Tim Anderson  SS        1       0
        1       Luis Robert  CF        4       0
        2        Jose Abreu  1B       10       0
        3   Yasmani Grandal   C        7       0
        4      Eloy Jimenez  LF        2       0
        """
        # these stats will be only for this CURRENT GAME (with the exception of a player's fielding average stat)
        tm = self.__home_stats["fielding"]
        players = self.__home_player_data
        headers = ["Player","Pos","Putouts","Assists","Errors","Chances","Stolen Bases","Pickoffs","Passed Balls",f"Fld % ({self.game_date[:4]})","mlbam"]
        rows = []
        for playerid in (self.__home_lineup+self.__home_pitcher_lineup):
            player = players[f"ID{playerid}"]
            stats = player["stats"]["fielding"]

            name = player["person"]["fullName"]
            pos = player["position"]["abbreviation"]
            putouts = stats["putOuts"]
            assists = stats["assists"]
            errors = stats["errors"]
            chances = stats["chances"]
            sbs = stats["stolenBases"]
            pkoffs = stats["pickoffs"]
            passedballs = stats["passedBall"]
            field_perc = player["seasonStats"]["fielding"]["fielding"]

            row_data = [name,pos,putouts,assists,errors,chances,sbs,pkoffs,passedballs,field_perc,playerid]
            rows.append(row_data)
        summary = [
            "Summary",
            " ",
            tm["putOuts"],
            tm["assists"],
            tm["errors"],
            tm["chances"],
            tm["stolenBases"],
            tm["pickoffs"],
            tm["passedBall"],
            " ",
            " "
        ]
        rows.append(summary)
        df = pd.DataFrame(data=rows,columns=headers)
        return df

    def timestamps(self) -> pd.DataFrame:
        plays = self.__all_plays
        ts = []
        for p in plays:
            play_type       = p.get("result",{}).get("type")
            playStartTime   = p.get("about",{}).get("startTime")
            playEndTime     = p.get("playEndTime","-")
            playEvents      = p.get("playEvents",[])
            # print(playEvents)
            try:
                abIndex         = p.get("atBatIndex")
                ab_num          = abIndex + 1
            except:
                pass
            for e in playEvents:
                play_id         = e.get("playId")
                eventStart      = e.get("startTime")
                eventEnd        = e.get("endTime")
                event_idx       = e.get("index")
                event_num       = event_idx + 1
                details         = e.get("details",{})
                event_type      = details.get("eventType")
                event_type2      = details.get("event")
                event_desc      = details.get("description")
                if eventStart is None:
                    start_tc = "-"
                else:
                    start_tc        = dt.datetime.strptime(eventStart,r"%Y-%m-%dT%H:%M:%S.%fZ")
                    start_tc        = start_tc.strftime(r"%Y%m%d_%H%M%S")
                if eventEnd is None:
                    end_tc = "-"
                else:
                    end_tc          = dt.datetime.strptime(eventEnd,r"%Y-%m-%dT%H:%M:%S.%fZ")
                    end_tc          = end_tc.strftime(r"%Y%m%d_%H%M%S")

                ts.append([
                    abIndex,
                    play_type,
                    # playStartTime,
                    # playEndTime,
                    event_idx,
                    f"{event_type2} ({event_type})",
                    event_desc,
                    eventStart,
                    start_tc,
                    eventEnd,
                    end_tc,
                    play_id
                ])
        df = pd.DataFrame(data=ts,columns=("ab_idx","type","event_idx","event_type","event_desc","event_start","start_tc","event_end","end_tc","play_id"))

        return df

    def flags(self) -> dict:
        f = self.__flags
        return f

    def get_content(self):
        url = BASE + f"/game/{self.gamePk}/content"
        resp = requests.get(url)
        return resp.json()
    
    def get_feed_data(self,timecode=None):
        if timecode is not None:
            url = f"https://statsapi.mlb.com/api/v1.1/game/{self.gamePk}/feed/live?timecode={timecode}"
        else:
            url = f"https://statsapi.mlb.com/api/v1.1/game/{self.gamePk}/feed/live"
        resp = requests.get(url)
        return resp.json()

    def batter_events(self,playerid):
        """This class method has not been configured yet"""

    def pitcher_events(self,playerid):
        """This class method has not been configured yet"""

    def context_splits(self,batterID,pitcherID):  #  applicable DYNAMIC splits for the current matchup
        """This class method has not been configured yet"""
        pass

    def away_season_stats(self):
        """This method has not been configured yet"""
        pass

    def home_season_stats(self):
        """This method has not been configured yet"""
        pass

    def away_splits(self): # applicable STATIC splits for the AWAY team
        """This class method has not been configured yet"""
        pass

    def home_splits(self): # applicable STATIC splits for the HOME team
        """This class method has not been configured yet"""
        pass
    

class StatsAPI:
    class stats:
        pass

    class people:
        def __new__(cls,mlbam):
            self = object.__new__(cls)
            self.mlbam = mlbam
            return self

        def info(self):
            url = BASE + f"/people/{self.mlbam}"
            resp = requests.get(url)
            return resp.json()

        def stats(self,statType,statGroup,season=None,**kwargs):
            
            if season is not None:
                season = default_season()
            params = {
                "stats":statType,
                "group":statGroup,
                "season":season
            }
            url = BASE + f"/people/{self.mlbam}/stats"
            resp = requests.get(url,params)
            return resp.json()

    class team:
        pass

    class teams:
        pass

    class standings:
        pass
