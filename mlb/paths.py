import os

DATA_DIR = os.path.join(os.path.dirname(__file__),'data/')

BASEBALL_DB = os.path.join('sqlite:///' + os.path.dirname(__file__), 'baseball.db')
PEOPLE_CSV = os.path.join(os.path.dirname(__file__),'data/people.csv')
BIOS_CSV = os.path.join(os.path.dirname(__file__),'data/bios.csv')
TEAMS_CSV = os.path.join(os.path.dirname(__file__),'data/teams.csv')
SEASONS_CSV = os.path.join(os.path.dirname(__file__),'data/seasons.csv')
VENUES_CSV = os.path.join(os.path.dirname(__file__),'data/venues.csv')
YBY_RECORDS_CSV = os.path.join(os.path.dirname(__file__),'data/yby_records.csv')
YBY_STANDINGS_CSV = os.path.join(os.path.dirname(__file__),'data/yby_standings.csv')
HALL_OF_FAME_CSV = os.path.join(os.path.dirname(__file__),'data/hall_of_fame.csv')
BROADCASTS_CSV = os.path.join(os.path.dirname(__file__),'data/broadcasts.csv')
BBREF_BATTING_DATA_CSV = os.path.join(os.path.dirname(__file__),'data/bbref_war_hit.csv')
BBREF_PITCHING_DATA_CSV = os.path.join(os.path.dirname(__file__),'data/bbref_war_pitch.csv')