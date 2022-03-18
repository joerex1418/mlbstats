# Intro
This is an UNOFFICIAL Python wrapper for the Official MLB StatsAPI (https://statsapi.mlb.com). I wrote this library with the intention of making baseball data retrieval as intuitive and robust as possible.
## Copyright Notice
This repository and its author are not affiliated with the MLB in any way. Use of MLB data is subject to the notice posted at http://gdx.mlb.com/components/copyright.txt.

## Getting the library
I haven't had the chance to make the library available on the Python Package Index just yet. So for now, you can just download from the GitHub Repo. 

In the command line, copy and paste the following:
```
pip install git+https://github.com/joerex1418/simplestats-mlb.git
```

<!-- ## Installing from PyPI
In the command line, copy and paste the following:
```
pip install simplestats-mlb
``` -->

## Issues
https://github.com/joerex1418/simplestats-mlb/issues



# Quickstart
The class and function names should speak for themselves but if you need a good starting point, read on.
```python
>>> import mlb

# Creating an instance of the Person class (Jose Abreu)
>>> abreu = mlb.Person(547989)

# Get the 2005 schedule for the Chicago White Sox (additional arguments)
>>> mlb.schedule(mlbam=145,season=2005)

# Get hitting stats for the Chicago White Sox in their 1992 season
>>> mlb.team_hitting(mlbam=145,season=1992)
```
NOTE:
  Use `mlb.people()` to look up a person's official ID.
  Alternatively, you could use the api interface class and search for players directly from the API using `mlb.api.player_search()`

<br>
Now that we have our Person instance, we can start accessing the data!

<br>

# Classes
There are 4 primary classes in this library that have been configured to make data retrieval as painless as possible (at least, in my personal opinion)
* `Person`
  * Get data for a player (person)
* `Franchise`
  * Get data for team's parent franchise
* `Team`
  * Get team data for a specific season
* `Game`
  * Get live game data

The `Franchise` class is more oriented towards a franchise/team's "year-by-year" data. Where as the `Team` class focuses on a single year and can provide stats and other information for the player's on a team's roster.

<hr>

## Person
### Stats
```python
# Creating an instance of the Person class (Jose Abreu)
>>> abreu = mlb.Person(547989)

# Get year-by-year hitting stats for Jose Abreu
>>> abreu.stats.hitting.yby.regular

  season game_type  tm_mlbam            tm_name  lg_mlbam          lg_name    G   AB  ...  GIDP     P   TB  LOB  CI  AB/HR  sB  sF
8   2021         R       145  Chicago White Sox       103  American League  152  566  ...    28  2606  272  249   0  18.87   0  10
9   2021         P       145  Chicago White Sox       103  American League    4   14  ...     1    79    5    9   0   -.--   0   0
6   2020         R       145  Chicago White Sox       103  American League   60  240  ...    10  1027  148  106   0  12.63   0   1
7   2020         P       145  Chicago White Sox       103  American League    3   14  ...     1    62    8   11   0  14.00   0   0
5   2019         R       145  Chicago White Sox       103  American League  159  634  ...    24  2735  319  275   0  19.21   0  10
4   2018         R       145  Chicago White Sox       103  American League  128  499  ...    14  2108  236  174   0  22.68   0   6
3   2017         R       145  Chicago White Sox       103  American League  156  621  ...    21  2560  343  222   0  18.82   0   4
2   2016         R       145  Chicago White Sox       103  American League  159  624  ...    21  2695  292  276   0  24.96   0   9
1   2015         R       145  Chicago White Sox       103  American League  154  613  ...    16  2526  308  218   0  20.43   0   1
0   2014         R       145  Chicago White Sox       103  American League  145  556  ...    14  2351  323  203   0  15.44   0   4

```
Obviously, we all have different coding styles. For this reason, I made the data wrappers for the Person class subscriptable like a python dictionary - similar to how Pandas library allows users to access column data through dot notation and "key-value" behavior. In addition to this, if you prefer seeing the available parameters/attributes in your IDE, then you can use the `stats` attribute as a funciton.
```python
# METHOD 1
# Dot notation
>>> abreu.stats.hitting.yby.regular

# METHOD 2
# Key-value pair
>>> abreu.stats["hitting"]["yby"]["regular"]

# METHOD 2.5
# Combination of Dot-notation & key-value pair methods
>>> abreu.stats.hitting["yby"]["regular"]

# METHOD 3
# Function call
>>> abreu.stats('hitting','yby')
```
All three methods get you the same result.

### Names & Birth attributes
The Person class holds more than just stats data. It retrieves any piece of information that might be available on the Stats API. With the `name` class attribute, you can access the different names a person might go by (even their nickname!)
```python
>>> abreu.name.full
'Jose Abreu'
>>> abreu.name.middle
'Dariel'
>>> abreu.name.pronunciation
'uh-BRAY-you'
```

You can also access birth (or death if applicable) information such as date 
```python
>>> abreu.birth.date
'1987-01-29'
>>> abreu.birth.city
'Cienfuegos'
>>> abreu.birth.country
'Cuba'
```
Date property can also be returned in a variety of pre-defined types for convenience; including `datetime.date`

```python
>>> abreu.birth.date.full
'January 29, 1987'
>>> abreu.birth.date.short
'Jan 29, 1987'
>>> abreu.birth.date.obj
datetime.date(1987, 1, 29)
>>> abreu.birth.date()
datetime.date(1987, 1, 29)
```

# Data
Many of the functions utilize data gathered from Stats API. Additionally, the Chadwick Database and Baseball-Reference.com websites are used as supplements to the library data. There are useful CSV files representing this data that can be accessed at anytime by calling one of the functions below.

* `people()`
  * Get a dataframe of people and their different data IDs
* `bios()`
  * Get a dataframe of quick facts of people
* `teams()`
  * Get a dataframe of teams/franchises' information for each season
* `leagues()`
  * Get a dataframe of leagues' information for each season
* `venues()`
  * Get a dataframe of information for every ballpark
* `seasons()`
  * Get a dataframe of information detailing different start & end dates for each season

Alternatively, this data can be retrieved by the user by using the API functions

** NOT CONFIGURED YET **
