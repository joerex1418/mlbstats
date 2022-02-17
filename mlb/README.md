# SimpleStats
A Python Wrapper that allows users to interface with the Official MLB Stats API
## Copyright Notice
This repository and its author are not affiliated with the MLB in any way. Use of MLB data is subject to the notice posted at http://gdx.mlb.com/components/copyright.txt.

---------
## QUICKSTART
```python
import simplestats as ss
```
<br>

## Player
```python
>>> player = ss.Player(547989)
>>> print(player.fullName)

'José Dariel Abreu'
```

## Team
```python
>>> team = ss.Team(145,season=2021)
>>> df = team.player_hitting_reg()
>>> print(df.head()[["playerName","G","AB","H","R","HR","AVG"]])

     playerName    G   AB    H   R  HR   AVG
0    Jose Abreu  152  566  148  86  30  .261
1  Tim Anderson  123  527  163  94  17  .309
2  Aaron Bummer    5    0    0   0   0  .000
3   Jake Burger   15   38   10   5   1  .263
4     Ryan Burr    3    0    0   0   0  .000
```

## Franchise
```python
>>> franch = ss.Franchise(145)
>>> df = franch.ybyStats("fielding")
>>> print(df)

     Season    G   GS     INNs    PO     A    E    Ch thE   RF/G  RF/9   DP  TP  FLD%
120    2021  162  162  12630.0  4210  1210   97  5517  47  33.46  3.86  112   0  .982
119    2020   60   60   4743.0  1581   517   39  2137  14  34.97  3.98   48   0  .982
118    2019  161  161  12714.0  4238  1525  117  5880  50  35.80  4.08  170   1  .980
117    2018  162  162  12933.0  4311  1431  114  5856  46  35.44  4.00  134   0  .981
116    2017  162  162  12795.0  4265  1541  114  5920  35  35.84  4.08  157   0  .981
```

## League
```python
>>> league = ss.League(season=2005)
```
