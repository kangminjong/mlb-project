from pybaseball import statcast, pitching_stats_range, batting_stats_range, team_batting, team_pitching, team_fielding, batting_stats, pitching_stats # api 호출
data = batting_stats_range(start_dt=str('2025-04-01'), end_dt=str('2025-04-01'))

print(data)
