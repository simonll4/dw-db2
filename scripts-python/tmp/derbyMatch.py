import sqlite3
import pandas as pd

conn = sqlite3.connect('/home/simonll4/Desktop/db/OLTP/oltp.sqlite')
c = conn.cursor()

important_matches = [
    (8635, 8342),
    (8635, 9985),
    (10260, 8456),
    (10260, 8650),
    (9825, 8586),
    (9847, 8592),
    (9748, 9853),
    (9789, 9823),
    (9789, 10189),
    (8564, 8636),
    (8636, 9885),
    (8686, 8543),
    (8593, 10235),
    (8593, 8640),
    (8673, 2182),
    (10265, 2186),
    (9772, 9768),
    (9772, 9773),
    (9773, 9768),
    (9925, 8548),
    (8633, 8634),
    (9906, 8633),
    (8302, 8603),
    (9931, 10243),
    (10243, 10192)
]

for team_id_1, team_id_2 in important_matches:
    c.execute("""
        UPDATE Match 
        SET important = 1 
        WHERE (home_team_api_id = ? AND away_team_api_id = ?) 
        OR (home_team_api_id = ? AND away_team_api_id = ?)
    """, (team_id_1, team_id_2, team_id_2, team_id_1))

conn.commit()
conn.close()