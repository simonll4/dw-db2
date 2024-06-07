import pandas as pd
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET
import random

# Read the 'goal' and 'id' columns from the 'Match' table into a DataFrame
query1 = "SELECT id, goal FROM Match WHERE goal is not null"
df1 = pd.read_sql_query(query1, engine)

data_list = []
for index, row in df1.iterrows():
    goal = row['goal']
    if goal is not None:
        root = ET.fromstring(goal)
        for event in root.findall('value'):
            data_dict = {}
            data_dict['match_id'] = row['id']  # Add 'id' to the data dictionary
            data_dict['team'] = event.find('team').text if event.find('team') is not None else None
            data_list.append(data_dict)

df_goals = pd.DataFrame(data_list)

with open('goal.txt', 'w') as f:
    f.write(df_goals.to_string())

df_goals.to_sql('goalTable', engine, if_exists='replace')