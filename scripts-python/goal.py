import pandas as pd
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET
import random

# Create a SQLAlchemy engine
engine = create_engine('sqlite:////home/simonll4/Desktop/db/OLTP-sqlite/database.sqlite')
subtypes = ['header', 'shot', 'distance', 'volley', 'freekick', 'penalty', 'penalty', 'penalty', 'cross', 'clearance', 'lob', 'backheel', 'direct_freekick']

def assign_subtype(subtype):
    if subtype is None:
        return random.choice(subtypes)
    else:
        return subtype

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
            data_dict['subtype'] = assign_subtype(event.find('subtype').text if event.find('subtype') is not None else None)
            data_dict['team'] = event.find('team').text if event.find('team') is not None else None
            data_list.append(data_dict)

# Create a new DataFrame from the list of dictionaries
df_goals = pd.DataFrame(data_list)

with open('goal.txt', 'w') as f:
    f.write(df_goals.to_string())

# Write the new DataFrame to a new SQL table
df_goals.to_sql('goalTable', engine, if_exists='replace')