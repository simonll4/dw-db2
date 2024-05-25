import pandas as pd
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET

contador = 1

# Read the 'possession' and 'id' columns from the 'Match' table into a DataFrame
engine = create_engine('sqlite:////home/simonll4/Desktop/db/OLTP-sqlite/database.sqlite')
query = "SELECT id, possession FROM match WHERE possession IS NOT null"
df = pd.read_sql_query(query, engine)

data_list = []
for index, row in df.iterrows():
  possession = row['possession']
  if possession is not None:
    root = ET.fromstring(possession)
    awaypos_list = []
    homepos_list = []
    for event in root.findall('value'):
      awaypos = event.find('awaypos').text if event.find('awaypos') is not None else None
      homepos = event.find('homepos').text if event.find('homepos') is not None else None
      if awaypos is not None:
        awaypos_list.append(int(awaypos))
      if homepos is not None:
        homepos_list.append(int(homepos))
    data_dict = {}
    data_dict['id'] = contador
    contador = contador + 1
    data_dict['id_match'] = row['id']
    data_dict['awaypos'] = sum(awaypos_list) / len(awaypos_list) if awaypos_list else None
    data_dict['homepos'] = sum(homepos_list) / len(homepos_list) if homepos_list else None
    data_list.append(data_dict)

df_possession = pd.DataFrame(data_list)
with open('possession.txt', 'w') as f:
    f.write(df_possession.to_string())