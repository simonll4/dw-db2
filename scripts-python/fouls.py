import pandas as pd
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET

contador = 1

# Read the 'foulcommit' and 'id' columns from the 'Match' table into a DataFrame
engine = create_engine('sqlite:////home/simonll4/Desktop/db/OLTP-sqlite/database.sqlite')
query = "SELECT id, foulcommit FROM match WHERE foulcommit IS NOT null"
df = pd.read_sql_query(query, engine)

data_list = []
for index, row in df.iterrows():
  foulcommit = row['foulcommit']
  if foulcommit is not None:
    root = ET.fromstring(foulcommit)
    for event in root.findall('value'):
      data_dict = {}
      data_dict['id'] = contador
      contador = contador + 1
      data_dict['id_match'] = row['id']
      data_dict['team'] = event.find('team').text if event.find('team') is not None else None
      data_list.append(data_dict)

df_foulcommit = pd.DataFrame(data_list)
with open('foul_commit.txt', 'w') as f:
  f.write(df_foulcommit.to_string())

df_foulcommit.to_sql('foulTable', engine, if_exists='replace')