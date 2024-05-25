import pandas as pd
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET

contador = 1

# Read the 'card' and 'id' columns from the 'Match' table into a DataFrame
engine = create_engine('sqlite:////home/simonll4/Desktop/db/OLTP-sqlite/database.sqlite')
query = "SELECT id, shotoff FROM match WHERE shotoff IS NOT null"
df = pd.read_sql_query(query, engine)

data_list = []
for index, row in df.iterrows():
    shotOn = row['shotoff']
    if shotOn is not None:
        root = ET.fromstring(shotOn)
        for event in root.findall('value'):
            data_dict = {}
            data_dict['id'] = contador
            contador = contador + 1
            data_dict['id_match'] = row['id']
            data_dict['team'] = event.find('team').text if event.find('team') is not None else None
            data_list.append(data_dict)


df_shootOn = pd.DataFrame(data_list)
with open('shot_off.txt', 'w') as f:
    f.write(df_shootOn.to_string())

df_shootOn.to_sql('shotOffTable', engine, if_exists='replace')
