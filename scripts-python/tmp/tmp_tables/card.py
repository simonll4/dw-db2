import pandas as pd
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET
import random

# Read the 'card' and 'id' columns from the 'Match' table into a DataFrame
engine = create_engine('sqlite:////home/simonll4/Desktop/db/OLTP-sqlite/database.sqlite')
query = "SELECT id, card FROM Match WHERE card is not null"
df = pd.read_sql_query(query, engine)

card_types = ['y','y2','r' ]

def assign_type(card_type):
    if card_type is None:
        return random.choice(card_types)
    else:
        return card_type

data_list = []
for index, row in df.iterrows():
    card = row['card']
    if card is not None:
        root = ET.fromstring(card)
        for event in root.findall('value'):
            data_dict = {}
            data_dict['match_id'] = row['id']  
            data_dict['card_type'] = assign_type(event.find('card_type').text if event.find('card_type') is not None else None)
            data_dict['team'] = event.find('team').text if event.find('team') is not None else None
            data_list.append(data_dict)

# Create a new DataFrame from the list of dictionaries
df_cards = pd.DataFrame(data_list)

with open('card.txt', 'w') as f:
    f.write(df_cards.to_string())

# Write the new DataFrame to a new SQL table
df_cards.to_sql('cardTable', engine, if_exists='replace')