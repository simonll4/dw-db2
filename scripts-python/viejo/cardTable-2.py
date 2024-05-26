import pandas as pd
import sqlalchemy
from sqlalchemy import text
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET
import random
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('/home/simonll4/Desktop/db/OLTP-sqlite/database.sqlite')
c = conn.cursor()

# Add a new column 'card_id' to the 'Match' table
c.execute("ALTER TABLE Match ADD COLUMN card_id INTEGER")

# Fill the new column with unique values
c.execute("UPDATE Match SET card_id = rowid")

# Commit the changes and close the connection to the 'Match' table
conn.commit()
conn.close()

# Read the 'card' column from the 'Match' table into a DataFrame
engine = create_engine('sqlite:////home/simonll4/Desktop/db/OLTP-sqlite/database.sqlite')
query = "SELECT card, card_id FROM Match WHERE card is not null"
df = pd.read_sql_query(query, engine)

card_types = ['y','y2','r' ]

def assign_type(card_type):
    if card_type is None:
        return random.choice(card_types)
    else:
        return card_type

data_list = []
for _, row in df.iterrows():
    card = row['card']
    card_id = row['card_id']
    if card is not None:
        root = ET.fromstring(card)
        for event in root.findall('value'):
            data_dict = {}
            data_dict['card_id'] = card_id
            data_dict['elapse'] = event.find('elapsed').text if event.find('elapsed') is not None else None
            data_dict['card_type'] = assign_type(event.find('card_type').text if event.find('card_type') is not None else None)
            data_dict['player1'] = event.find('player1').text if event.find('player1') is not None else None
            data_dict['team'] = event.find('team').text if event.find('team') is not None else None
            data_list.append(data_dict)

# Create a new DataFrame from the list of dictionaries
df_cards = pd.DataFrame(data_list)

with open('output2.txt', 'w') as f:
    f.write(df_cards.to_string())

# Write the new DataFrame to a new SQL table
df_cards.to_sql('cardTable', engine, if_exists='replace', index=False,
                dtype={
                    'card_id': sqlalchemy.types.INTEGER(),
                    'elapse': sqlalchemy.types.INTEGER(),
                    'card_type': sqlalchemy.types.TEXT(),
                    'player1': sqlalchemy.types.INTEGER(),
                    'team': sqlalchemy.types.INTEGER(),
                })

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE cardTable
        ADD FOREIGN KEY (card_id)
        REFERENCES Match (card_id)
    """))