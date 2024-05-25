import pandas as pd
from sqlalchemy import create_engine
import random

# Create a connection to the database
engine = create_engine(
    'sqlite:////home/gabriel/Descargas/fifa_db_oltp.sqlite')

# Read the 'id' column from the 'Match' table into a DataFrame
query = "SELECT id FROM Match"
df = pd.read_sql_query(query, engine)

# List to store the possession data
possession_data = []

# Iterate over the DataFrame and generate random possession values for each match
for index, row in df.iterrows():
    # Multiply by 100 to get percentage, round, and convert to int
    home_pos = int(round(random.uniform(0.32, 0.68) * 100))
    # Divide by 100 before subtracting to keep the sum as 1, round, and convert to int
    away_pos = int(round((1 - home_pos / 100) * 100))
    possession_data.append(
        {'match_id': row['id'], 'home_pos': home_pos, 'away_pos': away_pos})

# Create a new DataFrame from the possession data
df_possession = pd.DataFrame(possession_data)

df_possession.index = df_possession.index + 1
# Write the new DataFrame to a new SQL table
df_possession.to_sql('possessionTable', engine, if_exists='replace')
