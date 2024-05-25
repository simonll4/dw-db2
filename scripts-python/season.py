import pandas as pd
from sqlalchemy import create_engine

# Create a connection to the SQLite database
engine = create_engine(
    'sqlite:////home/gabriel/Descargas/fifa_db_oltp.sqlite')

# Read the 'season' column from the 'Match' table into a DataFrame
query = "SELECT season FROM Match"
df = pd.read_sql_query(query, engine)

# Get all unique seasons
seasons = df['season'].unique()

data_list = []
for i, season in enumerate(seasons):
    # Split the season string into start year and end year
    start_year, end_year = season.split('/')
    data_dict = {}
    data_dict['id'] = i + 1
    data_dict['name'] = season
    data_dict['inicio'] = start_year
    data_dict['fin'] = end_year
    data_list.append(data_dict)

# Create a new DataFrame from the list of dictionaries
df_seasons = pd.DataFrame(data_list)

# Write the new DataFrame to a new SQL table
df_seasons.to_sql('seasonTable', engine, if_exists='replace')
