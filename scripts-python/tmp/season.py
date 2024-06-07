import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    'sqlite:////home/gabriel/Descargas/fifa_db_oltp.sqlite')

query = "SELECT season FROM Match"
df = pd.read_sql_query(query, engine)

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

df_seasons = pd.DataFrame(data_list)
df_seasons.to_sql('seasonTable', engine, if_exists='replace')
