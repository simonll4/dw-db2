import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://user:123@localhost:5432/dwdb2')
query = "SELECT * FROM dw.ft_matches"
df = pd.read_sql_query(query, engine)

# Calcular el ganador basado en los puntos de los equipos local y visitante
df['ganador'] = df.apply(lambda x: 'local' if x['puntos_local'] > x['puntos_visitante'] else ('visitante' if x['puntos_visitante'] > x['puntos_local'] else 'empate'), axis=1)
# Determinar invictos
df['invicto_local'] = df.apply(lambda x: 1 if x['ganador'] == 'local' or x['ganador'] == 'empate' else 0, axis=1)
df['invicto_visitante'] = df.apply(lambda x: 1 if x['ganador'] == 'visitante' or x['ganador'] == 'empate' else 0, axis=1)
# Preparar datos para calcular rachas
df_invictos_local = df[['id_teamhome', 'id_date', 'invicto_local']].rename(columns={'id_teamhome': 'equipo', 'invicto_local': 'invicto'})
df_invictos_visitante = df[['id_teamaway', 'id_date', 'invicto_visitante']].rename(columns={'id_teamaway': 'equipo', 'invicto_visitante': 'invicto'})
df_invictos = pd.concat([df_invictos_local, df_invictos_visitante]).sort_values(by=['equipo', 'id_date'])
# Calcular las rachas de invictos
df_invictos['racha_id'] = (df_invictos['invicto'] != df_invictos['invicto'].shift(1)) | (df_invictos['equipo'] != df_invictos['equipo'].shift(1))
df_invictos['racha_id'] = df_invictos['racha_id'].cumsum()
# Filtrar solo las rachas invictas
df_rachas_invictas = df_invictos[df_invictos['invicto'] == 1].copy()
# Calcular la longitud de cada racha
df_rachas_invictas.loc[:, 'racha_length'] = df_rachas_invictas.groupby(['equipo', 'racha_id']).cumcount() + 1
# Encontrar la racha más larga de invictos y la racha actual
racha_mas_larga = df_rachas_invictas.groupby('equipo')['racha_length'].max().reset_index(name='racha_mas_larga')
# Calcular racha actual
df_invictos['fecha'] = pd.to_datetime(df_invictos['id_date'], format='%Y%m%d')
racha_actual = df_invictos[df_invictos['fecha'] == df_invictos.groupby('equipo')['fecha'].transform('max')]
racha_actual = racha_actual[['equipo', 'invicto']].reset_index(drop=True)
# Unir con la racha más larga
df_resultados = pd.merge(racha_mas_larga, racha_actual, on='equipo')
df_resultados.rename(columns={'invicto': 'racha_actual'}, inplace=True)
df_resultados['racha_actual'] = df_resultados.apply(lambda x: x['racha_actual'] if x['racha_actual'] == 1 else 0, axis=1)
# Consulta para obtener los nombres de los equipos
query_equipos = "SELECT idteam, name FROM dw.d_teams"
equipos = pd.read_sql_query(query_equipos, engine)
# Unir df_resultados con el DataFrame de equipos para agregar el nombre del equipo
df_resultados_con_nombres = pd.merge(df_resultados, equipos, left_on='equipo', right_on='idteam')
df_resultados_con_nombres.rename(columns={'name': 'nombre_equipo'}, inplace=True)
df_resultados_con_nombres = df_resultados_con_nombres[['equipo', 'nombre_equipo', 'racha_mas_larga', 'racha_actual']]

df_resultados_con_nombres.to_csv('equipos_invictos.csv', index=False)
