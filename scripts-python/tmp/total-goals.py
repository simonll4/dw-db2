import psycopg2

# Establece la conexión a la base de datos
conn = psycopg2.connect(
    host="localhost",
    database="dwdb2",
    user="user",
    password="123"
)

cur = conn.cursor()
cur.execute("ALTER TABLE tmp.match_fact_table ADD COLUMN goles_totales INT")

# Actualiza la columna 'goles_totales' con la suma de 'goles_local' y 'goles_visitante'
cur.execute(
    "UPDATE tmp.match_fact_table SET goles_totales = goles_local + goles_visitante")

conn.commit()
cur.close()
conn.close()
