import psycopg2

# Establece la conexión a la base de datos
conn = psycopg2.connect(
    host="localhost",
    database="dwdb2",
    user="user",
    password="123"
)

cur = conn.cursor()

cur.execute("ALTER TABLE tmp.match_fact_table ADD COLUMN amarillas_totales INT")
cur.execute(
    "UPDATE tmp.match_fact_table SET amarillas_totales = amarillas_local + amarillas_visitante")

conn.commit()
cur.close()
conn.close()
