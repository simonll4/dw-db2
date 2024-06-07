import psycopg2

# Establece la conexión a la base de datos
conn = psycopg2.connect(
    host="localhost",
    database="dwdb2",
    user="user",
    password="123"
)

cur = conn.cursor()

cur.execute("ALTER TABLE tmp.match_fact_table ADD COLUMN rojas_totales INT")
cur.execute(
    "UPDATE tmp.match_fact_table SET rojas_totales = rojas_local + rojas_visitante")

conn.commit()
cur.close()
conn.close()
