import psycopg2
import pandas as pd

# Establece la conexión a la base de datos
conn = psycopg2.connect(
    host="localhost",
    database="dwdb2",
    user="user",
    password="123"
)

cur = conn.cursor()
cur.execute("ALTER TABLE dsa.d_time ADD COLUMN fechaFormateada TIMESTAMP")

conn.commit()

cur.execute("SELECT * FROM dsa.d_time")
rows = cur.fetchall()

# Convierte los datos a un DataFrame de pandas
df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

# Convierte la columna 'idfecha' al l formato deseado
df['fechaFormateada'] = pd.to_datetime(df['idfecha'].astype(int).astype(
    str), format='%Y%m%d').dt.strftime('%Y-%m-%d %H:%M:%S')

# Actualiza los datos en la base de datos
for index, row in df.iterrows():
    cur.execute(
        f"UPDATE dsa.d_time SET fechaFormateada = '{row['fechaFormateada']}' WHERE idfecha = {row['idfecha']}")

conn.commit()
cur.close()
conn.close()