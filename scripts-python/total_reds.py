import psycopg2

# Establece la conexión a la base de datos
conn = psycopg2.connect(
    host="localhost",
    database="dwdb2",
    user="user",
    password="123"
)

# Crea un cursor
cur = conn.cursor()

# Agrega la columna 'goles_totales' a la tabla 'ft_matchs'
cur.execute("ALTER TABLE dw.ft_matches ADD COLUMN rojas_totales INT")

# Actualiza la columna 'goles_totales' con la suma de 'goles_local' y 'goles_visitante'
cur.execute(
    "UPDATE dw.ft_matches SET rojas_totales = rojas_local + rojas_visitante")

# Confirma los cambios
conn.commit()

# Cierra la conexión
cur.close()
conn.close()
