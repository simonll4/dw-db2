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

# Agrega las nuevas columnas scoreHome y scoreAway
cur.execute("""
    ALTER TABLE dw.ft_matches
    ADD COLUMN scoreHome INT,
    ADD COLUMN scoreAway INT
""")

# Actualiza las nuevas columnas basándose en los goles de cada equipo
cur.execute("""
    UPDATE dw.ft_matches
    SET scoreHome = CASE
        WHEN goles_local > goles_visitante THEN 3
        WHEN goles_local < goles_visitante THEN 0
        ELSE 1
    END,
    scoreAway = CASE
        WHEN goles_local < goles_visitante THEN 3
        WHEN goles_local > goles_visitante THEN 0
        ELSE 1
    END
""")

# Confirma los cambios
conn.commit()

# Cierra la conexión
cur.close()
conn.close()