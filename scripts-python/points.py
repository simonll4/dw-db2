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

# Agrega las columnas 'puntos_local' y 'puntos_visitante' a la tabla 'ft_matchs'
cur.execute("ALTER TABLE dw.ft_matches ADD COLUMN puntos_local INT")
cur.execute("ALTER TABLE dw.ft_matches ADD COLUMN puntos_visitante INT")

# Actualiza las columnas 'puntos_local' y 'puntos_visitante' según las condiciones
cur.execute("""
    UPDATE dw.ft_matches 
    SET puntos_local = CASE 
        WHEN goles_local > goles_visitante THEN 3
        WHEN goles_local = goles_visitante THEN 1
        ELSE 0
    END,
    puntos_visitante = CASE 
        WHEN goles_local < goles_visitante THEN 3
        WHEN goles_local = goles_visitante THEN 1
        ELSE 0
    END
""")

# Confirma los cambios
conn.commit()

# Cierra la conexión
cur.close()
conn.close()
