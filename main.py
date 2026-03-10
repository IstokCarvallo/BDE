import bcchapi
import requests
import pyodbc
import logging
import os
from datetime import datetime, timedelta

# crear carpeta logs
os.makedirs("logs", exist_ok=True)

# nombre archivo log
log_file = f"logs/paridad_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logging.info("Inicio ejecución script paridades")

# Crear una instancia de la clase Siete utilizando el archivo de credenciales
siete = bcchapi.Siete(file="credenciales.txt")

# Fecha actual
hoy = datetime.today()
# hoy = datetime.strptime('2026-02-28', "%Y-%m-%d")

# ajustar si es fin de semana
if hoy.weekday() == 5:  # sábado
    fecha_consulta = hoy - timedelta(days=1)
elif hoy.weekday() == 6:  # domingo
    fecha_consulta = hoy - timedelta(days=2)
else:
    fecha_consulta = hoy

desde = (fecha_consulta - timedelta(days=7)).strftime("%Y-%m-%d")
hasta = fecha_consulta.strftime("%Y-%m-%d")

# Series del Banco Central
series = {
    "Dolar": "F073.TCO.PRE.Z.D",
    "Euro": "F072.CLP.EUR.N.O.D",
    # "Yuan": "F072.CLP.CNY.N.O.D",
    "UF": "F073.UFF.PRE.Z.D",
    # "Libra": "F072.CLP.GBP.N.O.D",
    "Yuan_USD": "F072.CNY.USD.N.O.D",
    "Libra_USD": "F072.GBP.USD.N.O.D"
}

paridades = {}

for nombre, codigo in series.items():
    try:
        datos = siete.cuadro(
            series=[codigo],
            desde=desde,
            hasta=hasta
        )

        if not datos.empty:
            paridades[nombre] = datos.iloc[-1, 0]
        else:
            paridades[nombre] = "Sin dato disponible"

        logging.info(f"Consultando serie {nombre} ({codigo})")

    except Exception as e:
        paridades[nombre] = None
        logging.error(f"Error consultando {nombre}: {e}")

# UTM desde API pública
try:
    r = requests.get("https://mindicador.cl/api/utm", timeout=10)
    r.raise_for_status()
    paridades["UTM"] = r.json()["serie"][0]["valor"]
    logging.info("Consultando UTM API")
except Exception as e:
    paridades["UTM"] = "Sin dato disponible"
    logging.error(f"Error consultando UTM API: {e}")
    logging.warning("Sugerencia: verificar conectividad o disponibilidad de https://mindicador.cl")

# mapeo monedas
monedas = {
    "Dolar":1,
    "UF":2,
    "UTM":3,
    "Euro":4,
    "Libra_USD":5,
    "Yuan_USD":6
}

# construir batch
rows = []
fecha_db = fecha_consulta.date()

for nombre, valor in paridades.items():

    if valor is None or valor == "Sin dato disponible":
        continue

    rows.append(
        (
            monedas[nombre],
            fecha_db,
            valor
        )
    )

logging.info(f"Registros totales obtenidos: {len(rows)}")

try:
    # conexión SQL Server
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "TrustServerCertificate=yes;"
        "SERVER=192.168.20.37;"
        "DATABASE=Produccion_2025;"
        "UID=istok.carvallo;"
        "PWD=prag"
    )

    logging.info("Conexión a SQL Server establecida")

    cursor = conn.cursor()
    cursor.fast_executemany = True

    sql = """
    INSERT INTO dbo.Paridad (Moneda, Fecha, Paridad)
    SELECT ?, ?, ?
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM dbo.Paridad
        WHERE Moneda = ?
        AND Fecha = ?
    )
    """

    rows_insert = [
        (moneda, fecha, valor, moneda, fecha)
        for moneda, fecha, valor in rows
    ]

    cursor.executemany(sql, rows_insert)

    conn.commit()

    logging.info(f"Insert ejecutado correctamente. Registros insertados: {len(rows_insert)}")
except Exception as e:
    logging.error(f"Error de conexión o ejecución SQL Server: {e}")
    logging.warning("Sugerencia: verificar servidor, credenciales o conectividad de red")
finally:
    cursor.close()
    conn.close()

logging.info("Ejecución finalizada correctamente")