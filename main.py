import bcchapi
import requests
import pyodbc
import logging
import os
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText


def normalizar_error(nombre, e):

    msg = str(e)

    if "SSLError" in msg:
        return f"{nombre}: Falla conexión SSL con Banco Central"

    if "Read timed out" in msg:
        return f"{nombre}: Timeout API externa"

    if "Login failed" in msg:
        return f"{nombre}: Error autenticación SQL Server"

    if "executemany must not be empty" in msg:
        return "No existen registros válidos para insertar"

    return f"{nombre}: {msg[:120]}"


def construir_html_alertas(alertas, total_registros):

    items = "".join([f"<li>{a}</li>" for a in alertas])

    html = f"""
    <html>
    <body style="font-family: Arial;">

    <h2 style="color:#b30000;">⚠️ Alerta ETL Paridades</h2>

    <p><b>Fecha ejecución:</b> {datetime.now()}</p>
    <p><b>Registros válidos obtenidos:</b> {total_registros}</p>

    <p><b>Deben ingresar de manera manual los datos faltantes</b></p>

    <h3>Errores detectados:</h3>

    <ul>
        {items}
    </ul>

    <hr>

    <p style="font-size:12px;color:gray;">
    Proceso automático ETL Paridades Banco Central
    </p>

    </body>
    </html>
    """

    return html


def enviar_mail(asunto, html):

    SMTP_SERVER = "smtp.office365.com"
    SMTP_PORT = 587
    SMTP_USER = "sendmail@rioblanco.net"
    SMTP_PASS = "Rh32NSene_%654"

    destinatarios = ["istok.carvallo@rioblanco.net",
                    "lino.escobedo@rioblanco.net",
                    "romina.espinoza@rioblanco.net"]

    msg = MIMEText(html, "html")
    msg["Subject"] = asunto
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(destinatarios)

    try:
        smtp = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        smtp.starttls()      
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.sendmail(SMTP_USER, destinatarios, msg.as_string())
        smtp.quit()

        logging.info("Correo enviado correctamente")
    except Exception as e:
        logging.error(f"Error enviando correo: {e}")

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
# hoy = datetime.strptime('2026-03-21', "%Y-%m-%d")

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
    "Yuan": "F072.CLP.CNY.N.O.D",
    "UF": "F073.UFF.PRE.Z.D",
    "Libra": "F072.CLP.GBP.N.O.D"
    # "Yuan_USD": "F072.CNY.USD.N.O.D",
    # "Libra_USD": "F072.GBP.USD.N.O.D"
}

alertas = []
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
            alertas.append(f"No se encontró dato BCCh para {nombre}")

        logging.info(f"Consultando serie {nombre} ({codigo})")

    except Exception as e:
        paridades[nombre] = None
        alertas.append(normalizar_error(f"BCCh {nombre}", e))
        logging.error(f"Error consultando {nombre}: {e}")

# UTM desde API pública
try:
    r = requests.get("https://mindicador.cl/api/utm", timeout=10)
    r.raise_for_status()
    paridades["UTM"] = r.json()["serie"][0]["valor"]
    logging.info("Consultando UTM API")
except Exception as e:
    paridades["UTM"] = "Sin dato disponible"
    alertas.append(normalizar_error("API UTM", e))
    logging.error(f"Error consultando UTM API: {e}")
    logging.warning("Sugerencia: verificar conectividad o disponibilidad de https://mindicador.cl")

# mapeo monedas
monedas = {
    "Dolar":1,
    "UF":2,
    "UTM":3,
    "Euro":4,
    "Libra":5,
    "Yuan":6
}

# construir batch
rows = []
fecha_db = hoy.date()

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
conn, cursor = None, None
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
    alertas.append(normalizar_error("SQL Server", e))
finally:
    cursor.close()
    conn.close()

if len(rows) == 0:
    alertas.append("No se obtuvieron registros válidos para insertar")

if alertas:

    html = construir_html_alertas(alertas, len(rows))

    enviar_mail("ALERTA ETL Paridades BCCh", html)

    logging.warning("Correo de alerta enviado")

logging.info("Ejecución finalizada correctamente")