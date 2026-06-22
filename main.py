import bcchapi, requests, pyodbc, logging,smtplib
import os, time, sys 
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()

EXEC_ID = (f"{datetime.now():%Y%m%d_%H%M%S}_{os.getpid()}")

def ejecutar_con_reintento(funcion, nombre, intentos=3, espera=300):
    inicio_total = time.time()

    ultimo_resultado = None
    ultimas_alertas = []

    for intento in range(1, intentos + 1):

        inicio_intento = time.time()
        logging.info("=" * 60)
        logging.info(f"{nombre} - intento {intento}/{intentos}")

        try:
            resultado, alertas_local = funcion()
            duracion = round(time.time() - inicio_intento, 2)
            logging.info(f"{nombre} duración intento: {duracion}s")
            ultimo_resultado = resultado

            if len(alertas_local) == 0:
                duracion_total = round(time.time() - inicio_total, 2)
                logging.info(f"{nombre} completado correctamente")
                logging.info(f"{nombre} completado en intento {intento}")
                logging.info(f"{nombre} duración total: {duracion_total}s")
                logging.info("=" * 60)

                return ultimo_resultado, []

            ultimas_alertas = alertas_local

            logging.warning(f"{nombre} terminó con {len(alertas_local)} alerta(s)")

            for alerta in alertas_local:
                logging.warning(alerta)

            if intento < intentos:
                logging.info(f"Esperando {espera} segundos antes del siguiente intento")
                time.sleep(espera)

        except Exception as e:
            logging.exception(f"Error inesperado en {nombre}: {e}")
            ultimas_alertas = [str(e)]

            if intento < intentos:
                time.sleep(espera)

    duracion_total = round(time.time() - inicio_total, 2)

    logging.error(f"{nombre} agotó todos los intentos")
    logging.error(f"Duración total etapa: {duracion_total}s")
    logging.info("=" * 60)

    return ultimo_resultado, ultimas_alertas

def cargar_bcch():
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

            logging.info(f"Consultando serie {nombre}")

        except Exception as e:
            paridades[nombre] = None
            alertas.append(normalizar_error(f"BCCh {nombre}", e ))
            logging.error(f"Error consultando {nombre}: {e}")

    return paridades, alertas

def cargar_utm():
    alertas = []
    try:
        r = requests.get("https://mindicador.cl/api/utm", timeout=20)
        r.raise_for_status()
        valor = r.json()["serie"][0]["valor"]
        logging.info("Consultando UTM API")
        return valor, []

    except Exception as e:
        logging.error(f"Error consultando UTM API: {e}")
        alertas.append(normalizar_error("API UTM", e))
        return None, alertas
    
def cargar_sql(rows):
    alertas = []

    conn = None
    cursor = None

    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={os.getenv('DB1_SERVER')};"
            f"DATABASE={os.getenv('DB1_NAME')};"
            f"UID={os.getenv('DB1_USER')};"
            f"PWD={os.getenv('DB1_PASS')};"
            f"TrustServerCertificate=yes;",
            autocommit=True
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
            (
                moneda,
                fecha,
                valor,
                moneda,
                fecha
            )

            for moneda, fecha, valor in rows
        ]

        if len(rows_insert) == 0:
            alertas.append("No existen registros válidos para insertar")
            return False, alertas

        cursor.executemany(sql, rows_insert)
        logging.info(f"Insert ejecutado correctamente. Registros insertados: {len(rows_insert)}")

        cursor.execute("EXEC dbo.FProc_CargaParidad_FICO")
        logging.info("SP dbo.FProc_CargaParidad_FICO ejecutado correctamente")
        return True, []

    except Exception as e:

        logging.error(
            f"Error SQL Server: {e}"
        )

        alertas.append(
            normalizar_error(
                "SQL Server",
                e
            )
        )

        return False, alertas

    finally:

        if cursor:
            cursor.close()

        if conn:
            conn.close()

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

    <p><b>ID ejecución:</b> {EXEC_ID}</p>
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
    SMTP_SERVER = os.getenv("SMTP_SERVER")
    SMTP_PORT = int(os.getenv("SMTP_PORT"))
    SMTP_USER = os.getenv("SMTP_USER")
    SMTP_PASS = os.getenv("SMTP_PASS")

    destinatarios = os.getenv("EMAIL_TO").split(",")

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
log_file = (f"logs/paridad_{EXEC_ID}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

logging.info("=" * 60)
logging.info(f"EXEC_ID={EXEC_ID}")
logging.info("Inicio ejecución script paridades")
logging.info(f"Python executable: {sys.executable}")
logging.info(f"Directorio actual: {os.getcwd()}")

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

paridades, alertas = ejecutar_con_reintento(
    cargar_bcch,
    nombre="Carga BCCh",
    intentos=3,
    espera=300
)

# UTM desde API pública
utm, alertas_utm = ejecutar_con_reintento(
    cargar_utm,
    nombre="Carga UTM",
    intentos=3,
    espera=300
)

alertas.extend(alertas_utm)

if utm:
    paridades["UTM"] = utm
else:
    paridades["UTM"] = "Sin dato disponible"

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

sql_ok, alertas_sql = ejecutar_con_reintento(
    lambda: cargar_sql(rows),
    nombre="Carga SQL",
    intentos=3,
    espera=300
)

alertas.extend(alertas_sql)

if alertas:
    html = construir_html_alertas(alertas, len(rows))
    enviar_mail(f"[{EXEC_ID}] ALERTA ETL Paridades BCCh", html)
    logging.warning("Correo de alerta enviado")
    sys.exit(1)
else:
    logging.info(f"""Resumen ejecución
                    -----------------
                    Paridades obtenidas: {len(paridades)}
                    Registros SQL: {len(rows)}
                    Alertas: {len(alertas)}
                    """)
    logging.info("Ejecución finalizada correctamente")
    sys.exit(0)