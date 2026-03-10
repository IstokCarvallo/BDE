import bcchapi
import requests
from datetime import datetime, timedelta

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
    "Yuan": "F072.CLP.CNY.N.O.D",
    "UF": "F073.UFF.PRE.Z.D",
    "Libra": "F072.CLP.GBP.N.O.D"
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

    except Exception as e:
        paridades[nombre] = None
        print(f"Error consultando {nombre}: {e}")

# UTM desde API pública
try:
    r = requests.get("https://mindicador.cl/api/utm", timeout=10)
    r.raise_for_status()
    paridades["UTM"] = r.json()["serie"][0]["valor"]
except Exception as e:
    print("Error consultando UTM:", e)
    paridades["UTM"] = "Sin dato disponible"

print("Paridades para", fecha_consulta)
for k, v in paridades.items():
    print(f"{k}: {v}")