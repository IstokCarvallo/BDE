import bcchapi
import requests
from datetime import datetime, timedelta

# Crear una instancia de la clase Siete utilizando el archivo de credenciales
siete = bcchapi.Siete(file="credenciales.txt")

# Fecha actual
fecha_hoy = datetime.today().strftime("%Y-%m-%d")
# desde = (fecha_hoy - timedelta(days=10)).strftime("%Y-%m-%d")
# hasta = fecha_hoy.strftime("%Y-%m-%d")

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

    datos = siete.cuadro(
        series=[codigo],
        desde=fecha_hoy,
        hasta=fecha_hoy
    )

    if not datos.empty:
        paridades[nombre] = datos.iloc[0, 0]
    else:
        paridades[nombre] = "Sin dato disponible"

# UTM desde API pública
try:
    r = requests.get("https://mindicador.cl/api/utm")
    paridades["UTM"] = r.json()["serie"][0]["valor"]
except:
    paridades["UTM"] = None

print("Paridades para", fecha_hoy)
for k, v in paridades.items():
    print(f"{k}: {v}")