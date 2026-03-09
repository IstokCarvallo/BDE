import bcchapi
from datetime import datetime

# Crear una instancia de la clase Siete utilizando el archivo de credenciales
siete = bcchapi.Siete(file="credenciales.txt")

# Fecha actual
fecha_hoy = datetime.today().strftime("%Y-%m-%d")

# Series del Banco Central
series = {
    "Dolar": "F073.TCO.PRE.Z.D",
    "Euro": "F072.CLP.EUR.N.O.D",
    "Yuan": "F072.CLP.CNY.N.O.D",
    "UF": "F073.UFF.PRE.Z.D"
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

print("Paridades para", fecha_hoy)
for k, v in paridades.items():
    print(f"{k}: {v}")