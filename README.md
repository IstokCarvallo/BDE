# ETL Paridades Banco Central

Proceso ETL desarrollado en Python para obtener paridades monetarias desde fuentes externas, almacenar resultados en SQL Server y ejecutar procesos posteriores de integración.

---

## Objetivo

Automatizar diariamente la obtención de indicadores económicos y cargarlos en base de datos evitando duplicados, incorporando:

* Reintentos automáticos ante fallas temporales.
* Registro detallado de ejecución.
* Alertas por correo electrónico.
* Compatibilidad con ejecución mediante Programador de tareas de Windows.

---

## Funcionalidades

### Extracción de datos

Obtiene información desde:

| Fuente        | Datos                        |
| ------------- | ---------------------------- |
| Banco Central | Dólar, Euro, Yuan, UF, Libra |
| API pública   | UTM                          |

### Transformación

* Ajuste automático de fecha si la ejecución ocurre fin de semana.
* Validación de registros obtenidos.
* Normalización de mensajes de error.
* Preparación de carga masiva.

### Carga

Inserta registros en:

```text
dbo.Paridad
```

Ejecución posterior del procedimiento:

```text
dbo.FProc_CargaParidad_FICO
```

La carga evita duplicados mediante validación por:

```text
Moneda + Fecha
```

---

# Arquitectura

```text
ETL
│
├── Extracción BCCh
│     └── Retry automático
│
├── Extracción UTM
│     └── Retry automático
│
├── Inserción SQL Server
│     └── Retry automático
│
├── Generación Alertas
│
└── Correo + Código salida
```

---

# Flujo de ejecución

```text
Inicio
 ↓
Obtención BCCh
 ↓
Obtención UTM
 ↓
Construcción Batch
 ↓
Carga SQL
 ↓
Ejecución SP
 ↓
Validación alertas
 ↓
Correo (si aplica)
 ↓
Fin
```

---

# Reintentos automáticos

Cada etapa posee reintentos independientes.

Configuración actual:

| Parámetro             | Valor        |
| --------------------- | ------------ |
| Intentos máximos      | 3            |
| Espera entre intentos | 300 segundos |

Comportamiento:

```text
Falla etapa
↓
Esperar
↓
Reintentar
↓
Éxito → continuar
↓
Falla definitiva → alerta
```

Las etapas reintentables son:

* BCCh
* API UTM
* SQL Server

---

# Variables de entorno

Crear archivo:

```text
.env
```

Ejemplo:

```env
DB1_SERVER=
DB1_NAME=
DB1_USER=
DB1_PASS=

SMTP_SERVER=
SMTP_PORT=
SMTP_USER=
SMTP_PASS=

EMAIL_TO=
```

---

# Credenciales Banco Central

Crear archivo:

```text
credenciales.txt
```

Ejemplo:

```text
usuario
password
```

---

# Instalación

Crear entorno virtual:

```bash
python -m venv venv
```

Activar:

```bash
venv\Scripts\activate
```

Instalar dependencias:

```bash
pip install -r requirements.txt
```

---

# Dependencias principales

```text
bcchapi
requests
pyodbc
python-dotenv
```

Generar archivo:

```bash
pip freeze > requirements.txt
```

---

# Ejecución local

```bash
python main.py
```

---

# Programador de tareas Windows

Configuración recomendada:

## Acción

Programa:

```text
python.exe
```

Argumentos:

```text
main.py
```

Inicio en:

```text
ruta_proyecto
```

---

## Configuración recomendada

☑ Ejecutar independientemente del inicio de sesión

☑ Reiniciar si falla

Cantidad:

```text
3 intentos
```

Intervalo:

```text
5 minutos
```

---

# Logs

Ubicación:

```text
logs/
```

Formato:

```text
paridad_YYYYMMDD_HHMMSS.log
```

Cada ejecución posee:

* EXEC_ID único
* Duración por etapa
* Cantidad de reintentos
* Resultado final

Ejemplo:

```text
EXEC_ID=20260701_080001
Carga BCCh completado
Carga SQL completado
Ejecución finalizada correctamente
```

---

# Correos de alerta

Se envían únicamente cuando persisten errores luego de todos los intentos.

Contenido:

* Fecha ejecución
* ID ejecución
* Cantidad registros
* Errores detectados

---

# Códigos de salida

| Código | Significado       |
| ------ | ----------------- |
| 0      | Ejecución exitosa |
| 1      | Error ETL         |

Permite integración con herramientas de automatización y monitoreo.

---

# Estructura proyecto

```text
.
├── main.py
├── .env
├── credenciales.txt
├── requirements.txt
├── logs/
└── README.md
```

---

# Mejoras futuras

* Dashboard monitoreo
* Métricas históricas
* Health checks
* Notificaciones adicionales
* Contenedorización
* Integración CI/CD

---

## Autor

Proyecto interno de automatización ETL para carga de paridades económicas.
