---
titulo: Qué es Spark y qué hace aquí
resumen: Un motor para procesar datos en paralelo. Driver, ejecutores, modo local, el laboratorio del curso y el caso real que sirve de hilo.
bloque: Fundamentos
minutos: 12
practica: si
---

**Apache Spark** es un motor para procesar muchos datos **en paralelo**: reparte el trabajo en trozos
(particiones) y los procesa a la vez en varios núcleos o en varias máquinas. **PySpark** es su API de
Python: escribes Python, pero el trabajo pesado lo hace Spark en la JVM (Java).

## Las piezas

```text
           tu script de Python
                  │  (py4j)
            ┌─────▼─────┐
            │  DRIVER   │  planifica: convierte tu código en un plan y lo parte en tareas
            └─────┬─────┘
       ┌──────────┼──────────┐
 ┌─────▼────┐ ┌───▼──────┐ ┌─▼────────┐
 │ ejecutor │ │ ejecutor │ │ ejecutor │  ejecutan tareas, una por partición de datos
 └──────────┘ └──────────┘ └──────────┘
```

| Pieza | Qué hace | En el observatorio |
| --- | --- | --- |
| **Driver** | El proceso que ejecuta tu programa, construye el plan y coordina. | El proceso `python -m observatorio.fusion`. |
| **Ejecutores** | Procesos (o hilos) que hacen el trabajo sobre las particiones. | Dos hilos dentro del mismo contenedor. |
| **Gestor del clúster** | Reparte máquinas entre aplicaciones (YARN, Kubernetes, Standalone). | Ninguno: modo local. |
| **SparkSession** | La puerta de entrada desde tu código. | Una función `crear_sesion` del servicio. |

## Modo local: Spark sin clúster

Con `master("local[2]")` el driver y los ejecutores viven en un solo proceso y Spark usa **2 hilos** como
si fueran 2 núcleos de un clúster. `local[*]` usaría todos los núcleos. Es lo que hace la fusión:

```yaml titulo="compose.yaml (servicio fusion)"
SPARK_MASTER: ${SPARK_MASTER:-local[2]}
```

El mismo código funcionaría contra un clúster cambiando solo el *master*: esa es la gracia.

## Por qué Spark en este proyecto

En el observatorio, con honestidad (así lo dejaron escrito al decidirlo):

- **En directo son unos cientos de terremotos al día.** Para eso Spark está sobredimensionado: un bucle de
  Python bastaría.
- **El relleno de 2025–2026 son 227.055 informes** que hay que leer de Kafka por lotes con tope de tamaño.
  Ahí Spark encaja: lee en paralelo, controla cuánto entra por lote y guarda por dónde va.
- **Objetivo docente**: Structured Streaming con Kafka es el patrón estándar de Big Data.
- **Deja el camino abierto** a más fuentes (ISC, INGV…): un topic más en el patrón de suscripción.

:::nota Spark frente a pandas
pandas carga todo en la memoria de **un** proceso y ejecuta cada operación **en el momento**. Spark
reparte los datos en particiones, **planifica** antes de ejecutar y puede usar muchas máquinas. Para
18.000 filas, pandas es más rápido (Spark tarda segundos solo en arrancar la JVM). Spark gana cuando los
datos no caben en una máquina o llegan sin parar.
:::

:::nota El caso real que sirve de hilo
El curso sigue un problema real: un **observatorio sísmico** que recibe terremotos de dos agencias (USGS,
de Estados Unidos, y EMSC, europea) por Kafka y tiene que **fusionar** los informes que describen el mismo
terremoto. Lo resolvió con un servicio en PySpark, «la fusión», y las cajas que la mencionan cuentan qué
decisión tomó y por qué. **No necesitas ese proyecto**: los datos vienen con el curso y en las lecciones 22
y 23 construyes tu propia versión de la fusión.
:::

## Tu primer script

Todas las prácticas se ejecutan en el **laboratorio** del curso, con Docker Desktop abierto. Está en
`curso-pyspark/laboratorio/compose.yaml` y tiene dos piezas:

| Servicio | Qué es |
| --- | --- |
| `kafka` | Un broker de Kafka 4.1 (para las lecciones 17 a 23). Dentro de Docker se llama `kafka:19092`. |
| `spark` | La imagen de las prácticas: Python 3.12, Java 17, PySpark 4.2.1 y el conector de Kafka. Monta `practicas/` en `/practicas` y `datos/` en `/datos`. |

No hace falta instalar Java ni Spark en Windows. En PowerShell, **desde la carpeta `laboratorio`**:

```powershell pruebalo
cd ruta\a\curso-pyspark\laboratorio
docker compose up -d
function spark-py { docker compose run --rm spark python @args }
```

`spark-py` es un atajo (dura hasta que cierres la ventana): `spark-py 01_hola_spark.py` ejecuta ese script
de `practicas/` en un contenedor temporal. La primera vez construye la imagen, que descarga unos cientos de
MB y tarda varios minutos.

```python titulo="curso-pyspark/practicas/01_hola_spark.py"
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("curso").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Spark", spark.version, "| master", spark.sparkContext.master)
print("Paralelismo por defecto:", spark.sparkContext.defaultParallelism)
df = spark.range(1_000_000)
print("Filas:", df.count(), "| particiones:", df.rdd.getNumPartitions())
spark.stop()
```

```powershell pruebalo
spark-py 01_hola_spark.py
```

```salida
Spark 4.2.1 | master local[2]
Paralelismo por defecto: 2
Filas: 1000000 | particiones: 2
```

Antes de esas líneas verás avisos de arranque de la JVM (`Using Spark's default log4j profile`,
`NativeCodeLoader`…). Son normales. Para ocultarlos, añade `2>$null` al final de la orden.

:::nota También aquí mismo, como en un cuaderno
Con el laboratorio en marcha (`.\curso.ps1`), los bloques de Python de las lecciones tienen un botón
**Ejecutar**: el código corre en Docker, en una sesión de Spark propia de cada lección, y la salida aparece
debajo del bloque. Los bloques comparten variables como las celdas de un cuaderno de Jupyter; si ejecutas
uno sin los anteriores, se ejecutan antes los que falten. Puedes editar el código y volver a ejecutarlo
(Mayús+Intro). **Abrir como cuaderno** abre la lección en JupyterLab (`.\curso.ps1 cuadernos` abre todas).
:::

:::cuidado Un millón de filas, dos particiones
`spark.range` reparte las filas según el paralelismo: 2 hilos → 2 particiones de 500.000. El número de
particiones decide cuántas tareas pueden ir a la vez. Lo verás a fondo en la lección de particiones.
:::

:::ejercicio Cambia el paralelismo
Copia `01_hola_spark.py` a `01b.py` y cambia `local[2]` por `local[4]` y por `local[*]`. ¿Qué cambia en
la salida? Mira cuántos núcleos tiene el contenedor con `spark-py -c "import os; print(os.cpu_count())"`.

:::solucion
Con `local[4]` el paralelismo por defecto es 4 y `spark.range` crea 4 particiones. Con `local[*]` usa
tantos hilos como núcleos vea el contenedor (en el equipo donde se escribió el curso, 12). El resultado
(`Filas: 1000000`) no cambia: solo cuánto trabajo va en paralelo.
:::

```quiz
¿Qué proceso construye el plan de ejecución y reparte las tareas?
* El driver.
- Cada ejecutor, por su cuenta.
- Kafka.
> El driver ejecuta tu programa, construye el plan y lo parte en tareas; los ejecutores solo ejecutan tareas sobre particiones.
```

```quiz
La fusión usa `local[2]`. ¿Qué significa?
- Dos máquinas en un clúster.
* Driver y ejecutores en un proceso, con 2 hilos de trabajo.
- Dos copias del servicio de fusión.
> En modo local todo vive en un proceso; el número entre corchetes es cuántas tareas pueden ir a la vez.
```
