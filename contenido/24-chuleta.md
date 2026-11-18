---
titulo: Chuleta, errores frecuentes y glosario
resumen: La API que más se usa en una página, los fallos típicos de PySpark con su causa y las palabras clave.
bloque: Proyecto
minutos: 10
---

## Ejecutar las prácticas

```powershell pruebalo
cd ruta\a\curso-pyspark\laboratorio            # todas las órdenes, desde aquí
docker compose up -d                           # Kafka del laboratorio
function spark-py { docker compose run --rm spark python @args }
spark-py 00_cargar_kafka.py 2>$null            # una vez: llena eventos.usgs y eventos.emsc
spark-py 02_dataframes.py 2>$null              # un script
spark-py                                       # intérprete interactivo: from comun import *
docker compose down -v                         # parar y borrar los datos de Kafka
```

## Sesión y lectura

```python fragmento
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = (SparkSession.builder.master("local[2]").appName("x")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

spark.read.json(ruta)                                   # JSON Lines (un objeto por línea)
spark.read.option("multiLine", True).json(ruta)         # un JSON repartido en varias líneas
spark.read.option("header", True).csv(ruta)
spark.read.parquet(ruta)
spark.createDataFrame([(1, "a")], "n int, s string")    # datos a mano, con esquema en texto
```

## Transformaciones

| Para… | Código |
| --- | --- |
| Elegir / renombrar | `df.select("a", F.col("b").alias("c"))` |
| Añadir columna | `df.withColumn("x", F.col("a") * 2)` |
| Filtrar | `df.filter((F.col("mag") >= 5) & (F.col("prof") < 70))` |
| Condicional | `F.when(cond, v).when(cond2, v2).otherwise(v3)` |
| Array → filas | `F.explode("features")` |
| Struct anidado | `F.col("f.properties.mag")`, `F.col("coords")[0]` |
| JSON en texto | `F.get_json_object(col, "$.a.b")`, `F.from_json(col, esquema)` |
| Fechas | `F.timestamp_millis`, `F.to_timestamp`, `F.unix_millis`, `F.to_date`, `F.month` |
| Primer no nulo | `F.coalesce(a, b)` |
| Agrupar | `df.groupBy("k").agg(F.count("*"), F.avg("x"), F.max("x"))` |
| Unir | `df.join(F.broadcast(peq), "k", "left")` |
| Ventana | `F.row_number().over(Window.partitionBy("k").orderBy(F.col("v").desc()))` |
| Fila anterior | `F.lag("x").over(Window.partitionBy("k").orderBy("t"))` |
| Ordenar | `df.orderBy(F.col("x").desc())` |
| Reparticionar | `df.repartition(8)`, `df.repartition(8, "k")`, `df.coalesce(2)` |

## Acciones

| Acción | Trae al driver |
| --- | --- |
| `df.show(n, truncate=False)` | `n` filas, para mirar |
| `df.count()` | un número |
| `df.take(n)`, `df.first()` | `n` filas / una |
| `df.collect()`, `df.toPandas()` | **todo**: cuidado con el tamaño |
| `df.write.mode("overwrite").parquet(ruta)` | nada; escribe en disco |

## Inspeccionar

```python fragmento
df.printSchema()
df.explain()                         # plan físico; busca los Exchange (shuffles)
df.rdd.getNumPartitions()
df.rdd.glom().map(len).collect()     # filas por partición (solo con datos pequeños)
```

## Streaming con Kafka

```python fragmento
flujo = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", "kafka:19092")
         .option("subscribePattern", r"eventos\.(usgs|emsc)")
         .option("startingOffsets", "earliest")       # solo la primera vez
         .option("maxOffsetsPerTrigger", "5000")
         .option("failOnDataLoss", "false")
         .load())                                     # key, value (binary), topic, partition, offset, timestamp…

def por_lote(lote, lote_id):                          # lote es un DataFrame normal
    ...
    salida.write.format("kafka").option("kafka.bootstrap.servers", "kafka:19092") \
          .option("topic", "eventos.live").option("kafka.acks", "all").save()

consulta = (flujo.writeStream.foreachBatch(por_lote)
            .option("checkpointLocation", "/ruta/duradera")
            .trigger(processingTime="15 seconds")     # o availableNow=True
            .start())
consulta.awaitTermination()
consulta.lastProgress                                 # informe del último micro-lote
```

## Errores frecuentes

| Síntoma | Causa probable | Arreglo |
| --- | --- | --- |
| `UNRESOLVED_COLUMN … Did you mean _corrupt_record` | JSON en varias líneas leído como JSON Lines. | `spark.read.option("multiLine", True).json(…)` |
| `Failed to find data source: kafka` | Falta el conector en `spark.jars`. | En la imagen del curso: `/opt/spark-jars/*.jar` (`sesion(kafka=True)`). |
| Conexión rechazada a `localhost:9092` desde el contenedor | `localhost` es el propio contenedor. | `kafka:19092`, el nombre del broker dentro de la red del laboratorio. |
| Error raro con `&` en un filtro | Precedencia de operadores en Python. | Paréntesis en cada condición. |
| Una tarea tarda mucho y las demás nada | Sesgo: una clave o un fichero se lleva casi todo. | Formato divisible (JSON Lines, Parquet), `repartition(n)` sin clave, revisar la clave. |
| El driver se queda sin memoria | `collect()` / `toPandas()` de un DataFrame grande. | `take`, `limit`, escribir a disco, o acotar (en streaming, `maxOffsetsPerTrigger`). |
| La consulta en streaming no lee nada nuevo al relanzarla | El checkpoint dice que ya lo leyó. | Es lo esperado. Para empezar de cero, otro checkpoint (o borrarlo, sabiendo lo que implica). |
| `print` dentro de una transformación no sale | Las transformaciones no se ejecutan hasta una acción (y se ejecutan en los ejecutores). | `show()`, `explain()`, o `print` dentro de `foreachBatch`, que corre en el driver. |
| Muchos avisos al arrancar (`NativeCodeLoader`, `log4j`…) | Ruido normal de la JVM. | `setLogLevel("ERROR")` y `2>$null`. |

## Glosario

| Término | En una frase |
| --- | --- |
| **Driver** | Proceso que ejecuta tu programa, planifica y reparte tareas. |
| **Ejecutor** | Proceso (o hilo, en local) que ejecuta tareas sobre particiones. |
| **Partición** | Trozo de las filas de un DataFrame; una tarea procesa una partición. |
| **Transformación** | Operación perezosa que devuelve otro DataFrame (`filter`, `select`…). |
| **Acción** | Operación que ejecuta el plan y devuelve un resultado (`count`, `show`…). |
| **Plan / Catalyst** | Lo que Spark va a hacer, y el optimizador que lo reescribe. |
| **Shuffle** | Redistribuir filas entre particiones por clave; corta etapas y es lo caro. |
| **Etapa** | Trozo del plan entre shuffles; dentro, cada partición va de principio a fin. |
| **AQE** | Ejecución adaptativa: reajusta el plan con lo que ve al ejecutar (p. ej., juntar particiones). |
| **Broadcast join** | Enviar la tabla pequeña entera a cada tarea para no barajar la grande. |
| **Ventana** | Cálculo por fila mirando su grupo ordenado (`row_number`, `lag`). |
| **Structured Streaming** | La API de DataFrames sobre datos que no paran de llegar. |
| **Micro-lote** | Cada ejecución incremental de una consulta en streaming. |
| **Disparador (trigger)** | Cuándo se lanza cada micro-lote. |
| **Checkpoint** | Carpeta donde la consulta guarda qué ha leído (`offsets/`) y qué ha terminado (`commits/`). |
| **foreachBatch** | Sumidero que entrega cada micro-lote como DataFrame normal a tu función. |
| **Watermark** | Límite de retraso para el estado de Spark; lo que llega más tarde se descarta. |
| **Determinista** | La misma entrada produce exactamente la misma salida; base de los reintentos seguros. |

```quiz
Último repaso: la fusión se reinicia tras un fallo. ¿De dónde saca Spark por qué offset seguir?
- De `__consumer_offsets` de Kafka.
- De la opción `startingOffsets`.
* Del checkpoint: el último `offsets/N` y si tiene su `commits/N`.
> La fuente de Kafka de Spark no usa grupos de consumidores; el checkpoint es su única memoria de progreso.
```
