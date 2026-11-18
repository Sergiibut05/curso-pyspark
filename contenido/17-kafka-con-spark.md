---
titulo: Leer y escribir Kafka con Spark
resumen: Las columnas que da la fuente de Kafka, las opciones que importan y cómo se escribe de vuelta.
bloque: Streaming
minutos: 22
practica: si
---

Spark trae un conector para Kafka (`spark-sql-kafka-0-10`). No viene dentro de PySpark: en la imagen de
la del curso sus jars se descargan al construirla y están en `/opt/spark-jars`, y la sesión los carga con
`spark.jars`: es lo que hace `comun.sesion(..., kafka=True)`.

## Antes de nada: llena los topics

El broker del laboratorio empieza vacío. `00_cargar_kafka.py` publica los terremotos de **enero de 2025**
de las dos agencias que usa el curso (los ficheros de `datos/relleno/`): cada informe en un sobre
`{"fuente": …, "event": <feature GeoJSON original>}`, con el id del informe como clave. A uno de cada 40
informes de USGS le añade además una **revisión** (mismo id, `updated` un minuto después, magnitud
corregida), como hace una agencia de verdad.

```powershell pruebalo
spark-py 00_cargar_kafka.py 2>$null
spark-py 00_cargar_kafka.py 2>$null
```

```salida
eventos.usgs: 2239 informes + 67 revisiones = 2306 mensajes
eventos.emsc: 5961 mensajes
Los topics ya tienen datos: no se carga nada. Para empezar de cero: docker compose down -v
```

La segunda ejecución no duplica nada: comprueba antes si los topics tienen datos. Los topics se crean solos
al escribir en ellos, con 3 particiones (el broker del laboratorio lo permite por comodidad; en producción
se crean a mano). Cómo escribe Spark en Kafka lo verás al final de esta lección.

```python preparacion
from pyspark.sql import functions as F

from comun import KAFKA, sesion

spark = sesion("kafka-lote", kafka=True)   # kafka=True carga el conector de Kafka
```

## Leer en modo lote para ver qué llega

`spark.read` (sin `Stream`) lee un rango de offsets una vez. Es la mejor forma de explorar:

```python titulo="curso-pyspark/practicas/17_kafka_lote.py"
kafka = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "eventos.usgs")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)
kafka.printSchema()
```

```salida
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)
```

**Siempre** el mismo esquema, lea lo que lea: clave y valor como **bytes**, y la posición de cada mensaje.
Interpretar el valor es cosa tuya (`cast("string")` y funciones JSON).

:::nota Dentro de Compose, kafka:19092
El contenedor `spark` y el broker están en la misma red de Docker (la del laboratorio), así que llega al
broker por su listener interno, `kafka:19092`. Con `localhost:9092` fallaría: dentro de un contenedor, `localhost`
es el propio contenedor (está explicado en el curso de Kafka, lección de listeners).
:::

```python
kafka.groupBy("partition").agg(F.count("*").alias("mensajes"), F.min("offset"), F.max("offset")).orderBy("partition").show()

texto = F.col("value").cast("string")
(kafka.select(
    F.col("key").cast("string").alias("clave"),
    "partition", "offset",
    F.get_json_object(texto, "$.event.properties.mag").cast("double").alias("mag"),
    F.get_json_object(texto, "$.event.properties.place").alias("lugar"),
 )
 .orderBy(F.col("offset").desc())
 .show(5, truncate=False))
```

```salida
+---------+--------+-----------+-----------+
|partition|mensajes|min(offset)|max(offset)|
+---------+--------+-----------+-----------+
|        0|     756|          0|        755|
|        1|     781|          0|        780|
|        2|     769|          0|        768|
+---------+--------+-----------+-----------+

+------------+---------+------+----+------------------------------------+
|clave       |partition|offset|mag |lugar                               |
+------------+---------+------+----+------------------------------------+
|ak0251fl9a6s|1        |780   |3.0 |67 km SSE of Perryville, Alaska     |
|us7000p9vy  |1        |779   |2.9 |27 km NE of Stanley, Idaho          |
|us7000pbu4  |1        |778   |4.5 |87 km W of Kendari, Indonesia       |
|us6000pmkg  |1        |777   |4.3 |26 km NW of Clayton, Idaho          |
|hv74601837  |1        |776   |2.71|21 km S of Honaunau-Napoopoo, Hawaii|
+------------+---------+------+----+------------------------------------+
only showing top 5 rows
```

Los últimos mensajes de cada partición son **revisiones**: se publicaron al final. `ak0251fl9a6s` tiene
dos mensajes, y los dos están en la partición 1 (misma clave, misma partición):

```salida
+---------+------+-------------+---+
|partition|offset|      updated|mag|
+---------+------+-------------+---+
|        1|   749|1744750526040|2.9|
|        1|   780|1744750586040|3.0|
+---------+------+-------------+---+
```

`updated` es un minuto mayor en la revisión. Quedarse con la versión más nueva de cada informe es lo que
hace la ventana de la lección 9.

### Un rango concreto de offsets

```python
trozo = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "eventos.usgs")
    .option("startingOffsets", '{"eventos.usgs": {"0": 100, "1": 100, "2": 100}}')
    .option("endingOffsets", '{"eventos.usgs": {"0": 110, "1": 105, "2": 101}}')
    .load()
)
print("Mensajes en el trozo:", trozo.count())
```

```salida
Mensajes en el trozo: 16
```

10 + 5 + 1 = 16: el final es **exclusivo**, como el `range` de Python.

```powershell pruebalo
spark-py 17_kafka_lote.py 2>$null
```

## Las opciones de lectura que importan

| Opción | Qué hace | En la fusión del observatorio |
| --- | --- | --- |
| `subscribe` / `subscribePattern` / `assign` | Topics por nombre, por expresión regular o particiones concretas. | `subscribePattern = eventos\.(usgs\|emsc)` |
| `startingOffsets` | Dónde empezar **la primera vez** (sin checkpoint): `earliest`, `latest` o JSON. | `earliest`: el relleno debe entrar entero. |
| `maxOffsetsPerTrigger` | Tope de mensajes por micro-lote, repartido entre particiones. | `5000` (`FUSION_MAX_POR_LOTE`). |
| `failOnDataLoss` | Qué hacer si los offsets que tocaba leer ya no existen (retención). | `false`: avisar y seguir. |
| `kafka.<lo que sea>` | Se pasa tal cual al cliente de Kafka. | `kafka.metadata.max.age.ms = 10000` |

:::nota Las decisiones de la fusión del observatorio
- **`subscribePattern`**: si mañana hay `eventos.isc`, solo hay que ampliar el patrón.
- **`kafka.metadata.max.age.ms=10000`**: por defecto el cliente refresca la lista de topics cada 5 minutos.
  Si la fusión arranca antes que el productor de EMSC (el orden en Compose no está garantizado), tardaría 5
  minutos en descubrir `eventos.emsc`. Con 10 s, casi enseguida.
- **`failOnDataLoss=false`**: si la fusión estuviera parada más de 7 días, los mensajes caducados ya no
  existirían. Con `true` se quedaría parada para siempre esperando a alguien; con `false` sigue con lo que
  hay. El riesgo (perder lo caducado) está documentado en `research.md`.
- **`startingOffsets` solo cuenta la primera vez.** Después manda el checkpoint.
:::

:::nota Spark no usa grupos de consumidores
La fuente de Kafka de Spark **no confirma offsets en Kafka**: los guarda en su checkpoint. Por eso
`kafka-consumer-groups.sh --list` no muestra la fusión, y por eso no tiene sentido mirar su «lag» con esa
herramienta. Su progreso está en `checkpoint/offsets/`.
:::

## Escribir en Kafka

Un DataFrame con columnas `key` y `value` (texto o bytes) y, opcionalmente, `topic`, `partition` y
`headers`, se escribe con `format("kafka")`. Así llena los topics `00_cargar_kafka.py`:

```python fragmento titulo="practicas/00_cargar_kafka.py (extracto)"
(df.select(
    F.col("f.id").alias("key"),
    F.to_json(F.struct(F.lit(fuente).alias("fuente"), F.col("f").alias("event"))).alias("value"))
 .write.format("kafka")
 .option("kafka.bootstrap.servers", KAFKA)
 .option("topic", topic)
 .option("kafka.acks", "all")                  # el broker confirma cada lote antes de seguir
 .option("kafka.enable.idempotence", "true")   # reintentos sin duplicados
 .save())                                      # no vuelve hasta que Kafka ha confirmado todo
```

- `write` (no `writeStream`): es un DataFrame estático. Dentro de `foreachBatch` (lección 18) se escribe
  igual, porque el lote también es estático.
- `F.to_json(F.struct(...))` construye el JSON del valor a partir de columnas; la clave es el id.
- `kafka.acks=all` y `kafka.enable.idempotence=true` son las garantías del productor de Kafka (curso de
  Kafka, lección 7). `save()` no vuelve hasta que Kafka confirma.
- El particionado lo decide la **clave** con murmur2: el conector usa el cliente Java de Kafka.
- Las revisiones se escriben en un **segundo** `save()`, después de los originales. Spark escribe cada
  partición del DataFrame en paralelo, así que en un solo `save()` no hay garantía de que la revisión
  quede detrás de su original.

:::ejercicio Revisiones por terremoto
Con `spark.read` sobre `eventos.usgs`, calcula cuántas **versiones** (mensajes) tiene cada clave y enseña
las 5 claves con más versiones. ¿Cuántas claves distintas hay en total?

:::solucion
```python
por_clave = kafka.groupBy(F.col("key").cast("string").alias("clave")).count()
print("Claves distintas:", por_clave.count())
por_clave.orderBy(F.desc("count"), "clave").show(5)
```
```salida
Mensajes: 2306 Claves distintas: 2239
+------------+-----+
|       clave|count|
+------------+-----+
|ak0251fjfrm4|    2|
|ak0251fl9a6s|    2|
| ak0253jmz6r|    2|
| ak025ne5ai6|    2|
|  ci41033480|    2|
+------------+-----+
only showing top 5 rows
```

2.306 mensajes y 2.239 terremotos: las 67 revisiones son 67 claves con dos mensajes. Recuerda: para
contar **terremotos**, claves distintas; nunca mensajes.
:::

```quiz
¿Qué tipo tiene la columna `value` al leer de Kafka con Spark?
- `string`, ya decodificado como UTF-8.
- `struct`, con el JSON interpretado.
* `binary`: los bytes tal cual; hay que convertirlos.
> El conector no sabe qué hay dentro. `F.col("value").cast("string")` y luego funciones JSON.
```

```quiz
La fusión lleva meses con su checkpoint. Cambias `startingOffsets` de `earliest` a `latest` y reinicias. ¿Qué pasa?
- Salta al final de los topics y pierde lo pendiente.
* Nada: con checkpoint, `startingOffsets` se ignora y sigue donde iba.
- Spark falla porque el checkpoint no coincide.
> `startingOffsets` solo decide la primera ejecución. Después, el checkpoint es la fuente de verdad.
```
