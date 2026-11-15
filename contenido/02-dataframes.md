---
titulo: SparkSession y DataFrames
resumen: Leer el GeoJSON de 2025, entender el esquema que deduce Spark y aplanarlo en una tabla.
bloque: Fundamentos
minutos: 20
practica: Con Docker, sin la pila
---

Un **DataFrame** de Spark es una tabla con columnas con nombre y tipo, como una tabla de SQL o un
DataFrame de pandas. La diferencia: está **repartida en particiones** y sus operaciones no se ejecutan al
escribirlas (lo verás en la lección siguiente).

## El dato: el catálogo de 2025

La carpeta del curso trae en `datos/historico/2025/eventos.geojson` los 18.302 terremotos M ≥ 4 de 2025
que publicó USGS (copiados del observatorio, donde son la base de la comparación histórica del panel). Es un GeoJSON: **un único objeto JSON** con
un array `features`, cada uno con `properties` y `geometry`.

```json titulo="Un elemento de features (recortado)"
{
  "type": "Feature",
  "id": "us6000pjr6",
  "properties": { "mag": 4.2, "place": "127 km WSW of Merizo Village, Guam", "time": 1735690402650,
                  "updated": 1742247237040, "felt": null, "magType": "mb", "net": "us", "status": "reviewed", … },
  "geometry": { "type": "Point", "coordinates": [143.6244, 12.7441, 35] }
}
```

## Leer y mirar el esquema

```python titulo="curso-pyspark/practicas/02_dataframes.py"
from pyspark.sql import functions as F
from comun import USGS_2025, sesion

spark = sesion("dataframes")

crudo = spark.read.json(USGS_2025)
print("Filas del fichero:", crudo.count())
crudo.printSchema()
```

```salida
Filas del fichero: 1
root
 |-- features: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- geometry: struct (nullable = true)
 |    |    |    |-- coordinates: array (nullable = true)
 |    |    |    |    |-- element: double (containsNull = true)
 |    |    |    |-- type: string (nullable = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- properties: struct (nullable = true)
 |    |    |    |-- alert: string (nullable = true)
 |    |    |    |-- felt: long (nullable = true)
 |    |    |    |-- mag: double (nullable = true)
 |    |    |    |-- magType: string (nullable = true)
 |    |    |    |-- place: string (nullable = true)
 |    |    |    |-- time: long (nullable = true)
 |    |    |    |-- updated: long (nullable = true)
 |    |    |    |-- …
 |-- type: string (nullable = true)
```

Dos cosas importantes:

1. **Una sola fila.** `spark.read.json` espera **un objeto JSON por línea** (JSON Lines). Este fichero es un
   único objeto en una línea, así que sale una fila con un array de 18.302 elementos.
2. **Spark ha deducido el esquema** leyendo el fichero: `mag` es `double`, `time` es `long`, `felt` es
   `long` aunque casi siempre sea `null`. Deducirlo exige leer los datos una vez más; en producción se suele
   dar el esquema explícito (lo verás con `from_json`).

`comun.py` es un módulo de la carpeta de prácticas con la sesión configurada como la de la fusión
(`local[2]`, zona horaria UTC, 4 particiones de shuffle) y las rutas de los datos.

## De un array a filas: `explode`

```python
features = crudo.select(F.explode("features").alias("f"))
print("Terremotos:", features.count())

tabla = features.select(
    F.col("f.id").alias("id"),
    F.timestamp_millis(F.col("f.properties.time")).alias("hora"),
    F.col("f.properties.mag").alias("mag"),
    F.col("f.properties.place").alias("lugar"),
    F.col("f.geometry.coordinates")[2].alias("prof"),
)
tabla.show(5, truncate=False)
```

```salida
Terremotos: 18302
+----------+-----------------------+---+-------------------------------------+-------+
|id        |hora                   |mag|lugar                                |prof   |
+----------+-----------------------+---+-------------------------------------+-------+
|us6000pjr6|2025-01-01 00:13:22.65 |4.2|127 km WSW of Merizo Village, Guam   |35.0   |
|us6000pgrt|2025-01-01 01:00:50.839|4.0|228 km SE of Marsaskala, Malta       |10.0   |
|us6000pgru|2025-01-01 01:07:10.492|4.4|256 km SSE of Ambon, Indonesia       |277.355|
|us6000pjqz|2025-01-01 01:46:59.538|4.7|67 km WNW of Rabaul, Papua New Guinea|10.0   |
|us6000pjr1|2025-01-01 02:00:06.918|4.5|71 km WNW of Rabaul, Papua New Guinea|10.0   |
+----------+-----------------------+---+-------------------------------------+-------+
only showing top 5 rows
```

- `F.explode(array)` produce **una fila por elemento**.
- `"f.properties.mag"` navega dentro de los `struct` con puntos.
- `coordinates[2]` toma un elemento del array: en GeoJSON el orden es `[longitud, latitud, profundidad]`.
- `F.timestamp_millis` convierte milisegundos desde 1970 a `timestamp`. La sesión usa UTC, así que las
  horas salen en UTC.

## Filtrar y ordenar

```python
print("M >= 7 en 2025:", tabla.filter(F.col("mag") >= 7).count())
tabla.orderBy(F.col("mag").desc()).show(5, truncate=False)
```

```salida
M >= 7 en 2025: 16
+----------+-----------------------+---+--------------------------------------------+------+
|id        |hora                   |mag|lugar                                       |prof  |
+----------+-----------------------+---+--------------------------------------------+------+
|us6000qw60|2025-07-29 23:24:52.483|8.8|2025 Kamchatka Peninsula, Russia Earthquake |35.0  |
|us7000qx2g|2025-09-18 18:58:14.939|7.8|140 km E of Petropavlovsk-Kamchatsky, Russia|27.0  |
|us7000pn9s|2025-03-28 06:20:52.715|7.7|2025 Mandalay, Burma (Myanmar) Earthquake   |10.0  |
|us7000pcdl|2025-02-08 23:23:14.697|7.6|210 km SSW of George Town, Cayman Islands   |14.326|
|us6000rgf4|2025-10-10 20:29:20.075|7.6|Drake Passage                               |5.639 |
+----------+-----------------------+---+--------------------------------------------+------+
only showing top 5 rows
```

```powershell pruebalo
spark-py 02_dataframes.py 2>$null
```

:::cuidado multiLine
Los ficheros del relleno (`datos/relleno/usgs/*.json`) tienen **un salto de línea por terremoto** dentro
del array. Leídos con `spark.read.json` a secas, cada línea se intenta interpretar como un objeto y sale
una columna `_corrupt_record`. Se leen con `spark.read.option("multiLine", True).json(ruta)`. Lo verás en
la lección 22.
:::

## Las tres formas de escribir una columna

```python
F.col("mag")          # la más explícita; la que usa el repositorio
tabla["mag"]          # atada a un DataFrame concreto (útil en joins con nombres repetidos)
"mag"                 # texto: vale en select, groupBy, orderBy…, pero no para operar (mag >= 7)
```

:::nota En la fusión del observatorio
La fusión nunca lee ficheros: su DataFrame sale de Kafka con columnas `key`, `value`, `topic`,
`partition`, `offset`… Pero las herramientas son las mismas: `select`, `withColumn`, `F.col`.
:::

:::ejercicio Explora el catálogo
Partiendo de `tabla` (o de `terremotos_2025(spark)` en `comun.py`, que ya trae más columnas):

1. ¿Cuántos terremotos de 2025 tienen más de 300 km de profundidad?
2. ¿Cuál fue el más profundo?
3. ¿Cuántos terremotos hay de cada `status` (`automatic`, `reviewed`)? Necesitarás sacar esa columna de
   `properties`.

:::solucion
```python
t = terremotos_2025(spark)
print(t.filter(F.col("prof") > 300).count())
t.orderBy(F.col("prof").desc()).select("id", "hora", "mag", "prof", "lugar").show(1, truncate=False)

crudo.select(F.explode("features").alias("f")) \
     .select("f.id", "f.properties.status") \
     .groupBy("status").count().show()
```

```salida
1102
+----------+-----------------------+---+-------+--------------------------+
|id        |hora                   |mag|prof   |lugar                     |
+----------+-----------------------+---+-------+--------------------------+
|us6000rk66|2025-10-29 17:07:34.157|4.8|669.556|205 km ESE of Levuka, Fiji|
+----------+-----------------------+---+-------+--------------------------+
+--------+-----+
|  status|count|
+--------+-----+
|reviewed|18302|
+--------+-----+
```

1.102 terremotos a más de 300 km, más de dos tercios en la zona de subducción de Fiyi y Tonga; el más
profundo, a 670 km. Y **todos** están revisados: el catálogo se descargó meses después. En directo no es así: un
terremoto llega `automatic` y pasa a `reviewed` en una revisión posterior, que es otra versión del mismo
`id` en Kafka.
:::

```quiz
`spark.read.json` sobre el GeoJSON de 2025 devuelve una sola fila. ¿Por qué?
- Porque Spark solo lee la primera línea de un fichero.
* Porque el fichero es un único objeto JSON en una línea, y `read.json` crea una fila por objeto/línea.
- Porque los 18.302 terremotos están duplicados.
> `read.json` espera JSON Lines. El GeoJSON es un objeto con un array: hay que `explode` el array para tener una fila por terremoto.
```

```quiz
¿Qué devuelve `F.col("f.geometry.coordinates")[1]` en un GeoJSON?
- La longitud.
* La latitud.
- La profundidad.
> GeoJSON ordena las coordenadas como `[longitud, latitud, profundidad]`. Confundirlas pone los terremotos en el lugar equivocado del mapa.
```
