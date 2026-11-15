---
titulo: Columnas, funciones y nulos
resumen: withColumn, when, fechas, nulos que no se rellenan y JSON dentro de una columna, como en la fusión.
bloque: Transformar datos
minutos: 20
practica: Con Docker, sin la pila
---

Casi todo lo que harás con un DataFrame es crear o cambiar columnas con funciones de
`pyspark.sql.functions` (por convención, importado como `F`). Son funciones de Spark: se ejecutan en la
JVM, en paralelo, y el optimizador las entiende. Evita las funciones de Python fila a fila (UDF) mientras
exista una de `F` que haga lo mismo.

## Añadir columnas: `withColumn` y `when`

```python titulo="curso-pyspark/practicas/05_columnas.py"
clasificados = (
    t.withColumn(
        "categoria",
        F.when(F.col("mag") >= 7, "mayor")
        .when(F.col("mag") >= 6, "fuerte")
        .when(F.col("mag") >= 5, "moderado")
        .otherwise("ligero"),
    )
    .withColumn("dia", F.to_date("hora"))
    .withColumn("profundo", F.col("prof") > 300)
)
clasificados.groupBy("categoria").count().orderBy("count").show()
```

```salida
+---------+-----+
|categoria|count|
+---------+-----+
|    mayor|   16|
|   fuerte|  129|
| moderado| 1984|
|   ligero|16173|
+---------+-----+
```

`F.when(...).when(...).otherwise(...)` es el `CASE WHEN` de SQL: la primera condición que se cumple gana.
Sin `otherwise`, lo que no cumple ninguna queda `null`.

Funciones que usarás a menudo:

| Para… | Función |
| --- | --- |
| Convertir tipo | `F.col("x").cast("double")`, `.cast("string")` |
| Milisegundos → fecha y hora | `F.timestamp_millis("time")` |
| Texto ISO → fecha y hora | `F.to_timestamp("time")` |
| Fecha y hora → milisegundos | `F.unix_millis("hora")` |
| Partes de una fecha | `F.to_date`, `F.year`, `F.month`, `F.hour` |
| Primer valor no nulo | `F.coalesce(a, b, …)` |
| Literal | `F.lit(0)` |
| Texto | `F.concat_ws(":", a, b)`, `F.lower`, `F.regexp_extract` |

## Nulos: contar, no rellenar

El catálogo tiene `felt` (cuántas personas dijeron haberlo sentido) casi siempre vacío. Mira cuántos nulos
hay sin cambiarlos:

```python
t.select(
    F.count("*").alias("filas"),
    F.count("mag").alias("con_mag"),
    F.count("sentido").alias("con_sentido"),
    F.sum(F.col("sentido").isNull().cast("int")).alias("sentido_nulo"),
).show()
```

```salida
+-----+-------+-----------+------------+
|filas|con_mag|con_sentido|sentido_nulo|
+-----+-------+-----------+------------+
|18302|  18302|       1923|       16379|
+-----+-------+-----------+------------+
```

- `F.count("*")` cuenta filas; `F.count("col")` cuenta **valores no nulos**.
- Las agregaciones (`avg`, `sum`, `max`) **ignoran los nulos**. Si rellenaras `felt` con 0, la media de
  personas que lo sintieron se desplomaría y sería falsa.

:::defensa La regla del proyecto
«Los nulos no se rellenan con 0.» Una magnitud desconocida no es magnitud 0, igual que una profundidad
desconocida no es «en la superficie». En Spark eso significa: no usar `fillna(0)` ni
`coalesce(col, lit(0))` sobre medidas; contar los nulos y dejarlos fuera de las medias.
:::

## JSON guardado como texto: igual que un mensaje de Kafka

En Kafka el valor de cada mensaje llega como bytes con JSON dentro. Spark tiene dos formas de sacar campos:

### `get_json_object`: un campo por ruta

```python
mensajes = spark.createDataFrame(
    [
        ('{"event": {"id": "us6000pjr6", "properties": {"mag": 4.2, "updated": 1742247237040}}}',),
        ('{"event": {"id": "20250101_0000001", "properties": {"mag": 2.6, "lastupdate": "2025-01-01T18:25:52Z"}}}',),
        ("esto no es JSON",),
    ],
    "valor string",
)
mensajes.select(
    F.get_json_object("valor", "$.event.id").alias("id"),
    F.coalesce(
        F.get_json_object("valor", "$.event.properties.updated"),
        F.get_json_object("valor", "$.event.properties.lastupdate"),
    ).alias("version"),
).show(truncate=False)
```

```salida
+----------------+--------------------+
|id              |version             |
+----------------+--------------------+
|us6000pjr6      |1742247237040       |
|20250101_0000001|2025-01-01T18:25:52Z|
|NULL            |NULL                |
+----------------+--------------------+
```

Devuelve siempre **texto** y, si el JSON está roto o el campo no existe, `null` (sin fallar). Es justo lo
que hace `ultimas_versiones` en la fusión: USGS guarda la versión en `updated` (milisegundos) y EMSC en
`lastupdate` (texto ISO), así que se toma la que exista con `coalesce`.

### `from_json`: el objeto entero con un esquema

```python
esquema = StructType([StructField("event", StructType([
    StructField("id", StringType()),
    StructField("properties", StructType([StructField("mag", DoubleType())])),
]))])
mensajes.select(F.from_json("valor", esquema).alias("m")).select("m.event.id", "m.event.properties.mag").show()
```

```salida
+----------------+----+
|              id| mag|
+----------------+----+
|      us6000pjr6| 4.2|
|20250101_0000001| 2.6|
|            NULL|NULL|
+----------------+----+
```

`from_json` convierte el texto en un `struct` tipado (aquí `mag` ya es `double`) y lee el JSON una vez para
todos los campos. Mejor cuando necesitas muchos campos; `get_json_object` es cómodo para uno o dos.

:::nota Por qué la fusión usa get_json_object (en el observatorio)
`ultimas_versiones` solo necesita dos campos (id y versión) para quedarse con la última versión de cada
informe. El resto del mensaje lo interpreta una función de Python, que conoce las diferencias
entre USGS y EMSC y devuelve un motivo cuando algo no cuadra. Un esquema de `from_json` para dos fuentes
distintas sería más largo y menos claro.
:::

```powershell pruebalo
spark-py 05_columnas.py 2>$null
```

:::ejercicio Ida y vuelta con las fechas
1. Añade una columna `hora_ms` con la hora en milisegundos (`F.unix_millis`) y comprueba que coincide con
   `properties.time` del GeoJSON para `us6000pjr6` (1735690402650).
2. Añade `semestre` (1 o 2) con `F.when` y cuenta terremotos por semestre.

:::solucion
```python
t2 = t.withColumn("hora_ms", F.unix_millis("hora")) \
      .withColumn("semestre", F.when(F.month("hora") <= 6, 1).otherwise(2))
t2.filter(F.col("id") == "us6000pjr6").select("id", "hora", "hora_ms").show(truncate=False)
t2.groupBy("semestre").count().orderBy("semestre").show()
```
`hora_ms` es 1735690402650: la conversión ida y vuelta no pierde nada. Por semestres salen 7.685 y
10.617: el segundo incluye julio, con las réplicas del M 8,8 de Kamchatka (2.684 terremotos M ≥ 4 ese mes,
el doble que un mes normal).
:::

```quiz
Una columna `felt` tiene 16.379 nulos de 18.302 filas. ¿Qué devuelve `F.avg("felt")`?
* La media de los 1.923 valores no nulos.
- La media de las 18.302 filas, contando los nulos como 0.
- `null`, porque hay nulos.
> Las agregaciones ignoran los nulos. Rellenarlos con 0 cambiaría la media y daría un dato falso.
```

```quiz
¿Qué devuelve `F.get_json_object(valor, "$.event.id")` si `valor` no es JSON válido?
- Lanza una excepción y detiene la consulta.
* `null`.
- El texto entero.
> No falla: devuelve `null`. Por eso la fusión agrupa los mensajes sin id por su posición (partición:offset) y deja que Python los descarte con motivo.
```
