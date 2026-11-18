---
titulo: Tipos complejos - struct, array y map
resumen: Datos anidados como vienen de las APIs. Navegarlos, construirlos, explotarlos, trabajar dentro de un array sin explotarlo y leer JSON con un esquema.
bloque: Transformar datos
minutos: 25
practica: si
---

Una tabla de una hoja de cálculo es plana: cada celda, un número o un texto. Los datos que llegan de APIs y
de Kafka casi nunca lo son: el GeoJSON de USGS tiene objetos dentro de objetos y listas de coordenadas.
Spark los representa con tres **tipos complejos**:

| Tipo | Qué es | Ejemplo en el GeoJSON | Se accede con |
| --- | --- | --- | --- |
| `struct` | Un registro con campos con nombre (como un objeto JSON o un diccionario con claves fijas). | `properties`, `geometry` | `col("properties.mag")` |
| `array` | Una lista de valores **del mismo tipo**. | `geometry.coordinates` (`[lon, lat, prof]`) | `col("coords")[2]` |
| `map` | Pares clave-valor con claves **variables**. | — (lo construiremos) | `col("etiquetas")["estado"]` |

Se pueden anidar sin límite: un array de structs, un struct con un map dentro…

```python preparacion
from pyspark.sql import functions as F

from comun import USGS_2025, sesion

spark = sesion("tipos-complejos")
f = spark.read.json(USGS_2025).select(F.explode("features").alias("f")).select("f.*")
# Dos terremotos como texto JSON, como el valor de un mensaje de Kafka (lo usa el bloque de from_json)
texto = f.select(F.to_json(F.struct("id", F.col("properties.mag").alias("mag"),
                                    F.col("geometry.coordinates").alias("coord"))).alias("json")).limit(2)
```

## Navegar

```python titulo="practicas/06_tipos_complejos.py (extracto)"
f = spark.read.json(USGS_2025).select(F.explode("features").alias("f")).select("f.*")
f.select(
    "id",
    F.col("properties.mag").alias("mag"),                       # struct: punto
    F.col("geometry.coordinates")[2].alias("prof"),            # array: [n], empieza en 0
    F.element_at("geometry.coordinates", -1).alias("ultimo"),   # element_at: empieza en 1; -1 es el último
    F.size("geometry.coordinates").alias("n"),
).show(3)
```

```powershell pruebalo
spark-py 06_tipos_complejos.py 2>$null
```

```salida
properties: struct | geometry: struct | coordinates: array<double>
+----------+---+-------+-------+---+
|        id|mag|   prof| ultimo|  n|
+----------+---+-------+-------+---+
|us6000pjr6|4.2|   35.0|   35.0|  3|
|us6000pgrt|4.0|   10.0|   10.0|  3|
|us6000pgru|4.4|277.355|277.355|  3|
+----------+---+-------+-------+---+
```

`select("f.*")` saca todos los campos de un struct como columnas. Y cuidado con los índices: `[n]`
empieza en **0** (como en Python) y `element_at` en **1** (como en SQL). Un índice fuera de rango da `null`
con `[n]` y error con `element_at` (con el modo ANSI de Spark 4).

## Construir

```python
compacto = f.select(
    "id",
    F.struct(F.col("properties.mag").alias("valor"), F.col("properties.magType").alias("tipo")).alias("magnitud"),
    F.create_map(F.lit("red"), F.col("properties.net"), F.lit("estado"), F.col("properties.status")).alias("etiquetas"),
)
compacto.printSchema()
compacto.select("id", "magnitud.valor", F.col("etiquetas")["estado"].alias("estado")).show(3)
```

```salida
root
 |-- id: string (nullable = true)
 |-- magnitud: struct (nullable = false)
 |    |-- valor: double (nullable = true)
 |    |-- tipo: string (nullable = true)
 |-- etiquetas: map (nullable = false)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)

+----------+-----+--------+
|        id|valor|  estado|
+----------+-----+--------+
|us6000pjr6|  4.2|reviewed|
|us6000pgrt|  4.0|reviewed|
|us6000pgru|  4.4|reviewed|
+----------+-----+--------+
```

¿Struct o map? Si los campos son **fijos y conocidos**, struct: cada campo tiene su tipo y Spark sabe qué
hay. Si las claves **varían** de una fila a otra (etiquetas libres, atributos opcionales), map. También
existe `F.array(c1, c2, …)` para juntar columnas en una lista.

## De texto a array, y de array a filas

USGS anota qué **productos** ha publicado para cada terremoto en un texto: `",origin,phase-data,"`. Con
`split` se convierte en array, y con `explode` en **una fila por elemento**:

```python
tipos = f.select("id", F.split(F.trim(F.col("properties.types"), F.lit(",")), ",").alias("productos"))
(tipos.select(F.explode("productos").alias("producto")).groupBy("producto").count()
 .orderBy(F.desc("count")).show(6))
```

```salida
+----------+--------------------+
|id        |productos           |
+----------+--------------------+
|us6000pjr6|[origin, phase-data]|
|us6000pgrt|[origin, phase-data]|
+----------+--------------------+

+--------------------+-----+
|            producto|count|
+--------------------+-----+
|              origin|18302|
|          phase-data|18302|
|                dyfi| 1923|
|       moment-tensor| 1453|
|internal-moment-t...| 1187|
|            shakemap| 1167|
+--------------------+-----+
```

Todos tienen `origin` y `phase-data`; 1.923 tienen `dyfi` («Did You Feel It?», los informes de la gente que
lo sintió) y 1.167 un mapa de sacudida (`shakemap`).

:::cuidado F.trim(col, ",") no hace lo que parece
La primera versión de este script fue `F.trim(F.col("properties.types"), ",")` y falló con
`UNRESOLVED_COLUMN … name ',' cannot be resolved`. En PySpark 4, el segundo argumento de `trim` es una
**columna**, así que `","` se interpreta como el nombre de una columna llamada `,`. Para un valor fijo:
`F.lit(",")`. Muchas funciones de `F` aceptan texto como nombre de columna: si quieres un literal, `lit`.
:::

Variantes de `explode`:

| Función | Diferencia |
| --- | --- |
| `explode` | Una fila por elemento. **Las filas con array vacío o nulo desaparecen.** |
| `explode_outer` | Igual, pero conserva esas filas con `null`. |
| `posexplode` | Añade la posición de cada elemento. |
| `inline` | Para un array de structs: una fila por struct y una columna por campo. |

## Trabajar dentro del array sin explotarlo

Explotar multiplica las filas y luego hay que volver a agrupar. Para operar **dentro** de cada array,
Spark tiene **funciones de orden superior**, que reciben una función (una `lambda` de Python que se traduce
a una expresión de Spark, no se ejecuta en Python):

```python
(tipos.filter(F.array_contains("productos", "shakemap")).select(
    "id",
    F.size("productos").alias("n"),
    F.filter("productos", lambda p: p.startswith("shake")).alias("shake"),
    F.transform("productos", lambda p: F.length(p)).alias("longitudes"),
    F.exists("productos", lambda p: p == "dyfi").alias("tiene_dyfi"),
).show(3, truncate=False))
```

```salida
+----------+---+-----------------------+---------------------------------------------------+----------+
|id        |n  |shake                  |longitudes                                         |tiene_dyfi|
+----------+---+-----------------------+---------------------------------------------------+----------+
|us6000pgsd|7  |[shakemap]             |[4, 22, 9, 13, 6, 10, 8]                           |true      |
|us6000pgv4|8  |[shakemap]             |[14, 22, 15, 9, 13, 6, 10, 8]                      |false     |
|nc75111126|14 |[shake-alert, shakemap]|[4, 14, 15, 14, 11, 9, 13, 13, 3, 6, 10, 12, 11, 8]|true      |
+----------+---+-----------------------+---------------------------------------------------+----------+
```

| Función | Hace |
| --- | --- |
| `transform(arr, f)` | Aplica `f` a cada elemento (el `map` de Python). |
| `filter(arr, f)` | Se queda con los elementos que cumplen `f`. |
| `exists` / `forall` | ¿Alguno / todos cumplen `f`? |
| `aggregate(arr, inicial, f)` | Reduce el array a un valor (sumar, máximo…). |
| `array_contains`, `array_distinct`, `array_sort`, `array_union`… | Las operaciones de conjuntos y orden habituales. |

## JSON dentro de una columna

El valor de un mensaje de Kafka es un JSON en una columna de texto (lección 17). `from_json` lo convierte en
un struct si le das el **esquema**:

```python
esquema = "id STRING, mag DOUBLE, coord ARRAY<DOUBLE>"
texto.select(F.from_json("json", esquema).alias("d")).select("d.id", "d.mag", F.col("d.coord")[2].alias("prof")).show()
```

```salida
+-------------------------------------------------------------+
|json                                                         |
+-------------------------------------------------------------+
|{"id":"us6000pjr6","mag":4.2,"coord":[143.6244,12.7441,35.0]}|
|{"id":"us6000pgrt","mag":4.0,"coord":[16.5823,34.6287,10.0]} |
+-------------------------------------------------------------+

+----------+---+----+
|        id|mag|prof|
+----------+---+----+
|us6000pjr6|4.2|35.0|
|us6000pgrt|4.0|10.0|
+----------+---+----+

esquema deducido de un ejemplo: STRUCT<coord: ARRAY<DOUBLE>, id: STRING, mag: DOUBLE>
```

- `to_json` hace lo contrario: de struct a texto (así se construye el valor de un mensaje para escribirlo
  en Kafka).
- `schema_of_json` deduce el esquema de **un** ejemplo. Útil para escribir el esquema la primera vez; no
  para producción, porque un ejemplo no tiene por qué tener todos los campos.
- Si el JSON no encaja con el esquema, `from_json` devuelve `null` (o los campos que pueda), no falla.
- Para sacar uno o dos campos sin declarar nada, `get_json_object(col, "$.ruta")` (lección 5).

:::ejercicio Las coordenadas como struct
1. A partir de `f`, construye una columna `pos` de tipo struct con los campos `lon`, `lat` y `prof` sacados
   de `geometry.coordinates`.
2. Con una función de orden superior y sin `explode`, calcula para cada terremoto cuántos productos tienen
   la palabra `moment` en el nombre.
3. ¿Cuántos terremotos tienen al menos uno?

:::solucion
```python
con_pos = f.select("id", F.struct(
    F.col("geometry.coordinates")[0].alias("lon"),
    F.col("geometry.coordinates")[1].alias("lat"),
    F.col("geometry.coordinates")[2].alias("prof")).alias("pos"))
con_pos.select("id", "pos.prof").show(2)

momento = tipos.select("id", F.size(F.filter("productos", lambda p: p.contains("moment"))).alias("n_moment"))
print(momento.filter("n_moment > 0").count())
```

SOLUCION_06
:::

```quiz
Las etiquetas de cada terremoto varían: unos tienen `alerta`, otros `tsunami`, otros nada. ¿Qué tipo usarías?
- struct
- array
* map
> Un map admite claves distintas en cada fila. Un struct tiene los mismos campos en todas.
```

```quiz
Haces `explode` sobre una columna de arrays y desaparecen filas. ¿Por qué?
* Porque `explode` elimina las filas cuyo array está vacío o es nulo; `explode_outer` las conserva.
- Porque `explode` deduplica.
- Porque falta un `groupBy`.
> Un array vacío no tiene elementos de los que hacer filas.
```

```quiz
¿Qué devuelve `from_json` si el texto no cumple el esquema?
- Lanza un error y para el trabajo.
* `null` (o los campos que pueda leer): no falla.
- El texto original.
> Por eso conviene contar los nulos después de un from_json para detectar mensajes que no encajan.
```
