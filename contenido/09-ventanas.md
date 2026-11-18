---
titulo: Funciones de ventana
resumen: row_number, lag y rankings por grupo. La ventana exacta que usa la fusión para quedarse con la última versión.
bloque: Transformar datos
minutos: 20
practica: Con Docker, sin la pila
---

Una **función de ventana** calcula algo para cada fila mirando a otras filas de su grupo, **sin juntarlas
en una sola** (como haría `groupBy`). Sirve para «el mayor de cada grupo», «el anterior a este» o «la
última versión de cada cosa».

```python preparacion
from pyspark.sql import Window
from pyspark.sql import functions as F

from comun import sesion, terremotos_2025

spark = sesion("ventanas")
```

Una ventana se define con dos piezas:

```python
from pyspark.sql import Window
ventana = Window.partitionBy("grupo").orderBy(F.col("orden").desc())
```

- `partitionBy`: qué filas forman cada grupo (no confundir con las particiones físicas).
- `orderBy`: en qué orden se recorren dentro del grupo.

## La ventana de la fusión

Un micro-lote de Kafka puede traer **varias versiones del mismo informe** (USGS lo revisó dos veces en un
minuto) e incluso **el mismo mensaje dos veces** (un reenvío del productor). La fusión solo quiere la
última versión de cada uno. Así lo hace su función `ultimas_versiones`, reproducida con un lote de
juguete:

```python titulo="curso-pyspark/practicas/09_ventanas.py"
lote = spark.createDataFrame(
    [("eventos.usgs", 1, 100, "us6000pjr6", "1742247237040"),
     ("eventos.usgs", 1, 101, "us6000pjr6", "1742247297040"),   # revisión posterior
     ("eventos.usgs", 1, 102, "us6000pjr6", "1742247297040"),   # reenvío de la misma versión
     ("eventos.usgs", 0, 55, "us6000pgrt", "1742247000000"),
     ("eventos.emsc", 2, 7, "20250101_0000001", "2025-01-01T18:25:52.893201Z")],
    "topic string, partition int, offset long, id string, version string",
)
v = Window.partitionBy("topic", "id").orderBy(F.col("version").desc_nulls_last(), F.col("offset").desc())
lote.withColumn("n", F.row_number().over(v)).show(truncate=False)
```

```salida
+------------+---------+------+----------------+---------------------------+---+
|topic       |partition|offset|id              |version                    |n  |
+------------+---------+------+----------------+---------------------------+---+
|eventos.emsc|2        |7     |20250101_0000001|2025-01-01T18:25:52.893201Z|1  |
|eventos.usgs|0        |55    |us6000pgrt      |1742247000000              |1  |
|eventos.usgs|1        |102   |us6000pjr6      |1742247297040              |1  |
|eventos.usgs|1        |101   |us6000pjr6      |1742247297040              |2  |
|eventos.usgs|1        |100   |us6000pjr6      |1742247237040              |3  |
+------------+---------+------+----------------+---------------------------+---+
```

Y quedándose con `n = 1`:

```salida
+------------+---------+------+----------------+---------------------------+
|topic       |partition|offset|id              |version                    |
+------------+---------+------+----------------+---------------------------+
|eventos.emsc|2        |7     |20250101_0000001|2025-01-01T18:25:52.893201Z|
|eventos.usgs|0        |55    |us6000pgrt      |1742247000000              |
|eventos.usgs|1        |102   |us6000pjr6      |1742247297040              |
+------------+---------+------+----------------+---------------------------+
```

Cada decisión del código tiene su motivo:

| Detalle | Por qué |
| --- | --- |
| `partitionBy("topic", "id")` | Un id solo es único **dentro de su fuente**: el mismo texto en USGS y en EMSC serían informes distintos. |
| `version` como **texto** | USGS da milisegundos (13 cifras) y EMSC texto ISO. Dentro de una misma fuente ambos se ordenan bien como texto; no hace falta convertirlos. |
| `desc_nulls_last()` | Un mensaje sin versión legible va al final y no gana a uno bueno. |
| Desempate por `offset` desc | Con la misma versión (el reenvío), gana el último escrito. El resultado es **determinista**: la misma entrada, el mismo resultado. |
| `row_number` y no `rank` | `rank` daría 1 a los dos empatados y pasarían ambos. `row_number` numera 1, 2, 3 sin empates. |

:::nota Los mensajes sin id (en el observatorio)
En la fusión, un mensaje con JSON roto no tiene id. Si se agrupara por `id = null`, todos los rotos del
lote caerían en el mismo grupo y solo sobreviviría uno. Por eso `ultimas_versiones` agrupa por
`coalesce(id, "partición:offset")`: cada mensaje roto va solo, llega a Python y se descarta con su motivo.
:::

## El mayor terremoto de cada mes

```python
t = terremotos_2025(spark)
por_mes = Window.partitionBy(F.month("hora")).orderBy(F.col("mag").desc(), "hora")
(t.withColumn("puesto", F.row_number().over(por_mes)).filter("puesto = 1")
  .select(F.month("hora").alias("mes"), "mag", "lugar").orderBy("mes").show(12, truncate=False))
```

```salida
+---+---+--------------------------------------------+
|mes|mag|lugar                                       |
+---+---+--------------------------------------------+
|1  |7.1|2025 Southern Tibetan Plateau Earthquake    |
|2  |7.6|210 km SSW of George Town, Cayman Islands   |
|3  |7.7|2025 Mandalay, Burma (Myanmar) Earthquake   |
|4  |6.9|Reykjanes Ridge                             |
|5  |7.4|2025 Drake Passage Earthquake               |
|6  |6.6|Scotia Sea                                  |
|7  |8.8|2025 Kamchatka Peninsula, Russia Earthquake |
|8  |7.5|2025 Southern Drake Passage Earthquake      |
|9  |7.8|140 km E of Petropavlovsk-Kamchatsky, Russia|
|10 |7.6|Drake Passage                               |
|11 |6.8|122 km E of Yamada, Japan                   |
|12 |7.6|2025 Aomori Prefecture, Japan Earthquake    |
+---+---+--------------------------------------------+
```

Con `groupBy(mes).agg(max("mag"))` tendrías la magnitud máxima, pero **no el resto de la fila** (el lugar).
La ventana conserva la fila entera.

## Mirar a la fila anterior: `lag`

Las réplicas de Kamchatka en las dos primeras horas, con los minutos desde la anterior:

```python
kam = t.filter(F.col("lat").between(50, 56) & F.col("lon").between(155, 165)
               & (F.col("hora") >= "2025-07-29 23:00:00") & (F.col("hora") < "2025-07-30 01:00:00"))
orden = Window.orderBy("hora")
(kam.withColumn("min_desde_anterior",
                F.round((F.unix_timestamp("hora") - F.unix_timestamp(F.lag("hora").over(orden))) / 60, 1))
    .select("hora", "mag", "min_desde_anterior").show(8, truncate=False))
```

```salida
+-----------------------+---+------------------+
|hora                   |mag|min_desde_anterior|
+-----------------------+---+------------------+
|2025-07-29 23:24:52.483|8.8|NULL              |
|2025-07-29 23:30:03.032|6.3|5.2               |
|2025-07-29 23:30:44.566|6.1|0.7               |
|2025-07-29 23:31:25.02 |5.9|0.7               |
|2025-07-29 23:34:53.625|5.9|3.5               |
|2025-07-29 23:36:01.341|6.0|1.1               |
|2025-07-29 23:37:34.403|6.0|1.6               |
|2025-07-29 23:38:28.604|5.7|0.9               |
+-----------------------+---+------------------+
only showing top 8 rows
Réplicas M >= 4 en las dos primeras horas (en la caja): 63
```

Réplicas de magnitud 6 cada minuto. Muchas están a menos de 30 s y 100 km de otra **de la misma fuente**:
por eso la regla de la fusión prohíbe unir dos informes de la misma fuente, aunque estén cerca.

:::cuidado Una ventana sin partitionBy
`Window.orderBy("hora")` sin `partitionBy` mete **todas las filas en una sola partición** para ordenarlas.
Con 63 filas da igual; con millones, una sola tarea hace todo el trabajo y puede quedarse sin memoria.
Filtra antes (como aquí) o añade un `partitionBy` con sentido.
:::

```powershell pruebalo
spark-py 09_ventanas.py 2>$null
```

:::ejercicio Los tres mayores de cada red
Con una ventana por `red`, saca los **tres** terremotos de mayor magnitud de cada red (las que tengan al
menos uno). ¿Qué cambia si usas `rank()` en lugar de `row_number()`?

:::solucion
```python
v = Window.partitionBy("red").orderBy(F.col("mag").desc())
(t.withColumn("puesto", F.row_number().over(v)).filter("puesto <= 3")
  .select("red", "puesto", "mag", "lugar").orderBy("red", "puesto").show(30, truncate=False))
```
Con `rank()`, dos terremotos con la misma magnitud comparten puesto y el siguiente salta (1, 2, 2, 4): una
red puede devolver más de tres filas. `dense_rank()` no salta (1, 2, 2, 3). Para «uno por grupo» sin
ambigüedad, `row_number()` con un desempate determinista.
:::

```quiz
En `ultimas_versiones`, ¿por qué se desempata por `offset` descendente?
- Para que el resultado salga ordenado por offset.
* Para que, con la misma versión repetida, gane siempre el mismo mensaje (el último escrito) y el resultado sea determinista.
- Porque Kafka no garantiza que los offsets sean únicos.
> Sin desempate, entre dos filas iguales `row_number` podría elegir una u otra según cómo lleguen. La fusión necesita que la misma entrada produzca exactamente la misma salida.
```

```quiz
¿Qué diferencia hay entre `groupBy("red").agg(F.max("mag"))` y una ventana con `row_number` por `red`?
* `groupBy` devuelve una fila por red solo con el máximo; la ventana conserva todas las columnas de la fila ganadora.
- Ninguna, dan lo mismo.
- La ventana es siempre más rápida.
> La agregación resume; la ventana anota cada fila y luego puedes filtrar la que quieras sin perder sus otras columnas.
```
