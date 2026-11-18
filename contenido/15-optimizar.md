---
titulo: Optimizar - leer menos, mover menos, repartir mejor
resumen: Filtros y columnas que Parquet se salta, el problema de los ficheros pequeños, el sesgo de datos, la técnica de la sal y cómo AQE parte un join sesgado.
bloque: Rendimiento
minutos: 30
practica: si
---

Casi toda la optimización en Spark se reduce a tres ideas:

1. **Leer menos**: no leer filas ni columnas que no hacen falta.
2. **Mover menos**: evitar shuffles, o que muevan pocos datos.
3. **Repartir mejor**: que el trabajo de cada tarea sea parecido.

Con el plan y la interfaz web (lección 14) ves dónde falla cada una. Esta lección es el «qué hacer».

```python preparacion
import os
import time

from pyspark.sql import functions as F

from comun import SALIDA, sesion, terremotos_2025

spark = sesion("optimizar")
t = terremotos_2025(spark)
t.write.mode("overwrite").parquet(f"{SALIDA}/opt/terremotos")   # el Parquet que lee el primer bloque


def detalle(df, clave):
    """El trozo del plan físico que empieza por clave (PushedFilters:, ReadSchema:…)."""
    plan = df._jdf.queryExecution().executedPlan().toString()
    linea = next(l for l in plan.splitlines() if clave in l)
    trozo = linea[linea.index(clave):]
    return trozo[:trozo.find("]", len(clave)) + 1] if "]" in trozo else trozo[:100]


# Sesgo: 2 millones de filas, el 90 % con la misma clave
datos = spark.range(2_000_000).select(
    F.when(F.col("id") % 10 < 9, F.lit("us")).otherwise(F.concat(F.lit("red"), (F.col("id") % 50).cast("string"))).alias("red"),
    F.col("id"))
# La tabla de redes del join sesgado, leída de Parquet
datos.select("red").distinct().withColumn("nombre", F.concat(F.lit("Red "), "red")) \
     .write.mode("overwrite").parquet(f"{SALIDA}/opt/redes")
redes = spark.read.parquet(f"{SALIDA}/opt/redes")
```

## Leer menos: Parquet se salta lo que no pides

```python titulo="practicas/15_optimizar.py (extracto)"
pq = spark.read.parquet(f"{SALIDA}/opt/terremotos")
consulta = pq.filter("mag >= 7").select("id", "mag")
print("PushedFilters:", detalle(consulta, "PushedFilters:"))
print("ReadSchema   :", detalle(consulta, "ReadSchema:"))
print("M >= 7:", consulta.count())
```

```powershell pruebalo
spark-py 15_optimizar.py 2>$null
```

```salida
PushedFilters: PushedFilters: [IsNotNull(mag), GreaterThanOrEqual(mag,7.0)]
ReadSchema   : ReadSchema: struct<id:string,mag:double>
M >= 7: 16
```

Dos optimizaciones que Spark aplica solo, y que se ven en el nodo `FileScan` del plan:

- **Filtros empujados** (*predicate pushdown*, `PushedFilters`): el filtro `mag >= 7` se pasa al lector de
  Parquet. Parquet guarda, por cada bloque de filas (*row group*), el mínimo y el máximo de cada columna:
  si un bloque tiene `mag` máxima 6,2, ni se lee.
- **Poda de columnas** (*column pruning*, `ReadSchema`): de las 10 columnas solo se leen `id` y `mag`.
  Como Parquet guarda por columnas, las otras ocho ni se tocan.

Con JSON o CSV no hay nada de esto: hay que leer y analizar cada línea entera. Y la **poda de particiones**
de la lección 3 (`partitionBy` y filtrar por esa columna) se salta carpetas enteras. Juntas, las tres
deciden si una consulta lee 2 GB o 200 MB de un histórico de 2 TB.

:::defensa Lo que más ayuda
Guardar en **Parquet** (o Delta/Iceberg, que son Parquet con más cosas), **particionado** por la columna con
la que más se filtra (normalmente la fecha), y en las consultas, **filtrar y seleccionar lo antes
posible**. Catalyst empuja muchos filtros solo, pero no puede empujar a través de una UDF (lección 11).
:::

## El problema de los ficheros pequeños

Los mismos datos, escritos en 1 fichero y en 200:

```salida
  1 ficheros (0.80 MB): leer y filtrar 0.27 s, 1 particiones al leer
200 ficheros (1.74 MB): leer y filtrar 7.36 s, 7 particiones al leer
```

- **Ocupa el doble**: cada fichero Parquet lleva su cabecera, sus metadatos y su esquema, y comprime peor
  con pocas filas.
- **Es mucho más lento**: cada fichero hay que listarlo, abrirlo y leer su pie de metadatos. Aquí la
  diferencia se exagera porque `practicas/` está montada desde Windows en el contenedor y abrir un fichero
  es especialmente lento; en S3 o HDFS cada fichero también es una petición con su latencia, y con cientos
  de miles de ficheros el coste es el mismo problema a otra escala.
- Spark **agrupa** ficheros pequeños al leer (`spark.sql.files.maxPartitionBytes`): 200 ficheros acaban en 7
  particiones. Ayuda, pero no evita abrirlos.

De dónde salen los ficheros pequeños: escribir con muchas particiones (cada partición del DataFrame es un
fichero), `partitionBy` por una columna con muchos valores, y procesos en streaming que escriben un
puñado de filas en cada micro-lote. Remedios:

| Remedio | Cómo |
| --- | --- |
| Menos particiones antes de escribir | `df.coalesce(n)` (sin shuffle) o `df.repartition(n)` (con shuffle, equilibrado). |
| Ficheros de tamaño razonable | Apunta a **100 MB – 1 GB por fichero**. `maxRecordsPerFile` pone un tope. |
| Compactar | Un proceso periódico que relee una carpeta y la reescribe en pocos ficheros. Delta e Iceberg lo traen (`OPTIMIZE`). |

## Repartir mejor: el sesgo

**Sesgo** (*skew*) es que una clave tenga muchísimas más filas que las demás. En un `groupBy` o un join por
esa clave, todas sus filas acaban en **una** partición, que procesa **una** tarea. Las otras terminan
enseguida y la etapa entera espera a la lenta. El script crea 2 millones de filas donde el 90 % son de la
red `us`:

```salida
filas por partición al repartir por red: [1800000, 40000, 80000, 40000, 40000]
```

Una partición con 1,8 millones y el resto con 40.000. (Son 8 particiones; las vacías no salen.) En la
interfaz web se ve como una tarea cuyo tiempo está muy por encima de la mediana (lección 14).

## La sal

La solución clásica: añadir a la clave caliente un número aleatorio (la **sal**) para partirla en varias
subclaves, agregar por `(clave, sal)` y luego volver a agregar por `clave`:

```python
SAL = 8
con_sal = datos.withColumn("sal", (F.rand(seed=1) * SAL).cast("int"))
resultado = (con_sal.groupBy("red", "sal").agg(F.count("*").alias("n"))   # el trabajo pesado, repartido
             .groupBy("red").agg(F.sum("n").alias("n")))                 # combinar: pocas filas

por_particion = con_sal.repartition(8, "red", "sal").groupBy(F.spark_partition_id().alias("p")).count().orderBy("p")
print("con sal                               :", [r["count"] for r in por_particion.collect()])
print("recuento de 'us' con sal:", resultado.orderBy(F.desc("n")).first()["n"])
```

```salida
con sal                               : [35028, 474871, 29957, 19916, 460939, 25307, 244926, 709056]
recuento de 'us' con sal: 1800000
```

La partición más grande baja de 1,8 millones a 709.000 y el resultado es el mismo. No queda perfectamente
equilibrado: las 8 subclaves de `us` se reparten con un hash entre 8 particiones y varias caen en la misma.
Con más sal (32, 64) o más particiones se reparte mejor, a cambio de más filas en el segundo paso.

Para un **join**, la sal es más laboriosa: se añade la sal al lado grande y se **replica** el lado pequeño
una vez por cada valor de sal, para que cada subclave encuentre su pareja.

## AQE parte los joins sesgados

Para los joins, la ejecución adaptativa lo hace sola (`spark.sql.adaptive.skewJoin.enabled`, activada por
defecto): tras el shuffle, si una partición es mucho mayor que la mediana (por defecto, 5 veces y más de
256 MB), la **parte** en trozos y replica la parte correspondiente del otro lado. El script baja los umbrales
para que se active con 2 millones de filas:

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")          # que no lo resuelva con broadcast
spark.conf.set("spark.sql.shuffle.partitions", "50")                  # con 4, la caliente no destaca
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256KB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256KB")
consulta = datos.join(redes, "red").groupBy("nombre").agg(F.sum("id").alias("suma"))
consulta.collect()
plan = consulta._jdf.queryExecution().executedPlan().toString().split("== Initial Plan ==")[0]
for linea in plan.splitlines():
    if "SortMergeJoin" in linea or ("AQEShuffleRead" in linea and "skewed" in linea):
        print("plan final:", linea.strip()[linea.strip().find("SortMergeJoin") if "SortMergeJoin" in linea else linea.strip().find("AQE"):][:45])
```

```salida
plan final: SortMergeJoin(skew=true) [red#102], [red#139]
plan final: AQEShuffleRead coalesced and skewed
```

`skew=true` en el join y `coalesced and skewed` en la lectura del shuffle: AQE ha juntado las particiones
pequeñas **y** partido la grande.

:::nota Lo que costó ver esto
Las primeras versiones de este ejemplo no activaban la optimización, por dos motivos que te vas a encontrar:

- Al otro lado del join había una agregación entre el shuffle y el join. AQE solo puede partir particiones
  cuando el join lee **directamente** de los dos shuffles.
- La partición caliente solo llevaba el texto `us`, que comprimido en el shuffle ocupaba muy poco y no
  llegaba al umbral. El sesgo se mide en **bytes**, no en filas. Por eso la consulta suma `id`, que obliga
  a llevarlo por el shuffle.
:::

## Lista rápida

| Síntoma | Causa probable | Qué probar |
| --- | --- | --- |
| Lee muchos más datos de los necesarios | JSON/CSV, sin particionar, filtros tarde, UDF en el filtro. | Parquet, `partitionBy`, filtrar antes, quitar la UDF del filtro. |
| Una tarea tarda mucho más que el resto | Sesgo. | AQE (joins), sal (agregaciones), filtrar la clave caliente y tratarla aparte. |
| Miles de tareas casi vacías | Demasiadas particiones de shuffle. | AQE activada; `spark.sql.shuffle.partitions` razonable. |
| Shuffle enorme en un join con una tabla pequeña | Spark no sabe que es pequeña. | `F.broadcast`, o leerla de un formato con estadísticas (lección 8). |
| Miles de ficheros diminutos | Muchas particiones al escribir o `partitionBy` demasiado fino. | `coalesce`/`repartition` antes de escribir; compactar. |
| Se queda sin memoria | `collect`, `toPandas`, grupos enormes en `applyInPandas`, broadcast de algo grande. | Acotar lo que va al driver; no forzar broadcast de tablas que crecen. |
| Recalcula lo mismo varias veces | Varias acciones sobre un DataFrame caro. | `cache()` (lección 13). |

:::ejercicio Mide la poda de columnas
1. Escribe los terremotos de 2025 en Parquet y en JSON Lines.
2. Cuenta `filter("mag >= 6")` leyendo cada formato, y mira en el plan (`explain`) el `ReadSchema` de cada
   uno.
3. Haz lo mismo seleccionando solo `id` y `mag`. ¿Cambia el `ReadSchema` del JSON?

:::solucion
```python
t.write.mode("overwrite").parquet(f"{SALIDA}/ej15/pq")
t.write.mode("overwrite").json(f"{SALIDA}/ej15/json")
for fmt in ("pq", "json"):
    df = spark.read.format("parquet" if fmt == "pq" else "json").load(f"{SALIDA}/ej15/{fmt}")
    q = df.filter("mag >= 6").select("id", "mag")
    plan = q._jdf.queryExecution().executedPlan().toString()
    print(fmt, q.count(), [l[l.find("ReadSchema"):][:70] for l in plan.splitlines() if "ReadSchema" in l])
```

SOLUCION_15
:::

```quiz
Lees Parquet, filtras `mag >= 7` y te quedas con dos columnas. ¿Qué lee Spark del disco?
- Todas las filas y todas las columnas, y luego filtra.
* Solo esas dos columnas, y se salta los bloques de filas cuyo máximo de `mag` es menor que 7.
- Solo las filas con `mag >= 7`, pero con todas las columnas.
> Poda de columnas (ReadSchema) y filtros empujados (PushedFilters), gracias al formato por columnas y sus estadísticas.
```

```quiz
Una etapa de 200 tareas: 199 tardan 2 s y una tarda 10 minutos. ¿Qué pasa?
- Faltan ejecutores.
* Sesgo: una clave concentra casi todas las filas en una partición.
- El shuffle está mal configurado.
> La etapa termina cuando termina su tarea más lenta.
```

```quiz
¿Por qué la sal resuelve el sesgo en un groupBy?
* Porque parte la clave caliente en varias subclaves que se procesan en tareas distintas, y luego se combinan.
- Porque elimina filas repetidas.
- Porque obliga a Spark a usar broadcast.
> Agregar por (clave, sal) reparte el trabajo pesado; agregar después por clave combina pocos resultados.
```
