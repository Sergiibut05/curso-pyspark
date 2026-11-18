---
titulo: Particiones, shuffle y el driver
resumen: Cómo se reparten los datos, por qué un fichero de una línea es un problema y qué cuesta collect().
bloque: Rendimiento
minutos: 18
practica: Con Docker, sin la pila
---

Spark es rápido cuando el trabajo se reparte bien. La unidad de reparto es la **partición**: un trozo de
las filas que procesa **una tarea**. Con 2 hilos (`local[2]`), dos tareas van a la vez.

```python preparacion
from pyspark.sql import functions as F

from comun import sesion, terremotos_2025

spark = sesion("particiones")
```

## El fichero que no se puede repartir

```python titulo="curso-pyspark/practicas/12_particiones.py"
t = terremotos_2025(spark)
print("Particiones tras leer:", t.rdd.getNumPartitions())
print("Filas por partición:", t.rdd.glom().map(len).collect())
```

```salida
Particiones tras leer: 2
Filas por partición: [18302, 0]
```

Dos particiones… y **todas las filas en una**. El GeoJSON es un único objeto JSON en una línea: Spark no
puede partir una línea por la mitad, así que una tarea lee el fichero entero y la otra no hace nada.

:::nota Formatos que se reparten bien
JSON Lines (un objeto por línea), CSV y, sobre todo, **Parquet** se pueden partir: cada tarea lee un trozo
del fichero. Un JSON de un solo objeto o un fichero comprimido con gzip no. En un proyecto real, el primer
paso suele ser convertir los datos a Parquet una vez y trabajar desde ahí.
:::

## `repartition` y `coalesce`

```python
r = t.repartition(8, "red")
print("Tras repartition(8, 'red'):", r.rdd.glom().map(len).collect())
print("Tras coalesce(2):", r.coalesce(2).rdd.glom().map(len).collect())
```

```salida
Tras repartition(8, 'red'): [18149, 25, 82, 3, 0, 6, 0, 37]
Tras coalesce(2): [18183, 119]
```

- **`repartition(n, col)`** hace un **shuffle** y reparte por hash de la columna: todas las filas de la
  misma `red` acaban juntas. Como el 99 % son de la red `us`, una partición se lleva casi todo. Es el
  **sesgo** (*skew*): repartir por una clave muy desigual no reparte el trabajo.
- **`repartition(n)`** sin columna reparte al azar y equilibrado (también con shuffle).
- **`coalesce(n)`** junta particiones **sin shuffle**; solo sirve para reducir. Es lo que usarías antes de
  escribir un resultado pequeño en pocos ficheros.

## Las particiones del shuffle

```python
agg = t.groupBy("red").count()
print("Particiones tras groupBy (spark.sql.shuffle.partitions=4):", agg.rdd.getNumPartitions())
agg.explain()
```

```salida
Particiones tras groupBy (spark.sql.shuffle.partitions=4): 1
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage 1
   +- *(2) HashAggregate(keys=[red#16], functions=[count(1)])
      +- AQEShuffleRead coalesced
         +- ShuffleQueryStage 0
            +- Exchange hashpartitioning(red#16, 4), ENSURE_REQUIREMENTS, [plan_id=181]
               +- *(1) HashAggregate(keys=[red#16], functions=[partial_count(1)])
               …
```

Se pidieron 4 particiones de shuffle, pero hay **1**. Lo ha decidido **AQE** (*Adaptive Query Execution*):
después del shuffle vio que el resultado eran unas pocas filas y juntó las particiones
(`AQEShuffleRead coalesced`). Como `getNumPartitions()` necesitó ejecutar la consulta, el plan ya es el
final (`isFinalPlan=true`).

:::nota Por qué la fusión fija 4 (en el observatorio)
El valor por defecto de `spark.sql.shuffle.partitions` es **200**: pensado para clústeres con muchos datos.
Con lotes de unos miles de mensajes, 200 tareas casi vacías por shuffle serían pura sobrecarga.
`crear_sesion` lo baja a 4, el doble de los hilos de `local[2]`.
:::

## El driver no es un ejecutor más

```python
print("take(2):", [f.id for f in t.take(2)])
print("collect() traería", t.count(), "filas al proceso de Python")
```

```salida
take(2): ['us6000pjr6', 'us6000pgrt']
collect() traería 18302 filas al proceso de Python
```

`collect()`, `toPandas()` y similares **traen todas las filas al driver**, que es un solo proceso con
memoria limitada. Con 18.302 filas no pasa nada; con 200 millones, el driver se queda sin memoria.
`take(n)`, `limit(n)` y `show(n)` traen solo `n`.

:::defensa La fusión sí hace collect(), y está bien
`procesar_lote` hace `ultimas_versiones(lote).collect()`: trae el lote al driver porque la lógica de
emparejamiento está en Python puro y consulta PostgreSQL. Es seguro porque el lote está **acotado**:
`maxOffsetsPerTrigger=5000` mensajes como mucho, unos pocos MB. Sin ese tope, el primer lote del relleno
habría intentado traer 227.055 informes de golpe.
:::

```powershell pruebalo
spark-py 12_particiones.py 2>$null
```

:::ejercicio Reparte de verdad
1. Haz `t.repartition(8)` (sin columna) y mira las filas por partición. ¿Está equilibrado?
2. Repite el `groupBy("red").count()` sobre ese DataFrame repartido. ¿Cambia el resultado? ¿Y el plan?

:::solucion
```python
r8 = t.repartition(8)
print(r8.rdd.glom().map(len).collect())
r8.groupBy("red").count().explain()
```

```salida
[2287, 2287, 2288, 2288, 2288, 2288, 2288, 2288]
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[red#16], functions=[count(1)])
   +- Exchange hashpartitioning(red#16, 4), ENSURE_REQUIREMENTS, [plan_id=79]
      +- HashAggregate(keys=[red#16], functions=[partial_count(1)])
         +- Exchange RoundRobinPartitioning(8), REPARTITION_BY_NUM, [plan_id=75]
            +- Project [f#9 AS red#16]
            …
```

El reparto queda equilibrado (18.302 / 8 ≈ 2.288 por partición). El resultado del `groupBy` es idéntico:
las particiones cambian **cómo** se calcula, nunca **qué**. En el plan aparece un `Exchange` más
(`RoundRobinPartitioning(8)`): el `repartition` es un shuffle que pagas antes del de la agregación.
:::

```quiz
Lees un JSON de 2 GB que es un único objeto en una línea, con `local[8]`. ¿Cuántas tareas trabajan al leerlo?
* Una: una línea no se puede partir.
- Ocho, una por hilo.
- Depende de `spark.sql.shuffle.partitions`.
> Spark parte los ficheros de texto por líneas. Un único objeto en una línea va entero a una tarea; por eso se prefieren JSON Lines o Parquet.
```

```quiz
¿Qué diferencia hay entre `repartition(2)` y `coalesce(2)` sobre un DataFrame de 8 particiones?
- Ninguna.
* `repartition` hace shuffle y equilibra; `coalesce` junta particiones vecinas sin shuffle y puede quedar desequilibrado.
- `coalesce` solo funciona con columnas numéricas.
> `coalesce` es barato porque no mueve filas entre tareas por red; `repartition` cuesta un shuffle pero deja particiones parecidas.
```
