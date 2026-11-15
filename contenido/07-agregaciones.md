---
titulo: Agregaciones y joins
resumen: groupBy con varias métricas, filtros por zona y uniones con una tabla pequeña (broadcast).
bloque: Transformar datos
minutos: 18
practica: Con Docker, sin la pila
---

## `groupBy` + `agg`

```python titulo="curso-pyspark/practicas/07_agregaciones.py"
(t.groupBy(F.month("hora").alias("mes"))
  .agg(F.count("*").alias("terremotos"),
       F.round(F.avg("mag"), 2).alias("mag_media"),
       F.max("mag").alias("mag_max"))
  .orderBy("mes")
  .show(12))
```

```salida
+---+----------+---------+-------+
|mes|terremotos|mag_media|mag_max|
+---+----------+---------+-------+
|  1|      1413|      4.5|    7.1|
|  2|      1394|     4.46|    7.6|
|  3|      1178|      4.5|    7.7|
|  4|      1316|     4.51|    6.9|
|  5|      1228|     4.53|    7.4|
|  6|      1156|      4.5|    6.6|
|  7|      2684|     4.55|    8.8|
|  8|      2222|     4.48|    7.5|
|  9|      1604|     4.53|    7.8|
| 10|      1509|     4.54|    7.6|
| 11|      1321|     4.52|    6.8|
| 12|      1277|      4.5|    7.6|
+---+----------+---------+-------+
```

- Se puede agrupar por una **expresión** (`F.month("hora")`), no solo por una columna.
- `agg` acepta varias métricas a la vez: una sola pasada por los datos.
- Julio y agosto disparan el recuento: las réplicas del M 8,8 de Kamchatka (29 de julio).

:::nota Agregar el estado actual, no los mensajes
En el observatorio los recuentos salen de `evento_actual` (una fila por terremoto), **nunca contando
mensajes** de Kafka: un terremoto revisado tres veces son tres mensajes y un solo terremoto. Este catálogo
ya trae una fila por terremoto, así que contar filas es correcto.
:::

## Filtrar por una zona

```python
europa = t.filter(F.col("lat").between(34, 72) & F.col("lon").between(-25, 45))
print("M >= 4 de USGS en la caja de Europa en 2025:", europa.count())
```

```salida
M >= 4 de USGS en la caja de Europa en 2025: 802
```

Las condiciones se combinan con `&` (y), `|` (o) y `~` (no), **cada una entre paréntesis** si no es una
llamada: `(F.col("mag") > 5) & (F.col("prof") < 70)`. Python evalúa `&` antes que `>`, y sin paréntesis
la expresión no significa lo que parece.

Es la caja que usa la fusión para decidir la fuente principal (EMSC dentro, USGS fuera). 802 al año con
M ≥ 4: en el día a día, la sismicidad europea pequeña solo la trae EMSC, que es el motivo de la feature 013.

## Joins

Una tabla pequeña con el nombre de cada red sismológica, unida a los recuentos:

```python
redes = spark.createDataFrame(
    [("us", "USGS NEIC (mundial)"), ("ak", "Alaska"), ("av", "Observatorio volcánico de Alaska"),
     ("ci", "California Sur (Caltech)"), ("nc", "California Norte"), ("pt", "Centro de tsunamis del Pacífico")],
    "red string, nombre string",
)
(t.groupBy("red").count()
  .join(F.broadcast(redes), "red", "left")
  .orderBy(F.col("count").desc())
  .show(6, truncate=False))
```

```salida
+---+-----+------------------------+
|red|count|nombre                  |
+---+-----+------------------------+
|us |18139|USGS NEIC (mundial)     |
|ak |82   |Alaska                  |
|pr |37   |NULL                    |
|nc |20   |California Norte        |
|tx |8    |NULL                    |
|ci |5    |California Sur (Caltech)|
+---+-----+------------------------+
only showing top 6 rows
```

| Tipo | Qué filas quedan |
| --- | --- |
| `inner` (por defecto) | Solo las que tienen pareja en los dos lados. |
| `left` | Todas las de la izquierda; sin pareja, columnas de la derecha a `null` (`pr`, `tx`). |
| `right`, `full` | Lo simétrico, o todas de los dos lados. |
| `left_anti` | Las de la izquierda **sin** pareja (útil para «¿qué redes no tengo nombradas?»). |
| `left_semi` | Las de la izquierda **con** pareja, sin añadir columnas. |

### `broadcast`: no barajar la tabla grande

Un join normal hace **shuffle de los dos lados** para juntar las filas con la misma clave en la misma
partición. Si un lado es pequeño, es más barato **enviarlo entero a cada tarea** y no mover el grande:
eso hace `F.broadcast(redes)`. Spark lo decide solo si la tabla estimada mide menos de
`spark.sql.autoBroadcastJoinThreshold` (10 MB), pero marcarlo deja clara la intención.

:::cuidado Joins por rango
Las uniones por igualdad (`"red"`) son eficientes. Una unión por condición de rango («a menos de 30 s y
100 km») no tiene clave de igualdad, y Spark acaba comparando todo con todo. En la lección 22 verás el
truco para evitarlo con la regla de la fusión.
:::

```powershell pruebalo
spark-py 07_agregaciones.py 2>$null
```

:::ejercicio Redes sin nombre
1. Con un `left_anti`, lista las redes del catálogo que no están en la tabla `redes` y cuántos terremotos
   tienen.
2. ¿Qué tipo de magnitud (`tipo_mag`) es la más frecuente? ¿Y la media de magnitud por tipo?

:::solucion
```python
t.groupBy("red").count().join(redes, "red", "left_anti").orderBy(F.desc("count")).show()
t.groupBy("tipo_mag").agg(F.count("*").alias("n"), F.round(F.avg("mag"), 2).alias("media")) \
 .orderBy(F.desc("n")).show()
```
Entre las redes sin nombre están `pr` (Puerto Rico) y `tx` (Texas). En tipos de magnitud domina `mb`
(ondas de cuerpo, la habitual de USGS para M 4–5,5 lejos de EE. UU.), seguida de `mww` en los grandes: por
eso la fusión **no compara magnitudes** para decidir si dos informes son el mismo terremoto, porque cada
agencia usa escalas distintas.
:::

```quiz
¿Qué hace `F.broadcast(redes)` en un join?
- Copia el resultado del join a todos los ejecutores.
* Envía la tabla pequeña entera a cada tarea para no tener que hacer shuffle de la grande.
- Guarda la tabla en caché en el driver.
> El broadcast join evita el shuffle del lado grande. Solo tiene sentido si el lado difundido cabe holgadamente en memoria.
```

```quiz
`t.filter(F.col("mag") > 5 & F.col("prof") < 70)` da un error raro. ¿Por qué?
* `&` se evalúa antes que `>` y `<`; hacen falta paréntesis en cada condición.
- `filter` no admite dos condiciones.
- `prof` es texto.
> En Python, `&` tiene más prioridad que las comparaciones. Se escribe `(F.col("mag") > 5) & (F.col("prof") < 70)`.
```
