---
titulo: Joins a fondo
resumen: Los seis tipos de join con cada fila a la vista, los nulos en la clave, las claves repetidas que multiplican filas, las columnas duplicadas y cómo decide Spark entre broadcast y sort-merge.
bloque: Transformar datos
minutos: 25
practica: si
---

Un **join** combina dos tablas por una clave: a cada terremoto, el nombre de su red sismológica. Es la
operación que más errores silenciosos produce (filas que desaparecen o se duplican sin avisar) y una de
las más caras. En la lección 7 viste uno sencillo; aquí, a fondo.

## Dos tablas para verlo todo

```python titulo="practicas/08_joins.py (extracto)"
sismos = spark.createDataFrame(
    [("s1", "us", 6.1), ("s2", "ak", 4.3), ("s3", "pr", 4.0), ("s4", "xx", 5.0), ("s5", None, 4.8)],
    "id STRING, red STRING, mag DOUBLE")
redes = spark.createDataFrame(
    [("us", "USGS NEIC"), ("ak", "Alaska"), ("nc", "California norte"), (None, "sin red")],
    "red STRING, nombre STRING")
for tipo in ("inner", "left", "right", "full", "left_semi", "left_anti"):
    filas = sismos.join(redes, "red", tipo).orderBy("red", "id").collect()
```

`us` y `ak` están en las dos; `pr` y `xx` solo en `sismos`; `nc` solo en `redes`; y cada tabla tiene una
fila con la red nula.

```powershell pruebalo
spark-py 08_joins.py 2>$null
```

```salida
inner      2 filas: [('ak', 's2', 4.3, 'Alaska'), ('us', 's1', 6.1, 'USGS NEIC')]
left       5 filas: [(None, 's5', 4.8, None), ('ak', 's2', 4.3, 'Alaska'), ('pr', 's3', 4.0, None), ('us', 's1', 6.1, 'USGS NEIC'), ('xx', 's4', 5.0, None)]
right      4 filas: [(None, None, None, 'sin red'), ('ak', 's2', 4.3, 'Alaska'), ('nc', None, None, 'California norte'), ('us', 's1', 6.1, 'USGS NEIC')]
full       7 filas: [(None, None, None, 'sin red'), (None, 's5', 4.8, None), ('ak', 's2', 4.3, 'Alaska'), ('nc', None, None, 'California norte'), ('pr', 's3', 4.0, None), ('us', 's1', 6.1, 'USGS NEIC'), ('xx', 's4', 5.0, None)]
left_semi  2 filas: [('ak', 's2', 4.3), ('us', 's1', 6.1)]
left_anti  3 filas: [(None, 's5', 4.8), ('pr', 's3', 4.0), ('xx', 's4', 5.0)]
```

| Tipo | Se queda con | Uso típico |
| --- | --- | --- |
| `inner` (por defecto) | Solo las filas con pareja en los dos lados. | Combinar cuando la pareja es obligatoria. |
| `left` | Todas las de la izquierda; columnas de la derecha a `null` si no hay pareja. | **El más habitual**: enriquecer sin perder filas. |
| `right` | Todas las de la derecha. | Raro: se escribe como `left` cambiando el orden. |
| `full` | Todas las de los dos lados. | Comparar dos fuentes: qué hay en una y no en otra. |
| `left_semi` | Las de la izquierda **que tienen** pareja, sin añadir columnas. | «Terremotos de redes conocidas». Es un filtro, no duplica. |
| `left_anti` | Las de la izquierda **que no tienen** pareja. | «Terremotos de redes que no están en el catálogo»: encontrar huecos. |

:::cuidado inner pierde filas en silencio
Con `inner`, `pr`, `xx` y el terremoto sin red **desaparecen** sin ningún aviso. Si la tabla de la derecha
está incompleta, perderás datos. Por eso, para enriquecer, `left`; y un `left_anti` es la forma rápida de
saber qué se habría perdido.
:::

## Los nulos no se emparejan

En SQL, `null = null` no es verdadero: es `null`. Así que la fila con red nula de `sismos` **no** se une
con la fila con red nula de `redes` (fíjate en `full`: salen las dos por separado). Si de verdad quieres
que dos nulos coincidan, `eqNullSafe` (el `<=>` de SQL):

```salida
nulo con ==          : 0
nulo con eqNullSafe  : 1
```

## Claves repetidas: las filas se multiplican

Si la clave se repite en un lado, cada fila del otro lado se une con **todas** sus repeticiones. Con una
segunda fila `us` en `redes`:

```salida
inner con 'us' repetida: 3 filas (antes, 2)
```

Con tablas grandes, una clave repetida por error duplica millones de filas y hace que las sumas y los
recuentos posteriores salgan inflados. Antes de unir, comprueba que la clave es única donde debe serlo:

```python
redes.groupBy("red").count().filter("count > 1").show()   # debería salir vacío
```

## Columnas con el mismo nombre

```salida
columnas con condición: ['id', 'red', 'mag', 'red', 'nombre'] | con on='red': ['red', 'id', 'mag', 'nombre']
select('red'): [AMBIGUOUS_REFERENCE] Reference `red` is ambiguous, could be: [`red`, `red`]. SQLSTATE: 42704
```

- Con `join(otra, "red")` (o una lista de nombres), Spark une por igualdad y deja **una sola** columna
  `red`. Es lo más cómodo cuando la columna se llama igual en los dos lados.
- Con una condición (`sismos.red == redes.red`) quedan las **dos**, y pedir `"red"` es ambiguo. Se
  resuelve nombrando la columna por su DataFrame (`sismos.red`) o con alias:

```python
s, r = sismos.alias("s"), redes.alias("r")
s.join(r, F.col("s.red") == F.col("r.red")).select("s.red", "r.nombre")
```

La condición es necesaria cuando no es una simple igualdad: varias columnas, rangos (`abs(t1 - t2) <= 30`),
desigualdades. Lo verás en la fusión de la lección 22.

## Cómo hace Spark un join

Para unir, las filas con la misma clave tienen que estar en el mismo sitio. Spark tiene dos estrategias
principales:

| Estrategia | Cómo | Cuándo |
| --- | --- | --- |
| **Broadcast hash join** | Manda una **copia entera** de la tabla pequeña a cada ejecutor; la grande no se mueve. | Un lado cabe en memoria (por defecto, menos de 10 MB: `spark.sql.autoBroadcastJoinThreshold`). |
| **Sort-merge join** | **Baraja** (*shuffle*) los dos lados por la clave, ordena y recorre a la vez. | Los dos lados son grandes. |

El broadcast evita mover la tabla grande por la red, que es lo más caro (lección 12). Spark lo elige solo…
si **sabe** que la tabla es pequeña:

```python
print("tamaño que estima Spark de 'redes':", redes._jdf.queryExecution().optimizedPlan().stats().sizeInBytes())
redes.write.mode("overwrite").parquet(f"{SALIDA}/redes")
redes_pq = spark.read.parquet(f"{SALIDA}/redes")
```

```salida
tamaño que estima Spark de 'redes': 9223372036854775807
tamaño que estima Spark de la leída de Parquet: 1466
sin AQE, redes de Python   : SortMergeJoin
sin AQE, redes de Parquet  : BroadcastHashJoin
sin AQE, con F.broadcast   : BroadcastHashJoin
```

Una tabla de cuatro filas creada desde Python tiene un tamaño estimado de **9,2 trillones de bytes**: el
máximo posible, porque Spark no tiene forma de saber cuánto ocupa y supone lo peor. Así que no la manda
entera y baraja los 18.302 terremotos para unirlos con cuatro filas. La misma tabla leída de Parquet tiene
tamaño conocido (1.466 bytes) y se manda por broadcast sin pedirlo.

`F.broadcast(df)` es una **pista**: obliga al broadcast aunque Spark no sepa el tamaño. Úsala cuando
**sabes** que un lado es pequeño (tablas de referencia, catálogos) y nunca con algo que pueda crecer: si
no cabe en la memoria de los ejecutores, el trabajo falla.

(Las líneas dicen «sin AQE» porque la ejecución adaptativa puede cambiar la estrategia **durante** la
ejecución, con los tamaños reales. Lo verás en la lección 14.)

## Un join con una tabla enorme

Cuando los dos lados son grandes y una clave concentra muchísimas filas (una red que publica el 90 % de
los terremotos), la tarea que recibe esa clave tarda mucho más que las demás: es el **sesgo** (*skew*). La
lección 15 lo trata con sus soluciones.

:::ejercicio Redes sin catálogo
Con los 18.302 terremotos de 2025 (`terremotos_2025(spark)`) y la tabla `redes` del script:

1. ¿Cuántos terremotos son de una red que **no** está en `redes`? (Pista: un tipo de join lo responde
   directamente.)
2. ¿Qué redes son, y cuántos terremotos tiene cada una?
3. ¿Cuántas filas da un `inner` y cuántas un `left`? ¿Cuadra con lo anterior?

:::solucion
```python
t = terremotos_2025(spark)
huerfanos = t.join(redes, "red", "left_anti")
print("sin catálogo:", huerfanos.count())
huerfanos.groupBy("red").count().orderBy(F.desc("count")).show()
print("inner:", t.join(redes, "red").count(), "| left:", t.join(redes, "red", "left").count())
```

SOLUCION_08
:::

```quiz
Quieres los terremotos cuya red **no** está en una lista de redes. ¿Qué join usas?
- `left` y luego filtrar `nombre` nulo.
* `left_anti`.
- `right`.
> `left_anti` devuelve justo las filas de la izquierda sin pareja, sin añadir columnas. El `left` + filtro funciona, pero es más largo y más fácil de equivocar.
```

```quiz
Tras un join, la suma de magnitudes es el doble de lo esperado. ¿Causa más probable?
* La clave está repetida en la otra tabla y cada fila se ha unido dos veces.
- Spark suma dos veces en los joins.
- El join era `left` en vez de `inner`.
> Antes de unir, comprueba que la clave es única en la tabla de referencia.
```

```quiz
Una tabla de 50 filas creada con `spark.createDataFrame` se une con una de 100 millones y Spark hace sort-merge. ¿Por qué?
- Porque sort-merge siempre es más rápido.
* Porque no conoce el tamaño de la pequeña (lo estima al máximo); con `F.broadcast` lo fuerzas.
- Porque el broadcast solo funciona con Parquet.
> Sin estadísticas, Spark supone lo peor. La pista F.broadcast le dice que es pequeña.
```
