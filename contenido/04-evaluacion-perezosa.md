---
titulo: Transformaciones, acciones y el plan
resumen: Spark no hace nada hasta que se lo pides. Qué es una acción, qué es un shuffle y cómo leer explain().
bloque: Fundamentos
minutos: 18
practica: Con Docker, sin la pila
---

La idea que más sorprende al llegar de pandas: **escribir una transformación no ejecuta nada**. Spark solo
apunta qué quieres hacer. Cuando pides un resultado (una **acción**), mira el plan completo, lo optimiza y
entonces lo ejecuta.

## Transformaciones y acciones

| Transformaciones (perezosas, devuelven otro DataFrame) | Acciones (ejecutan, devuelven un resultado) |
| --- | --- |
| `select`, `filter`/`where`, `withColumn`, `drop` | `show()`, `count()` |
| `groupBy(...).agg(...)`, `orderBy`, `distinct` | `collect()`, `take(n)`, `first()` |
| `join`, `union`, `repartition` | `write...save()`, `foreach`, `toPandas()` |

```widget
plan
```

## Pruébalo con el catálogo

```python titulo="curso-pyspark/practicas/04_perezoso.py"
t = terremotos_2025(spark)

inicio = time.perf_counter()
fuertes = t.filter(F.col("mag") >= 6).groupBy("red").count().orderBy(F.col("count").desc())
print(f"Definir la consulta: {time.perf_counter() - inicio:.3f} s  (no se ha leído nada)")

inicio = time.perf_counter()
fuertes.show()
print(f"show(): {time.perf_counter() - inicio:.3f} s  (ahora sí: lectura, filtro, agrupación)")

fuertes.explain()
```

```powershell pruebalo
spark-py 04_perezoso.py 2>$null
```

```salida
Definir la consulta: 0.072 s  (no se ha leído nada)
+---+-----+
|red|count|
+---+-----+
| us|  144|
| ak|    1|
+---+-----+

show(): 1.126 s  (ahora sí: lectura, filtro, agrupación)
```

Definir la consulta es instantáneo; el segundo largo es el `show()`. (Los 0,07 s del principio son Python
construyendo el plan, no Spark leyendo el fichero.)

## Leer `explain()`

```salida titulo="fuertes.explain() (de abajo arriba)"
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [count#31L DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(count#31L DESC NULLS LAST, 4), ENSURE_REQUIREMENTS, [plan_id=138]
      +- HashAggregate(keys=[red#16], functions=[count(1)])
         +- Exchange hashpartitioning(red#16, 4), ENSURE_REQUIREMENTS, [plan_id=135]
            +- HashAggregate(keys=[red#16], functions=[partial_count(1)])
               +- Project [f#9.properties.net AS red#16]
                  +- Filter (isnotnull(f#9.properties.mag) AND (f#9.properties.mag >= 6.0))
                     +- Generate explode(features#6), false, [f#9]
                        +- Filter ((size(features#6, false) > 0) AND isnotnull(features#6))
                           +- FileScan json [features#6] … ReadSchema: struct<features:array<struct<…
```

El plan se lee **de abajo arriba**:

1. `FileScan json`: leer el fichero. Fíjate en `ReadSchema`: Spark solo pide lo que la consulta usa.
2. `Filter … size(features) > 0`: un filtro que **tú no escribiste**. El optimizador (Catalyst) sabe que
   `explode` de un array vacío no produce filas y lo descarta antes.
3. `Generate explode`, `Filter mag >= 6`, `Project … AS red`: tu consulta, sin las columnas que no hacen
   falta (de las diez de `terremotos_2025` solo quedan `mag` y `red`).
4. `HashAggregate partial_count`: cada partición cuenta **lo suyo** primero (agregación parcial).
5. `Exchange hashpartitioning(red, 4)`: el **shuffle**. Las filas de la misma `red` tienen que acabar en la
   misma partición para sumarlas; se reparten por hash en 4 particiones (`spark.sql.shuffle.partitions=4`).
6. `HashAggregate count`: suma final por `red`.
7. `Exchange rangepartitioning` + `Sort`: otro shuffle, ahora para ordenar.

:::nota Etapas
Cada `Exchange` corta el trabajo en **etapas**. Dentro de una etapa, cada partición va de principio a fin
sin hablar con las demás (transformaciones *estrechas*: `filter`, `select`, `withColumn`). Un shuffle
(transformaciones *anchas*: `groupBy`, `orderBy`, `join`, `distinct`) obliga a escribir resultados
intermedios y a redistribuirlos. Es la operación cara de Spark: menos shuffles, consultas más rápidas.
:::

## Consecuencias prácticas

- **Los errores aparecen tarde.** Una columna mal escrita falla al definir (el plan se analiza enseguida),
  pero un dato que rompe una conversión falla al ejecutar la acción, quizá muchas líneas después.
- **Cada acción vuelve a ejecutar el plan.** Si haces `df.count()` y luego `df.show()`, Spark lee el
  fichero dos veces. Si vas a reutilizar un resultado, `df.cache()` (o `persist()`) lo guarda en memoria
  tras la primera acción.
- **`print` no sirve para depurar transformaciones**: dentro de una transformación no se ejecuta nada.
  Usa `show()`, `explain()` o `limit(5).collect()`.

:::nota En la fusión del observatorio
`procesar_lote` llama a `lote.count()` (acción) y luego a `ultimas_versiones(lote).collect()` (otra
acción). Son dos ejecuciones del plan, así que el lote se lee **dos veces** de Kafka. No es un error: el
lote son unos offsets fijos, así que las dos lecturas ven lo mismo. Con lotes de como mucho 5.000 mensajes
(`maxOffsetsPerTrigger`) el coste es pequeño; con lotes grandes, la guía de Spark recomienda
`lote.persist()` al principio de `foreachBatch` y `lote.unpersist()` al final.
:::

:::ejercicio Cuenta los shuffles
Sin ejecutar, predice cuántos `Exchange` tendrá cada consulta. Luego compruébalo con `explain()`.

1. `t.filter(F.col("mag") > 5).select("id", "mag")`
2. `t.groupBy("tipo_mag").count()`
3. `t.select("red").distinct().orderBy("red")`

:::solucion
1. **Ninguno**: `filter` y `select` son estrechas; una sola etapa.
2. **Uno**: `groupBy` necesita juntar cada `tipo_mag` en una partición.
3. **Dos**: `distinct` es una agregación (un shuffle) y `orderBy` otro (`rangepartitioning`).

Con AQE (la ejecución adaptativa, `AdaptiveSparkPlan`) Spark puede quitar o fusionar pasos en tiempo de
ejecución si ve que hay pocos datos; `explain()` antes de ejecutar enseña el plan inicial.
:::

```quiz
Escribes `df2 = df.filter(...).groupBy(...).count()`. ¿Cuántas filas ha leído Spark después de esa línea?
* Ninguna.
- Las que pasan el filtro.
- Todas, para poder agrupar.
> Son transformaciones: solo construyen el plan. La lectura empieza con la primera acción (`show`, `count`, `collect`…).
```

```quiz
¿Qué indica un `Exchange hashpartitioning` en el plan físico?
- Que Spark comprime los datos.
* Un shuffle: las filas se redistribuyen entre particiones según el hash de una clave.
- Que la consulta lee de Kafka.
> Es el límite entre etapas. Ocurre en agregaciones, joins y ordenaciones, y es la parte más cara de la consulta.
```
