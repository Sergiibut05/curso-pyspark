---
titulo: Planes, ejecución adaptativa y la interfaz web
resumen: Leer explain en modo formatted, qué cambia AQE mientras la consulta corre, y la interfaz web de Spark (trabajos, etapas, tareas) en el navegador y por su API.
bloque: Rendimiento
minutos: 25
practica: si
---

Cuando algo va lento en Spark, adivinar no sirve. Hay dos herramientas para **ver** qué está haciendo: el
**plan** (qué piensa hacer) y la **interfaz web** (qué ha hecho, cuánto ha tardado cada trozo).

## Del código a las tareas

```text
tu código ──► plan lógico ──(Catalyst optimiza)──► plan físico ──► trabajos ──► etapas ──► tareas
```

| Pieza | Qué es |
| --- | --- |
| **Trabajo** (*job*) | Lo que lanza **una acción** (`count`, `collect`, `write`…). |
| **Etapa** (*stage*) | Un trozo del trabajo que se puede hacer **sin mover datos** entre ejecutores. Cada **shuffle** (`Exchange` en el plan) parte el trabajo en una etapa más. |
| **Tarea** (*task*) | Una etapa aplicada a **una partición**. Una etapa con 4 particiones son 4 tareas, que corren en paralelo si hay núcleos. |

Una etapa lenta suele ser por una de tres cosas: lee muchos datos, baraja muchos datos, o una de sus
tareas tarda muchísimo más que las demás (**sesgo**, lección 15).

## explain en modo formatted

La lección 4 usaba `explain()`. Con `mode="formatted"` el árbol sale numerado arriba y los detalles de cada
nodo, abajo:

```python titulo="practicas/14_planes_y_ui.py (extracto)"
consulta = (t.filter("mag >= 5").join(redes, "red", "left")
            .groupBy("nombre").agg(F.count("*").alias("n"), F.max("mag").alias("mag_max")))
consulta.explain(mode="formatted")
```

```powershell pruebalo
spark-py 14_planes_y_ui.py 2>$null
```

```salida titulo="El árbol (los detalles de cada nodo, recortados)"
== Physical Plan ==
AdaptiveSparkPlan (17)
+- HashAggregate (16)
   +- Exchange (15)
      +- HashAggregate (14)
         +- Project (13)
            +- SortMergeJoin LeftOuter (12)
               :- Sort (7)
               :  +- Exchange (6)
               :     +- Project (5)
               :        +- Filter (4)
               :           +- Generate (3)
               :              +- Filter (2)
               :                 +- Scan json  (1)
               +- Sort (11)
                  +- Exchange (10)
                     +- Filter (9)
                        +- Scan ExistingRDD (8)

(4) Filter
Input [1]: [f#9]
Condition : (isnotnull(f#9.properties.mag) AND (f#9.properties.mag >= 5.0))

(6) Exchange
Input [2]: [mag#13, red#16]
Arguments: hashpartitioning(red#16, 4), ENSURE_REQUIREMENTS, [plan_id=54]

(14) HashAggregate
Input [2]: [mag#13, nombre#32]
Keys [1]: [nombre#32]
Functions [2]: [partial_count(1), partial_max(mag#13)]
…
(17) AdaptiveSparkPlan
Output [3]: [nombre#32, n#33L, mag_max#34]
Arguments: isFinalPlan=false
```

Se lee **de abajo arriba**:

1. `Scan json` (1): lee el fichero. Más abajo en los detalles, `ReadSchema` dice qué columnas lee.
2. `Generate` (3): el `explode` de los features. El filtro de `mag >= 5` (4) va **después**, sobre cada
   terremoto: Catalyst lo ha puesto lo antes posible.
3. Dos `Exchange` (6 y 10): barajan los dos lados del join por `red`. Son dos shuffles.
4. `SortMergeJoin` (12): el join. La tabla `redes` viene de Python (`Scan ExistingRDD`), así que Spark no
   sabe que es pequeña (lección 8).
5. `HashAggregate` (14) → `Exchange` (15) → `HashAggregate` (16): la agregación en **dos fases**. Cada
   partición cuenta lo suyo (`partial_count`), se baraja solo ese resumen y se combina. Mucho menos que
   barajar todas las filas.

## La ejecución adaptativa (AQE)

El nodo de arriba dice `AdaptiveSparkPlan … isFinalPlan=false`. Con **AQE** (*Adaptive Query Execution*,
activada por defecto desde Spark 3.2), el plan de arriba es solo el **inicial**. Spark ejecuta por etapas
y, al terminar cada shuffle, mira los **tamaños reales** y puede cambiar el resto del plan:

```python
print("antes de ejecutar :", nodos(consulta))
consulta.collect()
print("después           :", nodos(consulta))
```

```salida
antes de ejecutar : ['isFinalPlan=false', 'SortMergeJoin']
después           : ['isFinalPlan=true', 'BroadcastHashJoin', 'AQEShuffleRead']
```

La **misma** consulta, al terminar, tiene un **`BroadcastHashJoin`**: tras ejecutar la primera etapa, AQE
vio que el lado de `redes` ocupaba unos bytes y cambió el join a broadcast sobre la marcha. Es la respuesta
a lo que quedó pendiente en la lección 8: sin estadísticas, Spark planifica lo peor, y AQE lo corrige con
los datos reales (aunque los dos lados ya se habían empezado a barajar; con `F.broadcast` te ahorras eso).

AQE hace tres cosas principales:

| Optimización | Qué hace |
| --- | --- |
| Cambiar la estrategia de join | Sort-merge a broadcast si un lado resulta ser pequeño. |
| Juntar particiones del shuffle | Si `spark.sql.shuffle.partitions` crea muchas casi vacías, las junta (`AQEShuffleRead`). |
| Partir particiones sesgadas | En un join, divide una partición enorme en varias (lección 15). |

La segunda, medida:

```salida
AQE=false particiones tras el groupBy: 200
AQE=true  particiones tras el groupBy: 1
```

Un `groupBy("red")` sobre 18.302 filas produce 11 grupos. Sin AQE, el valor por defecto de 200
particiones crea **200 tareas**, casi todas vacías. Con AQE, se juntan en 1. Por eso, con AQE activada,
ajustar `spark.sql.shuffle.partitions` importa mucho menos que antes.

:::nota explain después de ejecutar
Para ver el plan **final** de AQE, llama a `explain()` sobre el mismo DataFrame **después** de una acción
sobre él. Spark muestra entonces el plan final y, debajo, el inicial (`== Initial Plan ==`). Una acción
distinta (`df.count()` sobre `df`) es otra consulta, con su propio plan.
:::

## La interfaz web

Cada aplicación de Spark levanta una web en el puerto **4040** del driver. En el laboratorio, el driver
está dentro de un contenedor, así que hay que **publicar el puerto** y mantener la aplicación viva un rato.
El script acepta los segundos de espera como argumento:

```powershell pruebalo
docker compose run --rm -p 4040:4040 spark python 14_planes_y_ui.py 120
```

Y abre <http://localhost:4040> mientras espera. Pestañas:

| Pestaña | Qué enseña |
| --- | --- |
| **Jobs** | Un trabajo por acción, con su duración y sus etapas. |
| **Stages** | Cada etapa: tareas, tiempo, datos leídos y barajados, y la **distribución del tiempo de las tareas** (mínimo, mediana, máximo): ahí se ve el sesgo. |
| **Storage** | Lo que hay en caché (lección 13). |
| **Environment** | Toda la configuración efectiva. Útil para saber qué valor tiene de verdad una opción. |
| **Executors** | Memoria, núcleos, tareas y tiempo de recolección de basura de cada ejecutor. |
| **SQL / DataFrame** | Cada consulta con su plan **dibujado**, con filas y tiempos reales en cada nodo. La más útil. |

Todo lo que enseña la web está también en una **API REST**, que es como la consulta el script:

```python
with urllib.request.urlopen(f"{url}/api/v1/applications/{app}/jobs") as r:
    trabajos = json.load(r)
```

```salida
interfaz web en http://f03832d45419:4040: 7 trabajos
  trabajo 4: SUCCEEDED, etapas [5, 6], tareas 3
  trabajo 5: SUCCEEDED, etapas [7, 8, 9], tareas 4
  trabajo 6: SUCCEEDED, etapas [10], tareas 2
etapa más costosa: 1 (collect at /practicas/14_planes_y_ui.py:), 2 tareas, 2694 ms de ejecución
```

(El nombre de la máquina es el del contenedor; desde Windows, `localhost`.) La web de una aplicación
desaparece cuando la aplicación termina. Para consultarla después existe el **History Server**, que lee los
registros de eventos que Spark guarda si se activa `spark.eventLog.enabled`.

:::ejercicio Encuentra los shuffles
1. Con `explain(mode="formatted")`, ¿cuántos `Exchange` tiene `t.groupBy("red").count().orderBy("count")`?
   ¿Por qué?
2. Ejecútala con la interfaz abierta (`… 14_planes_y_ui.py 120` y añadiendo la consulta) y busca en la
   pestaña **SQL / DataFrame** cuántas filas salen de cada nodo.

:::solucion
```python
t.groupBy("red").count().orderBy("count").explain(mode="formatted")
```

SOLUCION_14
:::

```quiz
¿Qué parte un trabajo de Spark en varias etapas?
- Cada filtro.
* Cada shuffle (`Exchange` en el plan): los datos tienen que moverse entre ejecutores.
- Cada columna nueva.
> Dentro de una etapa, cada partición se procesa sin hablar con las demás.
```

```quiz
Un join planificado como sort-merge aparece como broadcast al terminar. ¿Quién lo ha cambiado?
- El optimizador Catalyst, antes de ejecutar.
* La ejecución adaptativa (AQE), al ver el tamaño real de un lado tras la primera etapa.
- El usuario, con F.broadcast.
> AQE replanifica con los datos reales entre etapas.
```

```quiz
¿Dónde miras si una tarea de una etapa tarda mucho más que las demás?
- En la pestaña Environment.
* En la pestaña Stages, en la distribución del tiempo de las tareas (mínimo, mediana, máximo).
- En el plan de explain().
> Un máximo muy por encima de la mediana es la firma del sesgo.
```
