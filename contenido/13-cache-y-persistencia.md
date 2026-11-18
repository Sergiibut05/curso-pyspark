---
titulo: Cache y persistencia
resumen: Por qué Spark repite el trabajo en cada acción, cómo guardarlo con cache o persist, qué significa que sea perezoso, los niveles de almacenamiento y cuándo no compensa.
bloque: Rendimiento
minutos: 18
practica: si
---

```python preparacion
import time

from pyspark import StorageLevel
from pyspark.sql import functions as F

from comun import sesion, terremotos_2025

spark = sesion("cache")
```

## Cada acción empieza de cero

En la lección 4 viste que Spark es perezoso: las transformaciones solo construyen un plan, y cada
**acción** (`count`, `show`, `collect`, `write`…) lo ejecuta. La consecuencia que suele sorprender: **si
haces dos acciones sobre el mismo DataFrame, Spark lo calcula dos veces**, desde la lectura del fichero.

```python
t = terremotos_2025(spark).withColumn("energia", F.pow(F.lit(10.0), 1.5 * F.col("mag") + 4.8))
t.count()                          # lee el GeoJSON, lo aplana, calcula
t.agg(F.max("mag")).first()        # vuelve a leer el GeoJSON, a aplanarlo, a calcular
```

Cuando un DataFrame caro se reutiliza varias veces (explorar, varias agregaciones sobre el mismo
resultado, un bucle), conviene **guardarlo** la primera vez que se calcula: eso es `cache()`.

## Míralo

```powershell pruebalo
spark-py 13_cache.py 2>$null
```

```salida
sin cache, acción 1 (count)         0.86 s  -> 18302
sin cache, acción 2 (max mag)       0.49 s  -> 8.8
¿en caché tras cache()? True | nivel: Disk Memory Deserialized 1x Replicated
con cache, acción 1 (la llena)      1.21 s  -> 18302
con cache, acción 2 (max mag)       0.20 s  -> 8.8
con cache, acción 3 (por red)       0.56 s  -> 11
el plan lee de la caché: True
guardado: 2 partición(es), 1.7 MB en memoria, 0.0 MB en disco
¿en caché tras unpersist()? False
DISK_ONLY: 0.0 MB en memoria, 0.9 MB en disco
```

Cómo leerlo:

- **Sin caché**, cada acción paga la lectura y el aplanado del JSON.
- **`cache()` es perezoso**: no guarda nada al llamarlo. `is_cached` dice `True` porque el DataFrame está
  *marcado*, pero se llena en la **primera acción**, que por eso es la más lenta (1,21 s: calcula **y**
  guarda).
- A partir de ahí, las acciones leen de memoria: 0,20 s en vez de 0,49.
- El plan físico muestra **`InMemoryTableScan`** en lugar de la lectura del fichero: es la forma de
  comprobar que de verdad se está usando.
- La caché ocupa **1,7 MB** en memoria para un JSON de 12 MB: Spark guarda los datos por columnas y
  comprimidos, no el texto original.

Los tiempos son pequeños porque los datos lo son; con cientos de millones de filas, la diferencia es entre
segundos y minutos por acción.

## cache, persist y los niveles

`cache()` es `persist()` con el nivel por defecto. Con `persist(nivel)` se elige dónde:

| Nivel | Dónde | Cuándo |
| --- | --- | --- |
| `MEMORY_AND_DISK` (el de `cache()` en DataFrames) | En memoria; lo que no cabe, a disco. | Casi siempre. |
| `MEMORY_ONLY` | Solo en memoria; lo que no cabe se **recalcula** cuando hace falta. | Datos que caben de sobra. |
| `DISK_ONLY` | Solo en disco local de los ejecutores. | Mucho dato, poca memoria, cálculo muy caro. |
| `…_2` (`MEMORY_AND_DISK_2`…) | Con una segunda copia en otro ejecutor. | Clústeres donde perder un ejecutor es frecuente. |

El texto `Disk Memory Deserialized 1x Replicated` de la salida es `MEMORY_AND_DISK`: puede usar disco y
memoria, guardado deserializado (listo para usar), una copia.

## Liberar

`unpersist()` libera la caché. Spark también la libera solo cuando necesita memoria (expulsa lo usado hace
más tiempo), pero en procesos largos conviene hacerlo a mano en cuanto un DataFrame deja de usarse, para no
expulsar algo que sí hace falta.

En la interfaz web de Spark (lección 14), la pestaña **Storage** enseña lo que hay en caché, cuánto ocupa y
qué fracción está en memoria y en disco: los mismos datos que el script imprime.

## Cuándo no compensa

- **Un DataFrame que se usa una sola vez**: cachearlo añade el coste de guardarlo y no se aprovecha nunca.
- **Leer Parquet y filtrar enseguida**: leer Parquet ya es rápido (lección 15) y cachear todo antes del
  filtro guarda filas que vas a tirar. Cachea **después** de filtrar y quedarte con las columnas que usas.
- **Datos que no caben**: si la caché expulsa y recalcula sin parar, puede ser más lento que no cachear.
- **En streaming**, cada micro-lote es un DataFrame distinto: dentro de `foreachBatch` sí tiene sentido
  cachear el lote si lo usas varias veces, y liberarlo al final (lo hace la lección 23).

:::cuidado cache no es un checkpoint ni una copia de seguridad
Lo cacheado vive en la memoria y el disco local de los ejecutores y desaparece con la aplicación. Si un
ejecutor muere, Spark recalcula su parte a partir del plan. Para guardar un resultado de verdad, se
escribe (lección 3).
:::

:::ejercicio Cuándo cachear
Tienes este código:

```python
t = terremotos_2025(spark)
fuertes = t.filter("mag >= 6").select("id", "red", "mag", "hora")
print(fuertes.count())
fuertes.groupBy("red").count().show()
fuertes.groupBy(F.month("hora")).count().show()
t.filter("mag < 4.5").count()
```

1. ¿Cuántas veces se lee el GeoJSON?
2. ¿Qué cachearías, y dónde pondrías `cache()` y `unpersist()`?
3. Compruébalo midiendo el tiempo total antes y después.

:::solucion
1. **Cuatro veces**: una por acción (`count`, dos `show` y el último `count`).
2. Cachear `fuertes` (pequeño: 145 filas y 4 columnas) justo al crearlo, y `unpersist()` después del
   segundo `groupBy`. **No** cachear `t`: la última acción es la única otra que lo usa y filtraría casi
   todo; cachearlo guardaría 18.302 filas para un solo uso más.

```python fragmento
fuertes = t.filter("mag >= 6").select("id", "red", "mag", "hora").cache()
...
fuertes.unpersist()
```

SOLUCION_13
:::

```quiz
Llamas a `df.cache()` y no haces nada más. ¿Qué hay en memoria?
- El DataFrame entero.
* Nada todavía: se llena con la primera acción.
- Solo el esquema.
> cache() es perezoso, como las transformaciones.
```

```quiz
¿Cómo compruebas que una consulta está usando la caché?
* Su plan físico muestra `InMemoryTableScan` en lugar de la lectura del origen.
- `df.is_cached` devuelve `True`.
- La consulta tarda menos.
> is_cached solo dice que el DataFrame está marcado; puede no haberse llenado aún, o la consulta puede venir de otro DataFrame.
```

```quiz
¿Qué pasa con la caché si un ejecutor se cae?
- Se pierden los datos y la aplicación falla.
* Spark recalcula las particiones perdidas a partir del plan.
- Se leen de la copia en el driver.
> La caché es una optimización: el plan sigue siendo la fuente de verdad.
```
