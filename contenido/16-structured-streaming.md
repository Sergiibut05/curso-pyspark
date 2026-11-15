---
titulo: Structured Streaming - micro-lotes y checkpoint
resumen: Un flujo es una tabla que no deja de crecer. Disparadores, sumideros, foreachBatch y el directorio de checkpoint.
bloque: Streaming
minutos: 22
practica: Con Docker, sin la pila
---

**Structured Streaming** usa la misma API de DataFrames para datos que no paran de llegar. La idea: un
flujo es una **tabla sin fin**. Escribes la consulta como si fuera sobre una tabla normal, y Spark la
ejecuta **una y otra vez sobre lo nuevo**, en **micro-lotes**.

## Las piezas de una consulta en streaming

```python
consulta = (
    spark.readStream.format("kafka")…load()        # 1. fuente
         .select(…).filter(…)                       # 2. transformaciones (las de siempre)
         .writeStream.foreachBatch(funcion)          # 3. sumidero
         .option("checkpointLocation", "/ruta")      # 4. dónde guarda por dónde va
         .trigger(processingTime="15 seconds")       # 5. cuándo dispara cada micro-lote
         .start()                                    # arranca en segundo plano
)
```

| Pieza | Opciones habituales | En la fusión |
| --- | --- | --- |
| Fuente | `kafka`, ficheros de una carpeta, `rate` (pruebas) | `kafka` con `subscribePattern` |
| Sumidero | `kafka`, ficheros, `console`, `memory`, `foreachBatch` | `foreachBatch` (y dentro, escritura a Kafka) |
| Disparador | `processingTime="15 seconds"`, `availableNow=True` | cada 15 s (`FUSION_TRIGGER_SEGUNDOS`) |
| Modo de salida | `append`, `update`, `complete` (para agregaciones) | no aplica con `foreachBatch` |
| Checkpoint | una carpeta duradera | volumen `fusion_checkpoint` en `/data/fusion/checkpoint` |

## Un flujo de prueba sin Kafka

La fuente `rate` genera filas `(timestamp, value)` a ritmo fijo. Sirve para ver los micro-lotes:

```python titulo="curso-pyspark/practicas/16_streaming.py"
flujo = spark.readStream.format("rate").option("rowsPerSecond", 20).load()
pares = flujo.withColumn("par", F.col("value") % 2 == 0)

def por_lote(lote, lote_id):
    filas = lote.count()
    rango = lote.agg(F.min("value"), F.max("value")).first()
    print(f"lote={lote_id} filas={filas} values={rango[0]}..{rango[1]}")

consulta = (
    pares.writeStream.foreachBatch(por_lote)
    .option("checkpointLocation", "/tmp/checkpoint-rate")
    .trigger(processingTime="3 seconds")
    .start()
)
time.sleep(14)
p = consulta.lastProgress
print("lastProgress:", {k: p[k] for k in ("batchId", "numInputRows", "inputRowsPerSecond", "processedRowsPerSecond")})
consulta.stop()
```

```powershell pruebalo
spark-py 16_streaming.py 2>$null
```

```salida
lote=0 filas=0 values=None..None
lote=1 filas=20 values=0..19
lote=2 filas=60 values=20..79
lote=3 filas=60 values=80..139
lote=4 filas=60 values=140..199
lote=5 filas=60 values=200..259
lastProgress: {'batchId': 5, 'numInputRows': 120, 'inputRowsPerSecond': 40.0, 'processedRowsPerSecond': 779.2}
```

- Cada 3 s llega un micro-lote con lo nuevo: 20 filas/s × 3 s = **60 filas**. Ninguna fila se repite ni
  se salta: `values` continúa exactamente donde lo dejó el lote anterior.
- `lote` dentro de `foreachBatch` es un **DataFrame normal** (estático): admite todo lo de las lecciones
  anteriores, incluidas acciones como `count()`.
- `lastProgress` es el informe del último micro-lote. `processedRowsPerSecond` mayor que
  `inputRowsPerSecond` significa que la consulta **da abasto**. Si fuera menor, se iría quedando atrás.

## El checkpoint

El checkpoint es lo que convierte un bucle en un sistema fiable. Por cada micro-lote, Spark escribe
**antes** de procesarlo qué va a leer y **después** que lo ha terminado:

```text
checkpoint/
├── metadata      id de la consulta
├── sources/0/…   dónde empezó la fuente
├── offsets/N     «el lote N va de A a B»   (ANTES de procesar)
└── commits/N     «el lote N ha terminado»  (DESPUÉS)
```

Al arrancar, Spark mira el último `offsets/N`. Si no tiene su `commits/N`, **repite el lote N con los
mismos offsets**. Pruébalo:

```widget
microlotes
```

:::defensa Lo que garantiza y lo que no
Spark garantiza que cada lote se **procesa al menos una vez** y que, si se repite, **lee exactamente los
mismos datos**. Lo que haga `foreachBatch` con ellos (escribir en Kafka, en PostgreSQL) puede ocurrir dos
veces. «Exactamente una vez» de punta a punta exige que esa escritura sea idempotente o determinista: es
lo que hace la fusión (lección 18).
:::

:::cuidado El checkpoint es de una consulta concreta
Si cambias la consulta (otra fuente, otro `subscribePattern`, otras agregaciones con estado), el checkpoint
viejo puede no servir: según el cambio, Spark se niega a arrancar o ignora lo que no reconoce. Y si lo borras, la consulta vuelve a empezar según
`startingOffsets`. En la fusión, borrar el volumen `fusion_checkpoint` haría releer los topics desde
`earliest`: no se duplicaría nada en la base (el estado de la fusión y el consumidor lo absorben), pero
sería trabajo repetido.
:::

## Disparadores

| Disparador | Comportamiento |
| --- | --- |
| (ninguno) | Un micro-lote detrás de otro, lo antes posible. |
| `processingTime="15 seconds"` | Uno cada 15 s; si un lote tarda más, el siguiente empieza en cuanto acaba. |
| `availableNow=True` | Procesa todo lo que haya ahora (en varios lotes si hay tope) y **termina**. Ideal para trabajos programados y para las prácticas. |
| `continuous="1 second"` | Experimental, latencia de milisegundos, muy limitado. |

:::ejercicio Más rápido de lo que da abasto
Cambia `rowsPerSecond` a 200.000 y el disparador a 1 s. Mira `lastProgress`: ¿sigue siendo
`processedRowsPerSecond` mayor que `inputRowsPerSecond`? ¿Qué pasa con el tamaño de los lotes?

:::solucion
Con mucha más entrada, cada lote trae cientos de miles de filas y el `count()` + `agg()` de cada lote tarda
más. Si `processedRowsPerSecond` cae por debajo de `inputRowsPerSecond`, los lotes empiezan uno detrás de
otro sin esperar al disparador y cada vez son más grandes: la consulta se queda atrás. Con Kafka eso se
nota en el lag. La defensa es el **tope por lote** (`maxOffsetsPerTrigger` en Kafka; `rate` no lo tiene),
que limita el tamaño de cada lote aunque haya mucho pendiente.
:::

```quiz
La fusión se cae después de escribir `offsets/40` y antes de `commits/40`. ¿Qué hace al arrancar?
- Empieza el lote 41 desde el final del 40.
* Repite el lote 40 con exactamente los mismos offsets.
- Vuelve a leer los topics desde `earliest`.
> Un `offsets/N` sin `commits/N` significa «planificado y no terminado». Spark lo repite con el mismo rango, así que `foreachBatch` debe tolerar procesar un lote dos veces.
```

```quiz
¿Qué tipo de objeto recibe la función de `foreachBatch`?
- Un iterador de mensajes de Kafka.
* Un DataFrame normal (estático) con las filas del micro-lote, y su número de lote.
- Un DataFrame en streaming que sigue creciendo.
> Dentro de `foreachBatch` el lote es un DataFrame corriente: se pueden usar acciones, joins con tablas estáticas y escribir en cualquier sitio.
```
