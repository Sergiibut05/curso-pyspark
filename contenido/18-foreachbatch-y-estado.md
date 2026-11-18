---
titulo: foreachBatch, estado e idempotencia
resumen: Leer Kafka en streaming con checkpoint, y por qué el estado de la fusión vive en PostgreSQL y no en Spark.
bloque: Streaming
minutos: 25
practica: si
---

```python preparacion
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from comun import KAFKA, sesion

spark = sesion("kafka-streaming", kafka=True)
sys.argv = sys.argv[:1]   # aquí no hay argumentos de línea de órdenes: el script usa su valor por defecto
```

## Una consulta como la de la fusión

Este script lee `eventos.usgs` en streaming, con tope por lote y checkpoint, y en cada lote se queda con
la última versión de cada informe. No escribe nada: solo informa.

```python titulo="curso-pyspark/practicas/18_kafka_streaming.py"
CHECKPOINT = "/practicas/_checkpoint-usgs"   # en la carpeta de prácticas: sobrevive entre ejecuciones

lectura = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", "eventos.usgs")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", sys.argv[1] if len(sys.argv) > 1 else "20000")
    .option("failOnDataLoss", "false")
    .load()
)

def por_lote(lote, lote_id):
    texto = F.col("value").cast("string")
    con_id = lote.withColumn("id", F.get_json_object(texto, "$.event.id")).withColumn(
        "version", F.get_json_object(texto, "$.event.properties.updated"))
    v = Window.partitionBy("id").orderBy(F.col("version").desc(), F.col("offset").desc())
    ultimas = con_id.withColumn("n", F.row_number().over(v)).filter("n = 1")
    rangos = lote.groupBy("partition").agg(F.min("offset").alias("desde"), F.max("offset").alias("hasta")).orderBy("partition").collect()
    print(f"lote={lote_id} mensajes={lote.count()} informes_distintos={ultimas.count()} "
          + " ".join(f"p{r['partition']}:{r['desde']}-{r['hasta']}" for r in rangos))

consulta = (
    lectura.writeStream.foreachBatch(por_lote)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)      # procesa lo que haya y termina
    .start()
)
consulta.awaitTermination()
```

```powershell pruebalo
spark-py 18_kafka_streaming.py 2>$null
spark-py 18_kafka_streaming.py 2>$null
```

```salida titulo="Primera ejecución"
lote=0 mensajes=2306 informes_distintos=2239 p0:0-755 p1:0-780 p2:0-768
Terminado. Vuelve a ejecutarlo: solo leerá lo nuevo.
```

```salida titulo="Segunda ejecución, sin mensajes nuevos"
Terminado. Vuelve a ejecutarlo: solo leerá lo nuevo.
```

Qué se ve:

- **Un solo lote**: el tope por defecto del script (20.000 mensajes) es mayor que todo el topic.
- **`informes_distintos` < `mensajes`**: 2.306 mensajes, 2.239 informes. Las 67 revisiones están en el
  mismo lote que su original, y la ventana se queda con la versión más nueva.
- **La segunda ejecución no lee nada**: el checkpoint (`practicas/_checkpoint-usgs/`) sabe por dónde iba.
  Bórralo y volverá a empezar desde `earliest`.

```powershell pruebalo
Get-ChildItem ..\practicas\_checkpoint-usgs\offsets, ..\practicas\_checkpoint-usgs\commits | Select-Object Name
Get-Content ..\practicas\_checkpoint-usgs\offsets\0
```

```salida titulo="offsets/0"
v1
{"batchWatermarkMs":0,"batchTimestampMs":…,"conf":{…}}
{"eventos.usgs":{"0":756,"1":781,"2":769}}
```

La última línea es **dónde termina** el lote 0 (exclusivo): el siguiente lote empezará ahí.

## Dónde guardar el estado

La fusión necesita **memoria**: para saber si un informe de EMSC es el mismo terremoto que uno de USGS que
llegó hace tres horas, tiene que recordar los informes anteriores. Hay dos sitios posibles.

### Opción A: el estado de Spark

Structured Streaming tiene operadores con estado: `dropDuplicates`, agregaciones por ventana de tiempo,
*joins* entre dos flujos, `applyInPandasWithState`… Guardan el estado en el checkpoint y lo limpian con
un **watermark**: «descarta todo lo que tenga una hora de evento más antigua que *máximo visto − X*».

```python fragmento
flujo.withWatermark("hora", "2 hours").dropDuplicates(["id", "hora"])
```

Funciona muy bien cuando los datos llegan **más o menos en orden**. Lo que llega más tarde que el
watermark, **se descarta**.

### Opción B: estado externo, consultado en cada lote

En `foreachBatch`, el lote es un DataFrame normal: se puede leer y escribir **cualquier base de datos**.
La fusión, en cada lote:

1. Lee de PostgreSQL (esquema `fusion`) los informes **cercanos en el tiempo** a los del lote (índice por
   hora).
2. Decide en Python qué se fusiona con qué.
3. Escribe los eventos fusionados en `eventos.live`.
4. Hace `COMMIT` del estado en PostgreSQL.
5. Al volver de `foreachBatch`, Spark escribe `commits/N`.

:::defensa Por qué la opción B
Está en `specs/013-multifuente/research.md`: **el relleno trae terremotos de hace meses**. Con un
watermark, un informe de enero de 2025 que llega en noviembre de 2026 sería «tarde» y se descartaría, o
habría que guardar dos años de estado en memoria. En PostgreSQL basta un índice por hora, el estado no
tiene límite de antigüedad y se puede consultar con SQL. Spark se queda con lo que hace bien: leer Kafka
por lotes con tope, deduplicar dentro del lote y escribir en Kafka.
:::

## Cada lote, exactamente una vez (en efecto)

`foreachBatch` puede ejecutarse **dos veces** para el mismo lote (lección 16). La fusión del observatorio lo tolera porque:

| Se cae entre… | Qué pasa al repetir el lote |
| --- | --- |
| leer estado y escribir en Kafka | Nada se había escrito. Se repite sin más. |
| escribir en Kafka y el `COMMIT` del estado | Los mensajes ya están en `eventos.live`. Como el estado es el mismo y la decisión es **determinista**, se escriben **los mismos mensajes** (mismo id y versión): el consumidor los descarta por `(event_id, updated)`. |
| el `COMMIT` y `commits/N` | El estado ya tiene esos informes con esa versión: no hay cambios y no se emite nada. |

Determinista significa: nada que dependa del reloj o del azar. La versión del evento es
`max(versión de sus informes, versión anterior + 1)` e `ingested_at` es el máximo de los de sus informes;
nunca `now()`. Por eso un lote repetido produce **bytes idénticos**.

:::nota La prueba, en el observatorio
`specs/013-multifuente/evidencias/resiliencia-fusion.txt`: se bloqueó a propósito la tabla
`fusion.informe` justo después de escribir en Kafka y se mató la fusión con SIGKILL. Al arrancar, el lote
68 se repitió idéntico, el consumidor registró `result=omitido reason=duplicado` y los recuentos finales
no cambiaron.
:::

:::ejercicio Lotes más pequeños
1. Borra el checkpoint de prácticas (`Remove-Item -Recurse ..\practicas\_checkpoint-usgs`).
2. Ejecuta `spark-py 18_kafka_streaming.py 500 2>$null` (el número es `maxOffsetsPerTrigger`).
3. ¿Cuántos lotes salen? ¿Cuántos mensajes por lote? ¿Qué pasa ahora con `informes_distintos`?

:::solucion
```salida
lote=0 mensajes=498 informes_distintos=498 p0:0-162 p1:0-168 p2:0-165
lote=1 mensajes=498 informes_distintos=498 p0:163-325 p1:169-337 p2:166-331
lote=2 mensajes=499 informes_distintos=499 p0:326-489 p1:338-506 p2:332-497
lote=3 mensajes=498 informes_distintos=498 p0:490-652 p1:507-674 p2:498-664
lote=4 mensajes=313 informes_distintos=304 p0:653-755 p1:675-780 p2:665-768
Terminado. Vuelve a ejecutarlo: solo leerá lo nuevo.
```

Cinco lotes: cuatro de unos 500 (el tope se reparte entre las tres particiones en proporción a lo
pendiente; sale 498 o 499 por redondeo) y el último con el resto. Los rangos son contiguos: el lote 1
empieza en 163 justo donde acabó el 0 en la partición 0.

Lo interesante es `informes_distintos`: solo el último lote quita **9** repetidos, no 67. Las revisiones
se publicaron al final, así que casi todas caen en el lote 4 y su original en un lote anterior: **dentro de
cada lote** no hay nada que deduplicar. La ventana solo ve su lote. Para saber que un informe ya pasó
antes hace falta estado entre lotes, que es justo lo que trata la sección anterior (y lo que la lección 23
pone a prueba).
:::

```quiz
¿Por qué la fusión no guarda su estado con un watermark de Spark?
- Porque Spark no permite estado en `foreachBatch`.
* Porque el relleno trae informes de hace meses que un watermark descartaría como tardíos.
- Porque PostgreSQL es más rápido que la memoria.
> El watermark limpia el estado viejo y descarta lo que llega tarde. Con datos históricos llegando ahora, eso sería perder informes.
```

```quiz
La fusión escribe en Kafka y se cae antes de guardar su estado. Al repetir el lote, ¿por qué no aparecen eventos duplicados en el panel?
- Porque Kafka detecta el mensaje repetido con la idempotencia del productor.
* Porque la decisión es determinista: se escribe el mismo mensaje (mismo id y versión) y el consumidor lo descarta.
- Porque Spark borra los mensajes del lote fallido.
> La idempotencia del productor solo cubre reintentos dentro de una sesión. Tras un reinicio, lo que evita el duplicado es el determinismo de la fusión más `ON CONFLICT (event_id, updated) DO NOTHING` en el consumidor.
```
