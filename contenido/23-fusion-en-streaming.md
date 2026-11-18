---
titulo: "Proyecto: la fusión en streaming, de Kafka a Kafka"
resumen: La regla de la lección 22 dentro de foreachBatch, leyendo los dos topics y escribiendo el resultado en Kafka. Checkpoint, reejecuciones y el fallo que aparece cuando las parejas caen en lotes distintos.
bloque: Proyecto
minutos: 30
practica: si
---

En la lección 22 la fusión leía dos ficheros, una vez. Un servicio de verdad lee de **Kafka**, sin parar,
y publica lo que decide en **otro topic**. Esta lección junta las piezas de las lecciones 16 a 22 en un
script de unas cien líneas, `23_fusion_streaming.py`, y lo pone a prueba.

```text
eventos.usgs ──┐                                          ┌──► eventos.fusionados
               ├──► readStream ──► foreachBatch(fusionar) ┤     (un mensaje por informe de USGS,
eventos.emsc ──┘    (subscribePattern)                    │      con su pareja de EMSC si la tiene)
                                                          └──► checkpoint: por dónde va
```

Necesitas los topics cargados (lección 17, `00_cargar_kafka.py`).

```python preparacion
from pyspark.sql import Window
from pyspark.sql import functions as F

from comun import KAFKA, sesion

spark = sesion("fusion-streaming", kafka=True)
POR_LOTE = "100000"   # el valor por defecto del script (su primer argumento): todo en un lote
```

## La lectura

```python titulo="practicas/23_fusion_streaming.py (lectura)"
lectura = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribePattern", r"eventos\.(usgs|emsc)")   # las dos fuentes en un solo flujo
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", POR_LOTE)              # tope de mensajes por lote (argumento)
    .load()
)
```

Un solo flujo con las dos fuentes: cada mensaje dice en su sobre de qué fuente viene (`$.fuente`).

## Cada lote

`fusionar(lote, lote_id)` hace, sobre el DataFrame estático del lote:

1. **Aplanar los dos formatos** a las mismas columnas (`id`, `t`, `lat`, `lon`, `mag`) con
   `get_json_object` y `coalesce`: USGS da la hora en milisegundos y las coordenadas en la geometría; EMSC,
   la hora en texto ISO y las coordenadas en sus propiedades (lección 5). De EMSC, solo `evtype = ke`.
   :::cuidado try_cast, no cast
   La primera versión del script hacía `cast("long")` de la hora para las dos fuentes, esperando `null` en
   EMSC (que la trae como texto). En Spark 4 el **modo ANSI** está activado por defecto y ese `cast` no
   devuelve `null`: **falla** el lote entero con `CAST_INVALID_INPUT … '2025-01-01T00:20:18.0Z' … cannot be
   cast to "BIGINT"`. Para «convertir si se puede y si no, null», `try_cast("long")`.
   :::

2. **Quedarse con la última versión** de cada informe dentro del lote: la ventana de la lección 9, por
   `fuente` e `id`, ordenada por `updated` y, a igualdad, por offset.
3. **Emparejar** con la regla de la lección 22 (≤ 30 s, ≤ 100 km, cubos de un minuto, mejor puntuación).
4. **Escribir** en `eventos.fusionados` un mensaje por informe de USGS, con clave el id de USGS y valor
   `{"id_usgs", "id_emsc", "mag", "hora"}` (`id_emsc` nulo si no tiene pareja), con `acks=all` e
   idempotencia (lección 17).

```python fragmento titulo="practicas/23_fusion_streaming.py (escritura y consulta)"
    salida = (usgs.join(elegidos, "id_usgs", "left")
              .select(F.col("id_usgs").alias("key"),
                      F.to_json(F.struct("id_usgs", "id_emsc", F.col("mag_usgs").alias("mag"),
                                         F.timestamp_millis("t_usgs").alias("hora"))).alias("value")))
    (salida.write.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("topic", SALIDA)
     .option("kafka.acks", "all").option("kafka.enable.idempotence", "true").save())

consulta = (lectura.writeStream.foreachBatch(fusionar)
            .option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True)          # procesa lo que haya y termina
            .start())
```

El lote se usa varias veces (contar, emparejar, escribir), así que los informes aplanados se guardan con
`cache()` y se liberan al final (lección 4: sin eso, cada acción volvería a leer el lote de Kafka).

## Ejecútalo

```powershell pruebalo
spark-py 23_fusion_streaming.py 2>$null
spark-py 23_fusion_streaming.py 2>$null
```

```salida titulo="Primera ejecución"
lote=0 mensajes=8267 usgs=2239 emsc=5948 con_pareja=1547
Terminado.
```

```salida titulo="Segunda ejecución, sin datos nuevos"
Terminado.
```

- **Un solo lote** con los 8.267 mensajes de los dos topics (2.306 de USGS, revisiones incluidas, y 5.961
  de EMSC): el tope por defecto es de 100.000.
- Tras quedarse con la última versión, **2.239** informes de USGS y **5.948** terremotos de EMSC, y **1.547
  parejas**: exactamente el resultado de la lección 22, ahora de Kafka a Kafka.
- La segunda ejecución no hace nada: el checkpoint (`practicas/_checkpoint-fusion/`) ya lo tiene todo.

## Lotes pequeños: el fallo

Ahora lo mismo con **lotes de 500 mensajes** y otro checkpoint (para empezar desde el principio sin borrar
el anterior):

```powershell pruebalo
spark-py 23_fusion_streaming.py 500 _checkpoint-fusion-500 2>$null
```

```salida
lote=0 mensajes=498 usgs=138 emsc=359 con_pareja=91
lote=1 mensajes=498 usgs=138 emsc=359 con_pareja=63
lote=2 mensajes=498 usgs=138 emsc=358 con_pareja=58
lote=3 mensajes=497 usgs=138 emsc=359 con_pareja=42
…
lote=14 mensajes=495 usgs=138 emsc=357 con_pareja=83
lote=15 mensajes=497 usgs=140 emsc=355 con_pareja=59
lote=16 mensajes=317 usgs=88 emsc=228 con_pareja=17
Terminado.
```

Diecisiete lotes que, sumados, encuentran **1.090 parejas, no 1.547**. Los mismos datos y la misma regla,
y un 30 % menos de parejas. ¿Por qué?

Cada lote es un trozo de **offsets**, no de tiempo: lleva unos 140 mensajes de USGS y unos 360 de EMSC,
según cómo estén ordenados en sus particiones. Si el informe de USGS de un terremoto cae en el lote 3 y el
de EMSC en el lote 4, **ninguno de los dos lotes ve la pareja completa**. `foreachBatch` solo conoce su
lote: no tiene memoria de los anteriores.

Y hay un efecto más sutil. Las revisiones de USGS se publicaron al final (lección 17), así que caen en los
últimos lotes, lejos de su informe de EMSC. Cuando llega una revisión, el script escribe un mensaje nuevo
para ese terremoto **sin pareja**, que sustituye al anterior que sí la tenía. El resultado empeora con
datos más nuevos. Lo verás en el ejercicio.

:::defensa Por eso la fusión del observatorio guarda estado fuera de Spark
Emparejar solo dentro del lote funciona si las dos versiones de un terremoto llegan juntas. En la vida real
no: USGS publica en minutos, EMSC a veces en horas, y un relleno histórico llega en orden de fuente, no de
hora. La fusión del observatorio guarda en PostgreSQL los informes ya vistos y, en cada lote, consulta los
**cercanos en el tiempo** a los del lote (lección 18, opción B). Así una pareja se encuentra aunque sus dos
mitades lleguen con días de diferencia. El tamaño del lote pasa a ser una cuestión de rendimiento, no de
resultado.
:::

## Reejecuciones y duplicados

Sin datos nuevos, una segunda ejecución no hace nada: el checkpoint sabe por dónde iba (lección 16). Pero
¿y si un lote se repite (el proceso cae después de escribir en Kafka y antes de `commits/N`)? Entonces
`eventos.fusionados` recibiría los mismos mensajes otra vez. No es grave **porque la salida es
determinista**: el mismo lote produce exactamente los mismos mensajes, con la misma clave. Quien lea
`eventos.fusionados` quedándose con el último valor de cada clave (o un topic compactado) no ve ninguna
diferencia. Es el «exactamente una vez en efecto» de la lección 18.

:::ejercicio Cuenta la salida
Con `spark.read` (en lote) sobre `eventos.fusionados`, cuenta cuántos mensajes hay, cuántas claves
distintas y cuántas claves tienen un `id_emsc` no nulo **en su último mensaje**. Compara con lo que
imprimieron las ejecuciones. ¿Por qué hay más mensajes que claves?

:::solucion
```python
f = (spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("subscribe", "eventos.fusionados")
     .option("startingOffsets", "earliest").load()
     .select(F.col("key").cast("string").alias("clave"), "partition", "offset",
             F.get_json_object(F.col("value").cast("string"), "$.id_emsc").alias("id_emsc")))
ultimo = Window.partitionBy("clave").orderBy(F.col("offset").desc())   # misma clave -> misma partición
ultimos = f.withColumn("n", F.row_number().over(ultimo)).filter("n = 1")
print("mensajes:", f.count(), "| claves distintas:", ultimos.count(),
      "| claves con pareja en su último mensaje:", ultimos.filter(F.col("id_emsc").isNotNull()).count())
```

```salida
mensajes: 4544 | claves distintas: 2239 | claves con pareja en su último mensaje: 1060
```

- **4.544 mensajes** = 2.239 de la ejecución en un lote + 2.305 de la de lotes de 500. Esta última escribe
  un mensaje por cada versión de un informe de USGS que ve, y las revisiones caen en otro lote que su
  original.
- **2.239 claves**: un informe de USGS por clave, como debe ser.
- **1.060 con pareja en su último mensaje**, aún menos que las 1.090 que contó la ejecución por lotes. La
  ejecución de lotes pequeños se escribió **después**, así que sus decisiones han sustituido a las buenas
  (1.547). Y algunas revisiones sin pareja han deshecho parejas encontradas en lotes anteriores.

Moraleja: dos ejecuciones con checkpoints distintos sobre el mismo topic de salida no se «suman», se
**pisan**. Y una salida que depende de cómo se cortan los lotes no es determinista en el sentido que
importa: los mismos datos dan resultados distintos.
:::

```quiz
¿Por qué la versión con lotes de 500 encuentra menos parejas que la de un solo lote?
- Porque Spark descarta mensajes cuando el lote es pequeño.
* Porque cada lote solo empareja lo que hay dentro de él: si las dos mitades de una pareja caen en lotes distintos, no se ven.
- Porque `maxOffsetsPerTrigger` cambia la regla de 30 s y 100 km.
> Sin estado entre lotes, el resultado depende de cómo se corten los lotes. Un servicio real guarda los informes vistos (lección 18).
```

```quiz
El proceso cae después de escribir un lote en `eventos.fusionados` y antes de que Spark escriba `commits/N`. ¿Qué pasa?
- Se pierde el lote.
* Al arrancar se repite el lote y se escriben otra vez los mismos mensajes, idénticos.
- Spark detecta que ya estaba escrito y no lo repite.
> El checkpoint garantiza repetir el mismo rango de offsets; la salida determinista hace que la repetición no cambie el resultado para quien se queda con el último valor por clave.
```

```quiz
¿Qué decide dónde empieza la segunda ejecución del script?
- `startingOffsets = earliest`.
* El checkpoint: `startingOffsets` solo cuenta la primera vez.
- Los grupos de consumidores de Kafka.
> Spark guarda los offsets en su checkpoint, no en Kafka.
```
