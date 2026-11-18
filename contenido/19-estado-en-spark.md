---
titulo: Estado en Spark - ventanas de tiempo, watermark y duplicados
resumen: Agregaciones en streaming que recuerdan entre lotes. Tiempo del evento, ventanas, watermark, modos de salida, datos tardíos, duplicados y joins entre flujos.
bloque: Streaming
minutos: 30
practica: si
---

En la lección 18 viste que `foreachBatch` solo conoce **su** lote: para recordar entre lotes, la fusión del
observatorio guarda su estado en PostgreSQL. Spark también sabe guardar estado **él mismo**, en el
checkpoint, para un conjunto de operaciones: agregaciones por ventanas de tiempo, eliminación de
duplicados y joins entre dos flujos. Esta lección es ese estado gestionado por Spark: cuándo sirve y
cuándo no.

## Tiempo del evento

Un terremoto tiene la hora en que **ocurrió** (tiempo del evento) y la hora en que **llega** a tu proceso
(tiempo de procesamiento). Contar «terremotos por semana» tiene sentido con la primera: el resultado no
puede depender de si el proceso estuvo parado o de si una agencia publicó tarde. El problema es que los
eventos **no llegan en orden**: uno de febrero puede llegar en diciembre.

## Ventanas y watermark

```python fragmento
por_semana = (lectura.withWatermark("hora", "1 day")
              .groupBy(F.window("hora", "7 days")).agg(F.count("*").alias("n")))
```

- `F.window("hora", "7 days")` asigna cada evento a su **ventana** fija de 7 días según su hora. Con un
  tercer argumento (`"7 days", "1 day"`) las ventanas se solapan (deslizantes). `F.session_window` crea
  ventanas de sesión, que se cierran tras un hueco sin eventos.
- `withWatermark("hora", "1 day")` declara el **retraso permitido**. Spark lleva la cuenta de la mayor hora
  vista y calcula la marca de agua (**watermark**) = mayor hora vista − 1 día. Lo que llegue con una hora
  anterior a la watermark es **tardío** y se descarta.

Gracias a la watermark Spark sabe cuándo una ventana **no puede cambiar más**: cuando la watermark pasa su
final. Entonces la emite y **borra su estado**. Sin watermark, tendría que guardar todas las ventanas para
siempre por si llega algo viejo.

## Modos de salida

| Modo | Qué emite cada lote | Con ventanas y watermark |
| --- | --- | --- |
| `append` | Solo filas **definitivas**, una vez. | Cada ventana cuando se cierra. Ideal para escribir en ficheros o Kafka. |
| `update` | Las filas que **han cambiado** en este lote. | Cada ventana cada vez que cambia: resultados provisionales. |
| `complete` | La tabla de resultados **entera**. | Todas las ventanas siempre: solo para resultados pequeños (pantallas, pruebas). |

## Míralo

El script publica en el topic `sismos.2025` los 18.302 terremotos de 2025 **en orden de hora**, salvo 30 de
febrero que publica **al final** (tardíos), y repite los 40 últimos del año (duplicados). Después los lee
en streaming con lotes de 3.000 mensajes y cuenta por semanas en modo `append`.

```powershell pruebalo
docker compose up -d
spark-py 19_estado.py 2>$null
```

```salida
sismos.2025: 18342 mensajes
lote 1: cierra 10 semana(s): 26/12=60, 02/01=415, 09/01=296, 16/01=299 …
lote 2: cierra 10 semana(s): 06/03=246, 13/03=263, 20/03=249, 27/03=299 …
lote 3: cierra 10 semana(s): 15/05=233, 22/05=253, 29/05=245, 05/06=243 …
lote 4: cierra 3 semana(s): 24/07=1023, 31/07=1123, 07/08=537
lote 5: cierra 9 semana(s): 14/08=362, 21/08=346, 28/08=331, 04/09=307 …
lote 6: cierra 9 semana(s): 16/10=297, 23/10=296, 30/10=356, 06/11=357 …
lote 7: cierra 1 semana(s): 18/12=287
retraso permitido 1 day: 8 lotes, descartados por tardíos: 2
estado al terminar: 2 ventana(s) abiertas, 4 KB
```

Cómo leerlo:

- **El lote 0 no emite nada**: en el primer lote no hay watermark todavía (se calcula al terminar cada
  lote, con lo visto hasta entonces).
- **Cada semana sale una sola vez**, cuando la watermark la deja atrás, con su recuento definitivo. Eso es
  `append`.
- **Las semanas empiezan en jueves** (26/12, 02/01…): las ventanas se alinean con el 1 de enero de 1970, que
  fue jueves. Para que empiecen en lunes, `F.window("hora", "7 days", startTime="4 days")`.
- **Julio** (24/07 = 1.023, 31/07 = 1.123): la secuencia de réplicas del M 8,8 de Kamchatka del 29 de julio.
- **El estado es pequeño**: al terminar, solo quedan las 2 últimas semanas abiertas (4 KB). La watermark ha
  ido borrando las cerradas. Las últimas no se emiten porque ningún evento posterior empuja la watermark:
  en un flujo de verdad, se cerrarían al llegar datos nuevos.
- **Los 30 tardíos de febrero se han descartado.** Llegan cuando la watermark ya está en diciembre.

:::cuidado El contador de descartados no cuenta eventos
`numRowsDroppedByWatermark` dice **2**, no 30. En una agregación, Spark agrega primero dentro de cada
partición y luego combina con el estado; el contador mide filas **en ese punto**, que ya son resúmenes
(una por ventana y partición). Los 30 tardíos de la primera semana de febrero, repartidos en dos
particiones, son 2 resúmenes. Para contar eventos tardíos, mira una operación sin agregación previa, como la
siguiente.
:::

## Duplicados

Las fuentes repiten: reintentos, reenvíos, una agencia que vuelve a publicar. `dropDuplicates` en streaming
tendría que recordar **todos** los ids vistos, para siempre. `dropDuplicatesWithinWatermark` recuerda cada
id solo mientras la watermark no lo cubra:

```python fragmento
sin_repetidos = lectura.withWatermark("hora", "1 day").dropDuplicatesWithinWatermark(["id"])
```

```salida
sin repetidos: 18272 filas emitidas, 30 descartadas por tardías
```

18.342 mensajes − 40 repetidos − 30 tardíos = **18.272**. Los 40 repetidos se eliminan porque llegan dentro
del día de retraso de su original; los 30 de febrero, porque llegan tarde. Aquí el contador sí cuenta
eventos: no hay agregación antes.

## Joins entre dos flujos

Spark también puede unir **dos flujos** (por ejemplo, USGS y EMSC), guardando en el estado los eventos de
cada lado mientras puedan tener pareja. Necesita una watermark en cada lado y una **condición de tiempo**
que acote cuánto hay que esperar. La forma, como esquema (no es una de las prácticas del curso):

```python fragmento
usgs = flujo_usgs.withWatermark("t_usgs", "1 hour")
emsc = flujo_emsc.withWatermark("t_emsc", "1 hour")
pares = usgs.join(emsc, F.expr("""
    t_emsc BETWEEN t_usgs - INTERVAL 30 SECONDS AND t_usgs + INTERVAL 30 SECONDS
    AND abs(lat_usgs - lat_emsc) < 1"""))
```

Es la fusión de la lección 23 sin perder parejas entre lotes… **siempre que los dos informes lleguen con
menos de una hora de diferencia**. Lo que llegue más tarde no encontrará pareja: el estado ya se habrá
limpiado.

## Cuándo no sirve el estado de Spark

Es exactamente el motivo por el que la fusión del observatorio guarda el estado en PostgreSQL (lección 18):
cargó de golpe **meses de datos históricos**. Con una watermark razonable, todo ese histórico sería
«tardío» y se descartaría; con una watermark de meses, el estado guardaría meses de eventos en memoria.

| Estado de Spark | Estado externo (una base de datos) |
| --- | --- |
| Lo gestiona Spark: checkpoint, recuperación, limpieza. | Lo gestionas tú: tablas, índices, transacciones. |
| Necesita datos **más o menos en orden**, con retraso acotado. | Admite cualquier orden y cualquier antigüedad. |
| Solo para lo que ofrece Spark (ventanas, duplicados, joins, `transformWithState`). | Cualquier lógica. |
| Rápido: memoria y disco local (RocksDB opcional). | Una consulta a la base por lote. |

:::nota Estado a medida
Para lógica que no encaja en ventanas ni joins (máquinas de estados, contadores por clave con caducidad
propia), Spark 4 tiene `transformWithStateInPandas`, sucesor de `applyInPandasWithState`: una función tuya
recibe los eventos de cada clave y su estado guardado, y decide qué emitir y cuándo caduca.
:::

:::ejercicio Cambia el retraso permitido
Ejecuta `spark-py 19_estado.py "60 days"` y `spark-py 19_estado.py "365 days"`. Para cada uno: ¿cuántos
eventos tardíos se descartan en la consulta de duplicados?, ¿cuántas semanas cierra cada lote?, ¿cuánto
estado queda al final?

:::solucion
SOLUCION_19
:::

```quiz
¿Para qué sirve la watermark en una agregación por ventanas?
* Para saber cuándo una ventana ya no puede cambiar, emitirla y borrar su estado; lo que llega más tarde se descarta.
- Para ordenar los eventos por hora antes de agregar.
- Para limitar cuántos mensajes se leen por lote.
> Sin watermark, Spark tendría que guardar todas las ventanas para siempre.
```

```quiz
En modo `append`, ¿cuándo aparece una ventana en la salida?
- En cada lote en que recibe eventos.
* Una sola vez, cuando la watermark pasa su final.
- Al terminar la consulta.
> update emitiría resultados provisionales en cada cambio; append solo los definitivos.
```

```quiz
Vas a cargar dos años de datos históricos en una consulta en streaming que agrupa por ventanas de una hora con watermark de 10 minutos. ¿Qué pasa?
- Spark ordena el histórico antes de procesarlo.
* Si el histórico no llega en orden de hora, gran parte se descartará como tardío.
- Funciona igual que con datos en directo.
> Por eso el observatorio guarda el estado de su fusión en una base de datos.
```
