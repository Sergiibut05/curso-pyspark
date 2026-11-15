"""Lección 19: estado gestionado por Spark. Ventanas por tiempo del evento con watermark, datos que llegan tarde
y duplicados eliminados dentro del watermark.

Uso: python 19_estado.py [retraso_permitido]   (por defecto, "1 day")
Escribe el topic sismos.2025 la primera vez; los checkpoints van a practicas/_salida/estado/.
"""
import shutil
import sys

from pyspark.sql import functions as F

from comun import KAFKA, SALIDA, sesion, terremotos_2025

RETRASO = sys.argv[1] if len(sys.argv) > 1 else "1 day"
TOPIC = "sismos.2025"
spark = sesion("estado", kafka=True)


def mensajes_en(topic):
    try:
        return spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("subscribe", topic).load().count()
    except Exception:
        return 0


# 1. Los datos: el año en orden de hora, salvo 30 terremotos de febrero que se publican al FINAL (tardíos),
#    y los 40 últimos del año publicados dos veces (repetidos, dentro del retraso permitido).
if not mensajes_en(TOPIC):
    t = terremotos_2025(spark).select("id", "hora", "mag")
    tardios = t.filter(F.month("hora") == 2).orderBy("hora").limit(30)
    repetidos = t.orderBy(F.desc("hora")).limit(40)
    a_tiempo = t.join(tardios.select("id"), "id", "left_anti")
    for parte in (a_tiempo.orderBy("hora"), repetidos, tardios):       # tres escrituras, en este orden
        (parte.select(F.col("id").alias("key"), F.to_json(F.struct("id", "hora", "mag")).alias("value"))
         .coalesce(1).write.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("topic", TOPIC).save())
print(f"{TOPIC}: {mensajes_en(TOPIC)} mensajes")

lectura = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("subscribe", TOPIC)
           .option("startingOffsets", "earliest").option("maxOffsetsPerTrigger", 3000).load()
           .select(F.from_json(F.col("value").cast("string"), "id STRING, hora TIMESTAMP, mag DOUBLE").alias("e"))
           .select("e.*"))


def ejecutar(nombre, consulta, por_lote):
    """Ejecuta una consulta en streaming hasta agotar el topic y devuelve su progreso."""
    shutil.rmtree(f"{SALIDA}/estado/{nombre}", ignore_errors=True)
    q = (consulta.writeStream.outputMode("append").foreachBatch(por_lote)
         .option("checkpointLocation", f"{SALIDA}/estado/{nombre}").trigger(availableNow=True).start())
    q.awaitTermination()
    return q.recentProgress


# 2. Recuento por semanas según la hora del evento, con watermark
def semanas(lote, lote_id):
    filas = lote.orderBy("window").collect()
    if filas:
        print(f"lote {lote_id}: cierra {len(filas)} semana(s): "
              + ", ".join(f"{r.window.start:%d/%m}={r.n}" for r in filas[:4]) + (" …" if len(filas) > 4 else ""))


por_semana = (lectura.withWatermark("hora", RETRASO)
              .groupBy(F.window("hora", "7 days")).agg(F.count("*").alias("n")))
progreso = ejecutar("semanas", por_semana, semanas)
descartados = sum(op["numRowsDroppedByWatermark"] for p in progreso for op in p["stateOperators"])
print(f"retraso permitido {RETRASO}: {len(progreso)} lotes, descartados por tardíos: {descartados}")
ultimo = progreso[-1]["stateOperators"][0]
print(f"estado al terminar: {ultimo['numRowsTotal']} ventana(s) abiertas, {ultimo['memoryUsedBytes'] / 1e3:.0f} KB")

# 3. Duplicados: dropDuplicatesWithinWatermark recuerda los ids vistos mientras no los cubra el watermark
total = {"filas": 0}


def contar(lote, lote_id):
    total["filas"] += lote.count()


sin_repetidos = lectura.withWatermark("hora", RETRASO).dropDuplicatesWithinWatermark(["id"])
progreso = ejecutar("duplicados", sin_repetidos, contar)
descartados = sum(op["numRowsDroppedByWatermark"] for p in progreso for op in p["stateOperators"])
print(f"sin repetidos: {total['filas']} filas emitidas, {descartados} descartadas por tardías")
spark.stop()
