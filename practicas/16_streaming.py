"""Lección 16: Structured Streaming con una fuente de prueba (rate) y foreachBatch."""
import time

from pyspark.sql import functions as F

from comun import sesion

spark = sesion("streaming")

# 'rate' genera filas (timestamp, value) a ritmo fijo: sirve para ver micro-lotes sin Kafka.
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
spark.stop()
