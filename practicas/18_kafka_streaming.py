"""Lección 18: leer eventos.usgs en streaming con checkpoint, como la fusión (sin escribir nada)."""
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from comun import KAFKA, sesion

CHECKPOINT = "/practicas/_checkpoint-usgs"   # en la carpeta de prácticas: sobrevive entre ejecuciones

spark = sesion("kafka-streaming", kafka=True)
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
print("Terminado. Vuelve a ejecutarlo: solo leerá lo nuevo.")
spark.stop()
