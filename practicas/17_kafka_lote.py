"""Lección 17: leer Kafka con Spark en modo lote (sin streaming) para ver qué columnas da."""
from pyspark.sql import functions as F

from comun import KAFKA, sesion

spark = sesion("kafka-lote", kafka=True)

kafka = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", "eventos.usgs")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)
kafka.printSchema()

print("Mensajes por partición:")
kafka.groupBy("partition").agg(F.count("*").alias("mensajes"), F.min("offset"), F.max("offset")).orderBy("partition").show()

texto = F.col("value").cast("string")
(kafka.select(
    F.col("key").cast("string").alias("clave"),
    "partition", "offset",
    F.get_json_object(texto, "$.event.properties.mag").cast("double").alias("mag"),
    F.get_json_object(texto, "$.event.properties.place").alias("lugar"),
 )
 .orderBy(F.col("offset").desc())
 .show(5, truncate=False))

# Solo un trozo concreto: offsets por partición en JSON
trozo = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", "eventos.usgs")
    .option("startingOffsets", '{"eventos.usgs": {"0": 100, "1": 100, "2": 100}}')
    .option("endingOffsets", '{"eventos.usgs": {"0": 110, "1": 105, "2": 101}}')
    .load()
)
print("Mensajes en el trozo:", trozo.count())
spark.stop()
