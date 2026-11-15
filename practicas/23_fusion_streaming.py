"""Lección 23: la fusión de la lección 22, ahora en streaming desde Kafka y escribiendo el resultado en Kafka.

Uso: python 23_fusion_streaming.py [mensajes_por_lote] [checkpoint]
  - mensajes_por_lote: tope de maxOffsetsPerTrigger (por defecto, 100000: todo en un lote)
  - checkpoint: nombre de la carpeta de checkpoint dentro de practicas/ (por defecto, _checkpoint-fusion)
"""
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from comun import KAFKA, sesion

POR_LOTE = sys.argv[1] if len(sys.argv) > 1 else "100000"
CHECKPOINT = f"/practicas/{sys.argv[2] if len(sys.argv) > 2 else '_checkpoint-fusion'}"
SALIDA = "eventos.fusionados"

spark = sesion("fusion-streaming", kafka=True)
lectura = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribePattern", r"eventos\.(usgs|emsc)")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", POR_LOTE)
    .load()
)

texto = F.col("value").cast("string")


def informes(lote):
    """Una fila por informe (la versión más nueva dentro del lote), con lo necesario para emparejar."""
    plano = lote.select(
        F.get_json_object(texto, "$.fuente").alias("fuente"),
        F.get_json_object(texto, "$.event.id").alias("id"),
        F.get_json_object(texto, "$.event.properties.updated").alias("version"),
        F.get_json_object(texto, "$.event.properties.evtype").alias("evtype"),
        F.get_json_object(texto, "$.event.properties.mag").cast("double").alias("mag"),
        # USGS da la hora en ms y las coordenadas en la geometría; EMSC, la hora en ISO y lat/lon en propiedades
        # try_cast: con el modo ANSI de Spark 4, cast("long") de un texto ISO falla; try_cast devuelve null
        F.coalesce(F.get_json_object(texto, "$.event.properties.time").try_cast("long"),
                   F.unix_millis(F.to_timestamp(F.get_json_object(texto, "$.event.properties.time")))).alias("t"),
        F.coalesce(F.get_json_object(texto, "$.event.properties.lat"),
                   F.get_json_object(texto, "$.event.geometry.coordinates[1]")).cast("double").alias("lat"),
        F.coalesce(F.get_json_object(texto, "$.event.properties.lon"),
                   F.get_json_object(texto, "$.event.geometry.coordinates[0]")).cast("double").alias("lon"),
        "offset",
    ).filter((F.col("fuente") == "usgs") | (F.col("evtype") == "ke"))
    v = Window.partitionBy("fuente", "id").orderBy(F.col("version").desc(), F.col("offset").desc())
    return plano.withColumn("n", F.row_number().over(v)).filter("n = 1").drop("n", "evtype", "offset")


def rad(c):
    return F.radians(F.col(c))


def fusionar(lote, lote_id):
    inf = informes(lote).cache()
    usgs = inf.filter("fuente = 'usgs'").select([F.col(c).alias(f"{c}_usgs") for c in ("id", "t", "lat", "lon", "mag")])
    emsc = inf.filter("fuente = 'emsc'").select([F.col(c).alias(f"{c}_emsc") for c in ("id", "t", "lat", "lon", "mag")])
    # Mismo truco que en la lección 22: cubos de un minuto para no comparar todo con todo.
    usgs_c = usgs.withColumn("cubo", F.floor(F.col("t_usgs") / 60000))
    emsc_c = emsc.withColumn("cubo", F.explode(F.array(*[F.floor(F.col("t_emsc") / 60000) + d for d in (-1, 0, 1)])))
    km = 2 * 6371 * F.asin(F.sqrt(
        F.pow(F.sin((rad("lat_emsc") - rad("lat_usgs")) / 2), 2)
        + F.cos(rad("lat_usgs")) * F.cos(rad("lat_emsc")) * F.pow(F.sin((rad("lon_emsc") - rad("lon_usgs")) / 2), 2)))
    pares = (usgs_c.join(emsc_c, "cubo")
             .withColumn("dt_s", F.abs(F.col("t_usgs") - F.col("t_emsc")) / 1000).filter("dt_s <= 30")
             .withColumn("km", km).filter("km <= 100")
             .withColumn("puntuacion", F.col("dt_s") / 30 + F.col("km") / 100))
    mejor = Window.partitionBy("id_usgs").orderBy("puntuacion", "id_emsc")
    elegidos = pares.withColumn("n", F.row_number().over(mejor)).filter("n = 1").select("id_usgs", "id_emsc")
    # Salida: un mensaje por informe de USGS, con su pareja de EMSC si la hay. Clave = id de USGS.
    salida = (usgs.join(elegidos, "id_usgs", "left")
              .select(F.col("id_usgs").alias("key"),
                      F.to_json(F.struct("id_usgs", "id_emsc", F.col("mag_usgs").alias("mag"),
                                         F.timestamp_millis("t_usgs").alias("hora"))).alias("value")))
    (salida.write.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("topic", SALIDA)
     .option("kafka.acks", "all").option("kafka.enable.idempotence", "true").save())
    n_usgs, n_emsc, n_pares = usgs.count(), emsc.count(), elegidos.count()
    print(f"lote={lote_id} mensajes={lote.count()} usgs={n_usgs} emsc={n_emsc} con_pareja={n_pares}")
    inf.unpersist()


consulta = (lectura.writeStream.foreachBatch(fusionar)
            .option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True)
            .start())
consulta.awaitTermination()
print("Terminado.")
spark.stop()
