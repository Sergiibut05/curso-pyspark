"""Prepara el laboratorio: publica en Kafka los terremotos de enero de 2025 de USGS y EMSC (datos/relleno/).

Cada mensaje es el informe original de la fuente dentro de un sobre {"fuente": ..., "event": <feature GeoJSON>},
con el id del informe como clave. USGS recibe además unas cuantas revisiones (mismo id, `updated` más reciente),
como pasa con una fuente de verdad.

Se ejecuta una vez, con el laboratorio en marcha:
    docker compose run --rm spark python 00_cargar_kafka.py
Si los topics ya tienen datos, no hace nada (para empezar de cero: docker compose down -v).
"""
from pyspark.sql import functions as F

from comun import EMSC_ENERO_2025, KAFKA, USGS_ENERO_2025, sesion

spark = sesion("cargar-kafka", kafka=True)


def mensajes_en(topic):
    try:
        return (spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA)
                .option("subscribe", topic).load().count())
    except Exception:  # el topic aún no existe
        return 0


def features(ruta):
    return spark.read.option("multiLine", True).json(ruta).select(F.explode("features").alias("f"))


def escribir(df, fuente, topic):
    """Escribe en Kafka un DataFrame con una columna «f» (el feature GeoJSON)."""
    (df.select(
        F.col("f.id").alias("key"),
        F.to_json(F.struct(F.lit(fuente).alias("fuente"), F.col("f").alias("event"))).alias("value"))
     .write.format("kafka")
     .option("kafka.bootstrap.servers", KAFKA)
     .option("topic", topic)
     .option("kafka.acks", "all")                  # el broker confirma cada lote antes de seguir
     .option("kafka.enable.idempotence", "true")   # reintentos sin duplicados
     .save())                                      # no vuelve hasta que Kafka ha confirmado todo


if mensajes_en("eventos.usgs") or mensajes_en("eventos.emsc"):
    print("Los topics ya tienen datos: no se carga nada. Para empezar de cero: docker compose down -v")
    spark.stop()
    raise SystemExit(0)

usgs = features(USGS_ENERO_2025)
emsc = features(EMSC_ENERO_2025)
# Las revisiones: uno de cada 40 informes (siempre los mismos), con `updated` un minuto más tarde y la
# magnitud corregida. Se escriben DESPUÉS que los originales, en otra escritura, para que cada revisión
# quede detrás de su original en la partición.
revisiones = (usgs.filter(F.abs(F.hash("f.id")) % 40 == 0)
              .withColumn("f", F.col("f")
                          .withField("properties.updated", F.col("f.properties.updated") + 60_000)
                          .withField("properties.mag", F.round(F.col("f.properties.mag") + 0.1, 2))))

escribir(usgs, "usgs", "eventos.usgs")
escribir(revisiones, "usgs", "eventos.usgs")
escribir(emsc, "emsc", "eventos.emsc")
print(f"eventos.usgs: {usgs.count()} informes + {revisiones.count()} revisiones = {mensajes_en('eventos.usgs')} mensajes")
print(f"eventos.emsc: {mensajes_en('eventos.emsc')} mensajes")
spark.stop()
