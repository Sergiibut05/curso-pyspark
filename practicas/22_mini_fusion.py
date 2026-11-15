"""Lección 22: la regla de la fusión (≤ 30 s y ≤ 100 km) sobre enero de 2025, en lote y solo con Spark."""
from pyspark.sql import Window
from pyspark.sql import functions as F

from comun import EMSC_ENERO_2025, USGS_ENERO_2025, sesion

spark = sesion("mini-fusion")


def features(ruta):
    # Estos ficheros tienen un salto de línea por terremoto: hace falta multiLine
    return spark.read.option("multiLine", True).json(ruta).select(F.explode("features").alias("f"))


usgs = features(USGS_ENERO_2025).select(
    F.col("f.id").alias("id_usgs"),
    F.col("f.properties.time").alias("t_usgs"),                    # ms desde 1970
    F.col("f.geometry.coordinates")[1].alias("lat_usgs"),
    F.col("f.geometry.coordinates")[0].alias("lon_usgs"),
    F.col("f.properties.mag").alias("mag_usgs"),
)
emsc = features(EMSC_ENERO_2025).filter(F.col("f.properties.evtype") == "ke").select(
    F.col("f.id").alias("id_emsc"),
    F.unix_millis(F.to_timestamp("f.properties.time")).alias("t_emsc"),  # ISO -> ms
    F.col("f.properties.lat").alias("lat_emsc"),
    F.col("f.properties.lon").alias("lon_emsc"),
    F.col("f.properties.mag").alias("mag_emsc"),
)
print("Informes USGS:", usgs.count(), "| EMSC (ke):", emsc.count())

# Unir todo con todo serían 2.200 x 6.000 = 13 millones de pares. Truco: cubos de un minuto.
# Cada informe de EMSC se apunta a su minuto y a los dos vecinos; así un par a <= 30 s siempre comparte cubo.
usgs_c = usgs.withColumn("cubo", F.floor(F.col("t_usgs") / 60000))
emsc_c = emsc.withColumn("cubo", F.explode(F.array(*[F.floor(F.col("t_emsc") / 60000) + d for d in (-1, 0, 1)])))

def rad(c):
    return F.radians(F.col(c))

haversine = 2 * 6371 * F.asin(F.sqrt(
    F.pow(F.sin((rad("lat_emsc") - rad("lat_usgs")) / 2), 2)
    + F.cos(rad("lat_usgs")) * F.cos(rad("lat_emsc")) * F.pow(F.sin((rad("lon_emsc") - rad("lon_usgs")) / 2), 2)
))

pares = (
    usgs_c.join(emsc_c, "cubo")
    .withColumn("dt_s", F.abs(F.col("t_usgs") - F.col("t_emsc")) / 1000)
    .filter(F.col("dt_s") <= 30)
    .withColumn("km", haversine)
    .filter(F.col("km") <= 100)
    .withColumn("puntuacion", F.col("dt_s") / 30 + F.col("km") / 100)
)
print("Pares candidatos (<= 30 s y <= 100 km):", pares.count())

# Un informe de USGS se queda con su mejor candidato EMSC (menor puntuación), como en emparejar.py
mejor = Window.partitionBy("id_usgs").orderBy("puntuacion", "id_emsc")
elegidos = pares.withColumn("n", F.row_number().over(mejor)).filter("n = 1")
total_usgs = usgs.count()
con_pareja = elegidos.count()
print(f"USGS con pareja EMSC: {con_pareja} de {total_usgs} ({100 * con_pareja / total_usgs:.1f} %)")
print("USGS con dos o más candidatos:", pares.groupBy("id_usgs").count().filter("count > 1").count())

elegidos.select("id_usgs", "id_emsc", F.round("dt_s", 1).alias("dt_s"), F.round("km", 1).alias("km"),
                "mag_usgs", "mag_emsc").orderBy(F.col("mag_usgs").desc()).show(5)
elegidos.select(F.transform(F.percentile_approx("dt_s", [0.5, 0.95]), lambda x: F.round(x, 1)).alias("dt_p50_p95"),
                F.transform(F.percentile_approx("km", [0.5, 0.95]), lambda x: F.round(x, 1)).alias("km_p50_p95")).show(truncate=False)
spark.stop()
