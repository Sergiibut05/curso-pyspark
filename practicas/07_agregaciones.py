"""Lección 7: agrupar, agregar y unir."""
from pyspark.sql import functions as F

from comun import sesion, terremotos_2025

spark = sesion("agregaciones")
t = terremotos_2025(spark)

(t.groupBy(F.month("hora").alias("mes"))
  .agg(F.count("*").alias("terremotos"),
       F.round(F.avg("mag"), 2).alias("mag_media"),
       F.max("mag").alias("mag_max"))
  .orderBy("mes")
  .show(12))

# Caja de Europa del proyecto: lat 34-72, lon -25-45
europa = t.filter(F.col("lat").between(34, 72) & F.col("lon").between(-25, 45))
print("M >= 4 de USGS en la caja de Europa en 2025:", europa.count())

# Unión con una tabla pequeña: nombre de cada red sismológica
redes = spark.createDataFrame(
    [("us", "USGS NEIC (mundial)"), ("ak", "Alaska"), ("av", "Observatorio volcánico de Alaska"),
     ("ci", "California Sur (Caltech)"), ("nc", "California Norte"), ("pt", "Centro de tsunamis del Pacífico")],
    "red string, nombre string",
)
(t.groupBy("red").count()
  .join(F.broadcast(redes), "red", "left")
  .orderBy(F.col("count").desc())
  .show(6, truncate=False))
spark.stop()
