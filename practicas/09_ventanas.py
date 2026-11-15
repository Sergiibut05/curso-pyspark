"""Lección 9: funciones de ventana. La de la fusión y dos más."""
from pyspark.sql import Window
from pyspark.sql import functions as F

from comun import sesion, terremotos_2025

spark = sesion("ventanas")

# 1) La ventana de fusion/job.py: quedarse con la última versión de cada informe del lote
lote = spark.createDataFrame(
    [("eventos.usgs", 1, 100, "us6000pjr6", "1742247237040"),
     ("eventos.usgs", 1, 101, "us6000pjr6", "1742247297040"),   # revisión posterior
     ("eventos.usgs", 1, 102, "us6000pjr6", "1742247297040"),   # reenvío de la misma versión
     ("eventos.usgs", 0, 55, "us6000pgrt", "1742247000000"),
     ("eventos.emsc", 2, 7, "20250101_0000001", "2025-01-01T18:25:52.893201Z")],
    "topic string, partition int, offset long, id string, version string",
)
v = Window.partitionBy("topic", "id").orderBy(F.col("version").desc_nulls_last(), F.col("offset").desc())
lote.withColumn("n", F.row_number().over(v)).show(truncate=False)
lote.withColumn("n", F.row_number().over(v)).filter("n = 1").drop("n").show(truncate=False)

# 2) El mayor terremoto de cada mes
t = terremotos_2025(spark)
por_mes = Window.partitionBy(F.month("hora")).orderBy(F.col("mag").desc(), "hora")
(t.withColumn("puesto", F.row_number().over(por_mes)).filter("puesto = 1")
  .select(F.month("hora").alias("mes"), "mag", "lugar").orderBy("mes").show(12, truncate=False))

# 3) Tiempo desde el terremoto anterior (lag) en una zona: Kamchatka tras el M 8,8
kam = t.filter(F.col("lat").between(50, 56) & F.col("lon").between(155, 165)
               & (F.col("hora") >= "2025-07-29 23:00:00") & (F.col("hora") < "2025-07-30 01:00:00"))
orden = Window.orderBy("hora")
(kam.withColumn("min_desde_anterior",
                F.round((F.unix_timestamp("hora") - F.unix_timestamp(F.lag("hora").over(orden))) / 60, 1))
    .select("hora", "mag", "min_desde_anterior").show(8, truncate=False))
print("Réplicas M >= 4 en las dos primeras horas (en la caja):", kam.count())
spark.stop()
