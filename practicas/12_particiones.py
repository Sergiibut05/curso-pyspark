"""Lección 12: particiones, shuffle y lo que cuesta traer datos al driver."""
from pyspark.sql import functions as F

from comun import sesion, terremotos_2025

spark = sesion("particiones")
t = terremotos_2025(spark)

print("Particiones tras leer:", t.rdd.getNumPartitions())
print("Filas por partición:", t.rdd.glom().map(len).collect())

r = t.repartition(8, "red")
print("Tras repartition(8, 'red'):", r.rdd.glom().map(len).collect())
print("Tras coalesce(2):", r.coalesce(2).rdd.glom().map(len).collect())

agg = t.groupBy("red").count()
print("Particiones tras groupBy (spark.sql.shuffle.partitions=4):", agg.rdd.getNumPartitions())
agg.explain()

# take/limit traen pocas filas; collect trae TODO al driver
print("take(2):", [f.id for f in t.take(2)])
print("collect() traería", t.count(), "filas al proceso de Python")
spark.stop()
