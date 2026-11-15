"""Lección 2: leer un GeoJSON real y convertirlo en una tabla."""
from pyspark.sql import functions as F

from comun import USGS_2025, sesion

spark = sesion("dataframes")

crudo = spark.read.json(USGS_2025)          # un único objeto JSON: una fila con un array enorme
print("Filas del fichero:", crudo.count())
crudo.printSchema()                          # Spark ha deducido el esquema leyendo el fichero

# explode: una fila por elemento del array
features = crudo.select(F.explode("features").alias("f"))
print("Terremotos:", features.count())

tabla = features.select(
    F.col("f.id").alias("id"),
    F.timestamp_millis(F.col("f.properties.time")).alias("hora"),
    F.col("f.properties.mag").alias("mag"),
    F.col("f.properties.place").alias("lugar"),
    F.col("f.geometry.coordinates")[2].alias("prof"),
)
tabla.show(5, truncate=False)

print("M >= 7 en 2025:", tabla.filter(F.col("mag") >= 7).count())
tabla.orderBy(F.col("mag").desc()).show(5, truncate=False)
spark.stop()
