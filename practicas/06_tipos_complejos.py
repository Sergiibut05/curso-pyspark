"""Lección 6: struct, array y map. Navegar, construir, explotar, funciones de orden superior y JSON con esquema."""
from pyspark.sql import functions as F

from comun import USGS_2025, sesion

spark = sesion("tipos-complejos")
f = spark.read.json(USGS_2025).select(F.explode("features").alias("f")).select("f.*")

# 1. Los tres tipos complejos, tal como vienen en el GeoJSON
print("properties:", f.schema["properties"].dataType.typeName(), "| geometry:", f.schema["geometry"].dataType.typeName(),
      "| coordinates:", f.schema["geometry"].dataType["coordinates"].dataType.simpleString())

# 2. Navegar: punto para struct, [n] o getItem para array, element_at (empieza en 1)
f.select(
    "id",
    F.col("properties.mag").alias("mag"),
    F.col("geometry.coordinates")[2].alias("prof"),
    F.element_at("geometry.coordinates", -1).alias("ultimo"),
    F.size("geometry.coordinates").alias("n"),
).show(3)

# 3. Construir: struct, array y map a partir de columnas
compacto = f.select(
    "id",
    F.struct(F.col("properties.mag").alias("valor"), F.col("properties.magType").alias("tipo")).alias("magnitud"),
    F.create_map(F.lit("red"), F.col("properties.net"), F.lit("estado"), F.col("properties.status")).alias("etiquetas"),
)
compacto.printSchema()
compacto.select("id", "magnitud.valor", F.col("etiquetas")["estado"].alias("estado")).show(3)

# 4. De texto a array y explode: los «productos» que USGS ha publicado para cada terremoto
tipos = f.select("id", F.split(F.trim(F.col("properties.types"), F.lit(",")), ",").alias("productos"))
tipos.show(2, truncate=False)
(tipos.select(F.explode("productos").alias("producto")).groupBy("producto").count()
 .orderBy(F.desc("count")).show(6))

# 5. Funciones de orden superior: trabajar dentro del array sin explotarlo
(tipos.filter(F.array_contains("productos", "shakemap")).select(
    "id",
    F.size("productos").alias("n"),
    F.filter("productos", lambda p: p.startswith("shake")).alias("shake"),
    F.transform("productos", lambda p: F.length(p)).alias("longitudes"),
    F.exists("productos", lambda p: p == "dyfi").alias("tiene_dyfi"),
).show(3, truncate=False))

# 6. JSON dentro de una columna de texto: from_json con esquema (como el valor de un mensaje de Kafka)
texto = f.select(F.to_json(F.struct("id", F.col("properties.mag").alias("mag"),
                                    F.col("geometry.coordinates").alias("coord"))).alias("json")).limit(2)
texto.show(truncate=False)
esquema = "id STRING, mag DOUBLE, coord ARRAY<DOUBLE>"
texto.select(F.from_json("json", esquema).alias("d")).select("d.id", "d.mag", F.col("d.coord")[2].alias("prof")).show()
print("esquema deducido de un ejemplo:", texto.select(F.schema_of_json(F.lit(texto.first().json))).first()[0])
spark.stop()
