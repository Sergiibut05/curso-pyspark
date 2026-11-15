"""Lección 5: columnas, funciones, nulos y JSON dentro de una columna."""
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from comun import sesion, terremotos_2025

spark = sesion("columnas")
t = terremotos_2025(spark)

clasificados = (
    t.withColumn(
        "categoria",
        F.when(F.col("mag") >= 7, "mayor")
        .when(F.col("mag") >= 6, "fuerte")
        .when(F.col("mag") >= 5, "moderado")
        .otherwise("ligero"),
    )
    .withColumn("dia", F.to_date("hora"))
    .withColumn("profundo", F.col("prof") > 300)
)
clasificados.groupBy("categoria").count().orderBy("count").show()

# Nulos: se cuentan, no se rellenan
t.select(
    F.count("*").alias("filas"),
    F.count("mag").alias("con_mag"),
    F.count("sentido").alias("con_sentido"),
    F.sum(F.col("sentido").isNull().cast("int")).alias("sentido_nulo"),
).show()

# JSON guardado como texto (como el valor de un mensaje de Kafka)
mensajes = spark.createDataFrame(
    [
        ('{"event": {"id": "us6000pjr6", "properties": {"mag": 4.2, "updated": 1742247237040}}}',),
        ('{"event": {"id": "20250101_0000001", "properties": {"mag": 2.6, "lastupdate": "2025-01-01T18:25:52Z"}}}',),
        ("esto no es JSON",),
    ],
    "valor string",
)
mensajes.select(
    F.get_json_object("valor", "$.event.id").alias("id"),
    F.coalesce(
        F.get_json_object("valor", "$.event.properties.updated"),
        F.get_json_object("valor", "$.event.properties.lastupdate"),
    ).alias("version"),
).show(truncate=False)

esquema = StructType([StructField("event", StructType([
    StructField("id", StringType()),
    StructField("properties", StructType([StructField("mag", DoubleType())])),
]))])
mensajes.select(F.from_json("valor", esquema).alias("m")).select("m.event.id", "m.event.properties.mag").show()
spark.stop()
