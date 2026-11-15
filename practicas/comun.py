"""Piezas comunes de las prácticas del curso de PySpark.

Se ejecutan con la imagen del curso (docker/Dockerfile: Python 3.12 + Java 17 + PySpark 4.2.1), que
tiene el conector de Kafka en /opt/spark-jars. La carpeta datos/ del curso se monta en /datos (solo lectura).
"""

import glob

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

USGS_2025 = "/datos/historico/2025/eventos.geojson"
EMSC_ENERO_2025 = "/datos/relleno/emsc/20250101T000000_20250201T000000_m2.5.json"
USGS_ENERO_2025 = "/datos/relleno/usgs/20250101T000000_20250201T000000_m2.5.json"
KAFKA = "kafka:19092"
SALIDA = "/practicas/_salida"   # lo que escriben las prácticas; se puede borrar cuando se quiera


def sesion(nombre: str = "curso", kafka: bool = False) -> SparkSession:
    """Spark en modo local con dos hilos, como la fusión del proyecto (SPARK_MASTER=local[2])."""
    constructor = (
        SparkSession.builder.master("local[2]")
        .appName(nombre)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if kafka:
        constructor = constructor.config("spark.jars", ",".join(sorted(glob.glob("/opt/spark-jars/*.jar"))))
    spark = constructor.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def terremotos_2025(spark: SparkSession) -> DataFrame:
    """Los 18.302 terremotos M ≥ 4 de 2025 (USGS) como tabla plana. Es lo que se construye en la lección 2."""
    crudo = spark.read.json(USGS_2025)
    return crudo.select(F.explode("features").alias("f")).select(
        F.col("f.id").alias("id"),
        F.timestamp_millis(F.col("f.properties.time")).alias("hora"),
        F.col("f.properties.mag").alias("mag"),
        F.col("f.properties.magType").alias("tipo_mag"),
        F.col("f.properties.place").alias("lugar"),
        F.col("f.properties.net").alias("red"),
        F.col("f.properties.felt").alias("sentido"),
        F.col("f.geometry.coordinates")[0].alias("lon"),
        F.col("f.geometry.coordinates")[1].alias("lat"),
        F.col("f.geometry.coordinates")[2].alias("prof"),
    )
