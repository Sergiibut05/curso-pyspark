"""Arranca Spark en modo local y comprueba que todo funciona."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("curso").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Spark", spark.version, "| master", spark.sparkContext.master)
print("Paralelismo por defecto:", spark.sparkContext.defaultParallelism)
df = spark.range(1_000_000)
print("Filas:", df.count(), "| particiones:", df.rdd.getNumPartitions())
spark.stop()
