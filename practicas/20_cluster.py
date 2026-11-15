"""Lección 20: el mismo tipo de trabajo, ahora en un clúster Standalone (un master y dos workers).

Se lanza con spark-submit, que decide el master y los recursos:
    docker compose --profile cluster up -d
    docker compose run --rm spark spark-submit --master spark://spark-master:7077 \\
        --total-executor-cores 2 --executor-memory 512m 20_cluster.py
"""
import socket
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from comun import terremotos_2025

# Los ejecutores (en los workers) tienen que poder conectar con el driver (este contenedor): se anuncia su IP.
spark = (SparkSession.builder.appName("curso-cluster")
         .config("spark.driver.host", socket.gethostbyname(socket.gethostname()))
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
print("master:", sc.master, "| aplicación:", sc.applicationId)

# Esperar a que se registren los ejecutores (arrancan en los workers al empezar la aplicación)
for _ in range(30):
    ejecutores = [e for e in sc._jsc.sc().getExecutorMemoryStatus().keySet().toList().mkString(",").split(",")]
    if len(ejecutores) > 2:              # el driver también aparece en la lista
        break
    time.sleep(1)
print("bloques de memoria registrados (driver + ejecutores):", sorted(ejecutores))
print("paralelismo por defecto:", sc.defaultParallelism)

t = terremotos_2025(spark).repartition(8)
# ¿Dónde se ha ejecutado cada partición? Cada tarea devuelve el nombre de la máquina en la que corre.
donde = (t.rdd.mapPartitionsWithIndex(lambda i, filas: [(socket.gethostname(), i, sum(1 for _ in filas))])
         .collect())
for maquina in sorted({m for m, _, _ in donde}):
    partes = [(i, n) for m, i, n in donde if m == maquina]
    print(f"  {maquina}: particiones {[i for i, _ in partes]} ({sum(n for _, n in partes)} filas)")
print("M >= 7:", t.filter("mag >= 7").count(), "| por red:",
      [(r["red"], r["count"]) for r in t.groupBy("red").count().orderBy(F.desc("count")).limit(3).collect()])
spark.stop()
