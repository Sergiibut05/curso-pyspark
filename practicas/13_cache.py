"""Lección 13: cache y persist. Cuánto cuesta repetir el trabajo, que cache() es perezoso, qué enseña el plan,
y los niveles de almacenamiento."""
import time

from pyspark import StorageLevel
from pyspark.sql import functions as F

from comun import sesion, terremotos_2025

spark = sesion("cache")


def cronometro(nombre, accion):
    inicio = time.time()
    resultado = accion()
    print(f"{nombre:<34} {time.time() - inicio:5.2f} s  -> {resultado}")


# Un DataFrame caro de calcular: leer y aplanar el GeoJSON (12 MB, una sola partición)
t = terremotos_2025(spark).withColumn("energia", F.pow(F.lit(10.0), 1.5 * F.col("mag") + 4.8))

cronometro("sin cache, acción 1 (count)", t.count)
cronometro("sin cache, acción 2 (max mag)", lambda: t.agg(F.max("mag")).first()[0])

t.cache()                                      # perezoso: aún no guarda nada
print("¿en caché tras cache()?", t.is_cached, "| nivel:", t.storageLevel)
cronometro("con cache, acción 1 (la llena)", t.count)
cronometro("con cache, acción 2 (max mag)", lambda: t.agg(F.max("mag")).first()[0])
cronometro("con cache, acción 3 (por red)", lambda: t.groupBy("red").count().count())

plan = t.agg(F.max("mag"))._jdf.queryExecution().executedPlan().toString()
print("el plan lee de la caché:", "InMemoryTableScan" in plan)

# Cuánto ocupa: el estado de almacenamiento que también enseña la pestaña Storage de la interfaz web
for info in spark.sparkContext._jsc.sc().getRDDStorageInfo():
    print(f"guardado: {info.numCachedPartitions()} partición(es), {info.memSize() / 1e6:.1f} MB en memoria, "
          f"{info.diskSize() / 1e6:.1f} MB en disco")

t.unpersist()
print("¿en caché tras unpersist()?", t.is_cached)

# persist con otro nivel: en disco, serializado
t.persist(StorageLevel.DISK_ONLY)
t.count()
info = spark.sparkContext._jsc.sc().getRDDStorageInfo()[0]
print(f"DISK_ONLY: {info.memSize() / 1e6:.1f} MB en memoria, {info.diskSize() / 1e6:.1f} MB en disco")
t.unpersist()

spark.stop()
