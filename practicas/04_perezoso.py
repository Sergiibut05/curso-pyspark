"""Lección 4: transformaciones (perezosas) y acciones."""
import time

from pyspark.sql import functions as F

from comun import sesion, terremotos_2025

spark = sesion("perezoso")
t = terremotos_2025(spark)

inicio = time.perf_counter()
fuertes = t.filter(F.col("mag") >= 6).groupBy("red").count().orderBy(F.col("count").desc())
print(f"Definir la consulta: {time.perf_counter() - inicio:.3f} s  (no se ha leído nada)")

inicio = time.perf_counter()
fuertes.show()
print(f"show(): {time.perf_counter() - inicio:.3f} s  (ahora sí: lectura, filtro, agrupación)")

fuertes.explain()
spark.stop()
