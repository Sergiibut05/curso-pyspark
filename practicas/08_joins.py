"""Lección 8: joins a fondo. Tipos, nulos en la clave, claves repetidas, columnas con el mismo nombre y
las estrategias que elige Spark (broadcast o sort-merge)."""
from pyspark.sql import functions as F

from comun import SALIDA, sesion, terremotos_2025

spark = sesion("joins")

# Dos tablas pequeñas para ver cada fila: terremotos (con una red desconocida y una nula) y redes.
sismos = spark.createDataFrame(
    [("s1", "us", 6.1), ("s2", "ak", 4.3), ("s3", "pr", 4.0), ("s4", "xx", 5.0), ("s5", None, 4.8)],
    "id STRING, red STRING, mag DOUBLE")
redes = spark.createDataFrame(
    [("us", "USGS NEIC"), ("ak", "Alaska"), ("nc", "California norte"), (None, "sin red")],
    "red STRING, nombre STRING")

for tipo in ("inner", "left", "right", "full", "left_semi", "left_anti"):
    filas = sismos.join(redes, "red", tipo).orderBy("red", "id").collect()
    print(f"{tipo:<10} {len(filas)} filas:", [tuple(f) for f in filas])

# Nulos: con == un nulo no coincide con nada (ni con otro nulo); eqNullSafe sí
print("nulo con ==          :", sismos.join(redes, sismos.red == redes.red).filter(sismos.red.isNull()).count())
print("nulo con eqNullSafe  :", sismos.join(redes, sismos.red.eqNullSafe(redes.red)).filter(sismos.red.isNull()).count())

# Clave repetida en la tabla de la derecha: las filas se multiplican
repetida = redes.union(spark.createDataFrame([("us", "USGS (otra fila)")], "red STRING, nombre STRING"))
print("inner con 'us' repetida:", sismos.join(repetida, "red").count(), "filas (antes, 2)")

# Columnas con el mismo nombre
por_condicion = sismos.join(redes, sismos.red == redes.red)
print("columnas con condición:", por_condicion.columns, "| con on='red':", sismos.join(redes, "red").columns)
try:
    por_condicion.select("red").show()
except Exception as exc:
    print("select('red'):", str(exc).splitlines()[0][:100])
por_condicion.select(sismos.red, redes.nombre).show(2)   # o con alias: s = sismos.alias("s"); F.col("s.red")

# Estrategias, con los 18.302 terremotos de verdad
t = terremotos_2025(spark).cache()
t.count()


def estrategia(df):
    """El join del plan físico. Se mira DESPUÉS de ejecutar: con AQE, el plan final puede cambiar."""
    df.count()
    plan = df._jdf.queryExecution().executedPlan().toString()
    return next(n for n in ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin") if n in plan)


print("tamaño que estima Spark de 'redes':", redes._jdf.queryExecution().optimizedPlan().stats().sizeInBytes())
redes.write.mode("overwrite").parquet(f"{SALIDA}/redes")
redes_pq = spark.read.parquet(f"{SALIDA}/redes")
print("tamaño que estima Spark de la leída de Parquet:", redes_pq._jdf.queryExecution().optimizedPlan().stats().sizeInBytes())

spark.conf.set("spark.sql.adaptive.enabled", "false")
print("sin AQE, redes de Python   :", estrategia(t.join(redes, "red", "left")))
print("sin AQE, redes de Parquet  :", estrategia(t.join(redes_pq, "red", "left")))
print("sin AQE, con F.broadcast   :", estrategia(t.join(F.broadcast(redes), "red", "left")))
spark.stop()
