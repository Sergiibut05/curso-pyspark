"""Lección 15: optimizar. Filtros y columnas que Parquet se salta, el problema de los ficheros pequeños, el sesgo
(una clave con casi todas las filas), la sal y lo que hace AQE con un join sesgado."""
import os
import time

from pyspark.sql import functions as F

from comun import SALIDA, sesion, terremotos_2025

spark = sesion("optimizar")
t = terremotos_2025(spark)
t.write.mode("overwrite").parquet(f"{SALIDA}/opt/terremotos")
pq = spark.read.parquet(f"{SALIDA}/opt/terremotos")


def detalle(df, clave):
    plan = df._jdf.queryExecution().executedPlan().toString()
    linea = next(l for l in plan.splitlines() if clave in l)
    trozo = linea[linea.index(clave):]
    return trozo[:trozo.find("]", len(clave)) + 1] if "]" in trozo else trozo[:100]


# 1. Parquet: el filtro baja a la lectura y solo se leen las columnas pedidas
consulta = pq.filter("mag >= 7").select("id", "mag")
print("PushedFilters:", detalle(consulta, "PushedFilters:"))
print("ReadSchema   :", detalle(consulta, "ReadSchema:"))
print("M >= 7:", consulta.count())

# 2. Ficheros pequeños: los mismos datos en 200 ficheros o en 1
for n in (1, 200):
    ruta = f"{SALIDA}/opt/en_{n}"
    t.repartition(n).write.mode("overwrite").parquet(ruta)
    ficheros = [f for f in os.listdir(ruta) if f.endswith(".parquet")]
    inicio = time.time()
    leido = spark.read.parquet(ruta)
    filas = leido.filter("mag >= 5").count()
    print(f"{n:>3} ficheros ({sum(os.path.getsize(os.path.join(ruta, f)) for f in ficheros) / 1e6:.2f} MB): "
          f"leer y filtrar {time.time() - inicio:.2f} s, {leido.rdd.getNumPartitions()} particiones al leer")

# 3. Sesgo: 2 millones de filas, el 90 % con la misma clave
datos = spark.range(2_000_000).select(
    F.when(F.col("id") % 10 < 9, F.lit("us")).otherwise(F.concat(F.lit("red"), (F.col("id") % 50).cast("string"))).alias("red"),
    F.col("id"))
por_particion = datos.repartition(8, "red").groupBy(F.spark_partition_id().alias("p")).count().orderBy("p")
print("filas por partición al repartir por red:", [r["count"] for r in por_particion.collect()])

# 4. La sal: añadir un número aleatorio a la clave caliente reparte su trabajo
SAL = 8
con_sal = datos.withColumn("sal", (F.rand(seed=1) * SAL).cast("int"))
por_particion = (con_sal.repartition(8, "red", "sal").groupBy(F.spark_partition_id().alias("p")).count().orderBy("p"))
print("con sal                               :", [r["count"] for r in por_particion.collect()])
# Agregar en dos pasos: primero por (red, sal), luego por red
resultado = (con_sal.groupBy("red", "sal").agg(F.count("*").alias("n")).groupBy("red").agg(F.sum("n").alias("n"))
             .orderBy(F.desc("n")).first())
print("recuento de 'us' con sal:", resultado["n"])

# 5. AQE y un join sesgado: umbrales bajados para que se active con pocos datos
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.shuffle.partitions", "50")                 # con 4, la caliente no destaca sobre la mediana
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256KB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256KB")
# La tabla de redes, leída de Parquet: así el join recibe los dos shuffles directamente (y su tamaño es conocido)
datos.select("red").distinct().withColumn("nombre", F.concat(F.lit("Red "), "red"))      .write.mode("overwrite").parquet(f"{SALIDA}/opt/redes")
redes = spark.read.parquet(f"{SALIDA}/opt/redes")
# sum("id") obliga a llevar el id por el shuffle: sin una columna con peso, la partición de «us» solo
# llevaría el texto «us», que comprimido no llega al umbral y AQE no la consideraría sesgada.
consulta = datos.join(redes, "red").groupBy("nombre").agg(F.sum("id").alias("suma"))
consulta.collect()                                                # ejecutar ESTA consulta, para ver su plan final
plan = consulta._jdf.queryExecution().executedPlan().toString().split("== Initial Plan ==")[0]
for linea in plan.splitlines():
    if "SortMergeJoin" in linea or ("AQEShuffleRead" in linea and "skewed" in linea):
        print("plan final:", linea.strip()[linea.strip().find("SortMergeJoin") if "SortMergeJoin" in linea else linea.strip().find("AQE"):][:45])
spark.stop()
