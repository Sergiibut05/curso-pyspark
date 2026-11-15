"""Lección 14: leer un plan con explain, ver qué cambia la ejecución adaptativa (AQE) y consultar la interfaz
web de Spark por su API.

Uso: python 14_planes_y_ui.py [segundos]
  Con un número, deja la aplicación viva ese tiempo para abrir http://localhost:4040 en el navegador
  (ejecútalo con el puerto publicado: docker compose run --rm -p 4040:4040 spark python 14_planes_y_ui.py 120).
"""
import json
import sys
import time
import urllib.request

from pyspark.sql import functions as F

from comun import sesion, terremotos_2025

spark = sesion("planes-y-ui")
t = terremotos_2025(spark)
redes = spark.createDataFrame([("us", "USGS NEIC"), ("ak", "Alaska"), ("nc", "California norte")],
                              "red STRING, nombre STRING")
consulta = (t.filter("mag >= 5").join(redes, "red", "left")
            .groupBy("nombre").agg(F.count("*").alias("n"), F.max("mag").alias("mag_max")))

# 1. explain en modo formatted: el árbol arriba, los detalles de cada nodo debajo
consulta.explain(mode="formatted")


def nodos(df):
    plan = df._jdf.queryExecution().executedPlan().toString()
    plan = plan.split("== Initial Plan ==")[0]     # tras ejecutar, Spark enseña el plan final y el inicial
    return [n for n in ("isFinalPlan=false", "isFinalPlan=true", "SortMergeJoin", "BroadcastHashJoin",
                        "AQEShuffleRead") if n in plan]


# 2. AQE: el plan antes y después de ejecutar la MISMA consulta
print("antes de ejecutar :", nodos(consulta))
consulta.collect()
print("después           :", nodos(consulta))

# 3. AQE junta las particiones del shuffle que salen casi vacías
spark.conf.set("spark.sql.shuffle.partitions", "200")
for aqe in ("false", "true"):
    spark.conf.set("spark.sql.adaptive.enabled", aqe)
    por_red = t.groupBy("red").count()
    print(f"AQE={aqe:<5} particiones tras el groupBy: {por_red.rdd.getNumPartitions()}")

# 4. La interfaz web: lo mismo que se ve en el navegador, por su API REST
url = spark.sparkContext.uiWebUrl
app = spark.sparkContext.applicationId


def api(ruta):
    with urllib.request.urlopen(f"{url}/api/v1/applications/{app}/{ruta}") as r:
        return json.load(r)


trabajos = api("jobs")
print(f"interfaz web en {url}: {len(trabajos)} trabajos")
for j in sorted(trabajos, key=lambda j: j["jobId"])[-3:]:
    print(f"  trabajo {j['jobId']}: {j['status']}, etapas {j['stageIds']}, tareas {j['numTasks']}")
etapa = max(api("stages"), key=lambda s: s["executorRunTime"])
print(f"etapa más costosa: {etapa['stageId']} ({etapa['name'][:40]}), {etapa['numTasks']} tareas, "
      f"{etapa['executorRunTime']} ms de ejecución")

if len(sys.argv) > 1:
    print(f"Interfaz abierta {sys.argv[1]} s en http://localhost:4040 …")
    time.sleep(int(sys.argv[1]))
spark.stop()
