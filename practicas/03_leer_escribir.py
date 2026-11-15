"""Lección 3: el mismo catálogo en CSV, JSON Lines y Parquet. Esquemas, modos de escritura, particionado en
disco y registros corruptos. Escribe en practicas/_salida/ (se puede borrar cuando quieras)."""
import os

from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from comun import SALIDA, sesion, terremotos_2025

spark = sesion("leer-escribir")
t = terremotos_2025(spark).withColumn("mes", F.month("hora"))


def tamano(ruta):
    """Bytes de los ficheros de datos de una carpeta escrita por Spark (sin los .crc ni _SUCCESS)."""
    total = ficheros = 0
    for raiz, _, nombres in os.walk(ruta):
        for n in nombres:
            if not n.startswith((".", "_")):
                total += os.path.getsize(os.path.join(raiz, n))
                ficheros += 1
    return total, ficheros


# 1. Escribir lo mismo en tres formatos
for formato in ("csv", "json", "parquet"):
    destino = f"{SALIDA}/terremotos.{formato}"
    escritor = t.write.mode("overwrite")
    if formato == "csv":
        escritor = escritor.option("header", True)
    escritor.format(formato).save(destino)
    total, n = tamano(destino)
    print(f"{formato:<8} {total / 1e6:6.2f} MB en {n} fichero(s)")

# 2. Leer el CSV: sin esquema, infiriéndolo y dándolo
sin_esquema = spark.read.option("header", True).csv(f"{SALIDA}/terremotos.csv")
inferido = spark.read.option("header", True).option("inferSchema", True).csv(f"{SALIDA}/terremotos.csv")
ESQUEMA = ("id STRING, hora TIMESTAMP, mag DOUBLE, tipo_mag STRING, lugar STRING, red STRING, "
           "sentido INT, lon DOUBLE, lat DOUBLE, prof DOUBLE, mes INT")
explicito = spark.read.option("header", True).schema(ESQUEMA).csv(f"{SALIDA}/terremotos.csv")
for nombre, df in (("sin esquema", sin_esquema), ("inferSchema", inferido), ("esquema DDL", explicito)):
    print(f"{nombre:<12}", ", ".join(f"{c}:{tipo}" for c, tipo in df.dtypes[:4]), "…")

# 3. Parquet guarda el esquema: se lee tal cual, sin inferir nada
print("parquet     ", ", ".join(f"{c}:{tipo}" for c, tipo in spark.read.parquet(f"{SALIDA}/terremotos.parquet").dtypes[:4]), "…")

# 4. Modos de escritura
try:
    t.write.parquet(f"{SALIDA}/terremotos.parquet")          # por defecto: errorifexists
except AnalysisException as exc:
    print("modo por defecto:", str(exc).splitlines()[0][:110])
t.limit(10).write.mode("append").parquet(f"{SALIDA}/terremotos.parquet")
print("tras append de 10 filas:", spark.read.parquet(f"{SALIDA}/terremotos.parquet").count())

# 5. Particionar en disco por una columna
t.write.mode("overwrite").partitionBy("mes").parquet(f"{SALIDA}/por_mes")
print("carpetas:", sorted(os.listdir(f"{SALIDA}/por_mes"))[:4], "…")
marzo = spark.read.parquet(f"{SALIDA}/por_mes").filter("mes = 3")
print("marzo:", marzo.count(), "terremotos")
plan = marzo._jdf.queryExecution().executedPlan().toString()
linea = [l for l in plan.splitlines() if "PartitionFilters" in l][0]
print("en el plan:", linea[linea.index("PartitionFilters"):][:70])

# 6. Registros corruptos en JSON Lines
os.makedirs(f"{SALIDA}/sucio", exist_ok=True)
with open(f"{SALIDA}/sucio/datos.jsonl", "w", encoding="utf-8") as f:
    f.write('{"id": "a", "mag": 4.1}\n{"id": "b", "mag": 4.5\n{"id": "c", "mag": "fuerte"}\n')
ESQ = "id STRING, mag DOUBLE, _corrupt_record STRING"
for modo in ("PERMISSIVE", "DROPMALFORMED", "FAILFAST"):
    try:
        filas = spark.read.schema(ESQ).option("mode", modo).json(f"{SALIDA}/sucio").collect()
        print(f"{modo:<13}", [(r.id, r.mag, r._corrupt_record) for r in filas])
    except Exception as exc:
        error = next((l for l in str(exc).splitlines() if "MALFORMED_RECORD" in l), str(exc).splitlines()[0])
        print(f"{modo:<13} falla:", error.strip()[error.strip().find("[MALFORMED"):][:95])
spark.stop()
