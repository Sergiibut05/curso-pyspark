"""Lección 10: Spark SQL. Vistas, el mismo plan que la API, CTE y ventanas en SQL, parámetros y tablas."""
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from comun import SALIDA, terremotos_2025

# Las tablas guardadas con saveAsTable van al «almacén» (warehouse): aquí, dentro de _salida.
spark = (SparkSession.builder.master("local[2]").appName("spark-sql")
         .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.warehouse.dir", f"{SALIDA}/almacen").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

t = terremotos_2025(spark)
t.createOrReplaceTempView("terremotos")          # un nombre para usar el DataFrame desde SQL

# 1. La misma consulta en SQL y con la API
sql = spark.sql("""
    SELECT red, count(*) AS n, round(max(mag), 1) AS mag_max
    FROM terremotos WHERE mag >= 5
    GROUP BY red ORDER BY n DESC LIMIT 3""")
api = (t.filter("mag >= 5").groupBy("red")
       .agg(F.count("*").alias("n"), F.round(F.max("mag"), 1).alias("mag_max"))
       .orderBy(F.desc("n")).limit(3))
sql.show()


def plan(df):
    return df._jdf.queryExecution().optimizedPlan().toString().replace("#", "").split("\n")


limpio = lambda p: [__import__("re").sub(r"\d+L?", "", l) for l in p]   # quitar los ids internos de columna
print("¿mismo plan optimizado?", limpio(plan(sql)) == limpio(plan(api)))

# 2. CTE y ventana en SQL: el mayor terremoto de cada trimestre
spark.sql("""
    WITH con_trimestre AS (
        SELECT id, mag, lugar, quarter(hora) AS trimestre FROM terremotos
    ), ordenados AS (
        SELECT *, row_number() OVER (PARTITION BY trimestre ORDER BY mag DESC, id) AS puesto
        FROM con_trimestre
    )
    SELECT trimestre, mag, lugar FROM ordenados WHERE puesto = 1 ORDER BY trimestre""").show(truncate=False)

# 3. Parámetros: nunca formatear el SQL con f-strings a partir de datos que no controlas
umbral, red = 6.5, "us"
spark.sql("SELECT count(*) AS n FROM terremotos WHERE mag >= :umbral AND red = :red",
          args={"umbral": umbral, "red": red}).show()
spark.sql("SELECT count(*) AS n FROM {t} WHERE mag >= {umbral}", t=t, umbral=umbral).show()   # DataFrame como tabla

# 4. SQL dentro de la API: expr y selectExpr
t.selectExpr("id", "round(mag * 10) / 10 AS mag", "CASE WHEN prof > 300 THEN 'profundo' ELSE 'normal' END AS tipo") \
 .groupBy("tipo").count().show()

# 5. Tablas: se guardan en el almacén y sobreviven a la sesión (las vistas temporales, no)
# El catálogo de esta sesión está en memoria: no recuerda la tabla de la ejecución anterior, pero su carpeta
# sigue ahí, y saveAsTable fallaría con LOCATION_ALREADY_EXISTS. Se borra la carpeta para poder repetir.
spark.sql("DROP TABLE IF EXISTS fuertes")
shutil.rmtree(f"{SALIDA}/almacen/fuertes", ignore_errors=True)
t.filter("mag >= 6").write.saveAsTable("fuertes")
print("tablas:", [(x.name, x.tableType, x.isTemporary) for x in spark.catalog.listTables()])
print("filas en la tabla fuertes:", spark.table("fuertes").count())
spark.stop()
