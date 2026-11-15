"""Lección 11: funciones propias (UDF). Lo que cuestan frente a las funciones de Spark, pandas UDF con Arrow,
los fallos típicos y applyInPandas para trabajar por grupos con pandas."""
import time

import pandas as pd
from pyspark.sql import functions as F

from comun import sesion, terremotos_2025

spark = sesion("udfs")
t = terremotos_2025(spark).select("id", "red", "mag").filter(F.col("mag").isNotNull()).cache()
grande = t.crossJoin(spark.range(100).withColumnRenamed("id", "copia")).cache()   # ~1,8 millones de filas
print("filas:", grande.count())


# Energía liberada por un terremoto (fórmula de Gutenberg-Richter): log10(E en julios) = 1,5·M + 4,8
@F.udf("double")
def energia_udf(m):
    return 10 ** (1.5 * m + 4.8)


@F.udf("double", useArrow=True)
def energia_udf_arrow(m):
    return 10 ** (1.5 * m + 4.8)


@F.pandas_udf("double")
def energia_pandas(m: pd.Series) -> pd.Series:
    return 10 ** (1.5 * m + 4.8)


formas = {
    "función de Spark": F.pow(F.lit(10.0), 1.5 * F.col("mag") + 4.8),
    "UDF de Python": energia_udf("mag"),
    "UDF de Python (Arrow)": energia_udf_arrow("mag"),
    "pandas UDF": energia_pandas("mag"),
}
for nombre, expresion in formas.items():
    inicio = time.time()
    total = grande.select(expresion.alias("e")).agg(F.sum("e")).first()[0]
    print(f"{nombre:<22} {time.time() - inicio:5.2f} s   suma={total:.4e}")

# Fallo 1: el tipo declarado no coincide con lo que devuelve la función: Spark 4 lo convierte sin avisar
@F.udf("int")
def redondeo_mal(m):
    return round(m, 1)          # devuelve un float, pero se declaró int


t.select("mag", redondeo_mal("mag").alias("udf_int")).show(3)

# Fallo 2: los nulos llegan a la UDF como None
@F.udf("double")
def doble(m):
    return m * 2


con_nulo = spark.createDataFrame([(4.5,), (None,)], "mag DOUBLE")
try:
    con_nulo.select(doble("mag")).collect()
except Exception as exc:
    linea = next(l for l in str(exc).splitlines() if "TypeError" in l)
    print("UDF con un nulo:", linea.strip()[:100])

# applyInPandas: cada grupo llega como un DataFrame de pandas y se devuelve otro
def resumen_red(pdf: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({"red": [pdf["red"].iloc[0]], "n": [len(pdf)],
                         "mediana": [pdf["mag"].median()], "p90": [pdf["mag"].quantile(0.9)]})


(t.groupBy("red").applyInPandas(resumen_red, "red STRING, n LONG, mediana DOUBLE, p90 DOUBLE")
 .orderBy(F.desc("n")).show(4))
spark.stop()
