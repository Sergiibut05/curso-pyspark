---
titulo: Spark SQL
resumen: Consultar DataFrames con SQL. Vistas temporales, por qué SQL y la API dan el mismo plan, CTE y ventanas, parámetros seguros y tablas guardadas en el catálogo.
bloque: Transformar datos
minutos: 20
practica: si
---

Todo lo que has hecho con `filter`, `groupBy` y ventanas se puede escribir en **SQL**. No es otro motor:
es otra forma de escribir lo mismo para el mismo optimizador.

```python preparacion
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from comun import SALIDA, terremotos_2025

# Como en el script: las tablas de saveAsTable van al «almacén», dentro de _salida
spark = (SparkSession.builder.master("local[2]").appName("spark-sql")
         .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.warehouse.dir", f"{SALIDA}/almacen").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
# La carpeta de la tabla «fuertes» de otra ejecución haría fallar saveAsTable: se borra
shutil.rmtree(f"{SALIDA}/almacen/fuertes", ignore_errors=True)
```

## Vistas temporales

Para usar un DataFrame desde SQL hay que darle un nombre:

```python titulo="practicas/10_spark_sql.py (extracto)"
t = terremotos_2025(spark)
t.createOrReplaceTempView("terremotos")

sql = spark.sql("""
    SELECT red, count(*) AS n, round(max(mag), 1) AS mag_max
    FROM terremotos WHERE mag >= 5
    GROUP BY red ORDER BY n DESC LIMIT 3""")
api = (t.filter("mag >= 5").groupBy("red")
       .agg(F.count("*").alias("n"), F.round(F.max("mag"), 1).alias("mag_max"))
       .orderBy(F.desc("n")).limit(3))
sql.show()
```

```powershell pruebalo
spark-py 10_spark_sql.py 2>$null
```

```salida
+---+----+-------+
|red|   n|mag_max|
+---+----+-------+
| us|2114|    8.8|
| ak|   8|    6.0|
| pr|   4|    5.9|
+---+----+-------+

¿mismo plan optimizado? True
```

`spark.sql` devuelve un **DataFrame**, como cualquier transformación: se puede seguir encadenando con la
API, guardar o volver a registrar como vista. Y el plan optimizado de la versión SQL y la de la API es
**idéntico** (el script lo compara quitando los identificadores internos de columna): Catalyst, el
optimizador, recibe lo mismo.

Una vista temporal vive mientras dure la sesión de Spark y solo la ve esa sesión. No copia datos: es un
nombre para un plan.

## SQL para lo que se lee mejor en SQL

Una consulta con varios pasos se lee bien con **CTE** (`WITH`), y las ventanas tienen su sintaxis de
siempre:

```sql
WITH con_trimestre AS (
    SELECT id, mag, lugar, quarter(hora) AS trimestre FROM terremotos
), ordenados AS (
    SELECT *, row_number() OVER (PARTITION BY trimestre ORDER BY mag DESC, id) AS puesto
    FROM con_trimestre
)
SELECT trimestre, mag, lugar FROM ordenados WHERE puesto = 1 ORDER BY trimestre
```

```salida
+---------+---+-------------------------------------------+
|trimestre|mag|lugar                                      |
+---------+---+-------------------------------------------+
|1        |7.7|2025 Mandalay, Burma (Myanmar) Earthquake  |
|2        |7.4|2025 Drake Passage Earthquake              |
|3        |8.8|2025 Kamchatka Peninsula, Russia Earthquake|
|4        |7.6|Drake Passage                              |
+---------+---+-------------------------------------------+
```

Es la ventana `row_number` de la lección 9, en SQL. El M 8,8 de Kamchatka (julio) fue el mayor de 2025.

## SQL o API: cuál usar

| Usa SQL cuando… | Usa la API cuando… |
| --- | --- |
| La consulta es una transformación «de tabla» clásica (filtros, agregados, joins). | Construyes la consulta con lógica de Python: columnas en un bucle, condiciones opcionales. |
| La lee gente que sabe SQL y no Python. | Quieres funciones reutilizables y probadas (lección 21). |
| La traes de otra herramienta (un almacén de datos, un informe). | Quieres que los errores de nombres salten pronto en el editor. |

Se mezclan sin problema. Dentro de la API, `expr` y `selectExpr` aceptan trozos de SQL:

```python
t.selectExpr("id", "round(mag * 10) / 10 AS mag",
             "CASE WHEN prof > 300 THEN 'profundo' ELSE 'normal' END AS tipo").groupBy("tipo").count().show()
```

```salida
+--------+-----+
|    tipo|count|
+--------+-----+
|profundo| 1102|
|  normal|17200|
+--------+-----+
```

## Parámetros, nunca f-strings

Montar SQL con `f"... WHERE red = '{red}'"` a partir de datos que llegan de fuera es invitar a una
**inyección de SQL** (un valor con comillas que cambia la consulta) y a errores de comillas y tipos. Spark
tiene parámetros con nombre:

```python
spark.sql("SELECT count(*) AS n FROM terremotos WHERE mag >= :umbral AND red = :red",
          args={"umbral": 6.5, "red": "us"}).show()
spark.sql("SELECT count(*) AS n FROM {t} WHERE mag >= {umbral}", t=t, umbral=6.5).show()
```

```salida
+---+
|  n|
+---+
| 45|
+---+
```

- `:nombre` con `args=` es el parámetro propiamente dicho: Spark lo trata como un **valor**, nunca como
  trozo de SQL.
- La segunda forma (`{t}` con argumentos con nombre) además permite pasar un **DataFrame** como si fuera
  una tabla, sin registrar ninguna vista.

## Tablas: lo que sobrevive a la sesión

Una vista temporal desaparece al terminar. Una **tabla** se guarda en el almacén (*warehouse*, la carpeta
`spark.sql.warehouse.dir`) y queda en el **catálogo**:

```python
t.filter("mag >= 6").write.saveAsTable("fuertes")
print("tablas:", [(x.name, x.tableType, x.isTemporary) for x in spark.catalog.listTables()])
```

```salida
tablas: [('fuertes', 'MANAGED', False), ('terremotos', 'TEMPORARY', True)]
filas en la tabla fuertes: 145
```

- `MANAGED`: Spark guarda los datos (Parquet, en `_salida/almacen/fuertes/`) y el catálogo.
  `DROP TABLE` borra las dos cosas.
- Las tablas **externas** (`.option("path", …)` al guardar) apuntan a datos que no son de Spark:
  `DROP TABLE` borra solo la entrada del catálogo.
- Aquí el catálogo vive **en memoria** (`spark.sql.catalogImplementation=in-memory`): al terminar la
  sesión se olvida de la tabla, aunque su carpeta siga en disco. La segunda vez que ejecutes el script,
  `saveAsTable` fallaría así (por eso el script borra antes la carpeta):

  ```salida
  [LOCATION_ALREADY_EXISTS] Cannot name the managed table as `spark_catalog`.`default`.`fuertes`, as its associated location 'file:/practicas/_salida/almacen/fuertes' already exists.
  ```

  En un clúster se usa un catálogo compartido y persistente (Hive
  Metastore, Unity Catalog, AWS Glue) y formatos de tabla como **Delta Lake** o **Apache Iceberg**, que
  añaden transacciones, versiones y actualizaciones de filas sobre Parquet.

:::ejercicio Traduce a SQL
Escribe en SQL, sobre la vista `terremotos`:

1. Cuántos terremotos hubo por mes y su magnitud media (redondeada a 2 decimales), ordenado por mes.
2. Los 3 lugares con más terremotos de magnitud 5 o más, usando un parámetro para el 5.
3. La misma consulta 2 con la API de DataFrames. ¿Dan lo mismo?

:::solucion
```python
spark.sql("""SELECT month(hora) AS mes, count(*) AS n, round(avg(mag), 2) AS media
             FROM terremotos GROUP BY mes ORDER BY mes""").show(3)
q = spark.sql("""SELECT lugar, count(*) AS n FROM terremotos WHERE mag >= :m
                 GROUP BY lugar ORDER BY n DESC, lugar LIMIT 3""", args={"m": 5})
q.show(truncate=False)
api = (t.filter(F.col("mag") >= 5).groupBy("lugar").count().withColumnRenamed("count", "n")
       .orderBy(F.desc("n"), "lugar").limit(3))
print(q.collect() == api.collect())
```

SOLUCION_10
:::

```quiz
¿Qué es más rápido, una consulta en SQL o la misma con la API de DataFrames?
- SQL, porque lo optimiza mejor.
- La API, porque evita traducir el texto.
* Igual: las dos llegan al mismo optimizador y producen el mismo plan.
> SQL y la API son dos formas de escribir lo mismo para Catalyst.
```

```quiz
Un formulario web pasa el nombre de una red a una consulta. ¿Cómo la construyes?
- `spark.sql(f"... WHERE red = '{red}'")`
* `spark.sql("... WHERE red = :red", args={"red": red})`
- Con `selectExpr` y el valor concatenado.
> Con parámetros, el valor nunca se interpreta como SQL.
```

```quiz
¿Qué pasa con una vista temporal al terminar la sesión?
* Desaparece: solo era un nombre para un plan dentro de esa sesión.
- Se guarda en el almacén.
- Se convierte en tabla externa.
> Para que algo sobreviva hay que escribirlo: saveAsTable o write a una ruta.
```
