---
titulo: UDF - funciones propias en Python, y lo que cuestan
resumen: Cuándo hace falta una función propia, cuánto más lenta es que una de Spark, por qué las pandas UDF con Arrow lo arreglan casi del todo, los fallos silenciosos y applyInPandas.
bloque: Transformar datos
minutos: 25
practica: si
---

A veces lo que necesitas no existe en `pyspark.sql.functions`: una fórmula propia, una librería de Python,
una regla de negocio retorcida. Para eso están las **UDF** (*user-defined functions*): funciones de Python
que Spark aplica a cada fila.

Tienen un precio que conviene entender antes de usarlas.

## Por qué una UDF de Python es lenta

Spark corre en la **JVM** (Java). Una función de `F` se ejecuta allí, dentro del plan optimizado. Una UDF
de Python no: Spark tiene que

1. arrancar procesos de Python en cada ejecutor,
2. **serializar** cada fila, mandarla al proceso de Python, ejecutar la función y mandar el resultado de
   vuelta,
3. y tratar la función como una **caja negra**: el optimizador no sabe qué hace, así que no puede
   reordenarla, simplificarla ni empujar filtros a través de ella.

Las **pandas UDF** (vectorizadas) reducen el coste del paso 2: en vez de fila a fila, mandan **bloques de
miles de filas** en formato **Arrow** (un formato de columnas en memoria que Java y Python leen sin
convertir) y la función trabaja con un `pandas.Series` entero de una vez.

```python preparacion
import pandas as pd
from pyspark.sql import functions as F

from comun import sesion, terremotos_2025

spark = sesion("udfs")
t = terremotos_2025(spark).select("id", "red", "mag").filter(F.col("mag").isNotNull()).cache()
```

## Míralo: la misma fórmula, cuatro formas

La energía que libera un terremoto se estima a partir de su magnitud: `log10(E) = 1,5·M + 4,8` (julios). El
script la calcula sobre 1,8 millones de filas (los 18.302 terremotos repetidos 100 veces) de cuatro formas:

```python titulo="practicas/11_udfs.py (extracto)"
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
```

```powershell pruebalo
spark-py 11_udfs.py 2>$null
```

```salida
filas: 1830200
función de Spark        0.35 s   suma=1.2101e+20
UDF de Python           2.03 s   suma=1.2101e+20
UDF de Python (Arrow)   0.79 s   suma=1.2101e+20
pandas UDF              0.34 s   suma=1.2101e+20
```

- El mismo resultado las cuatro veces.
- La **UDF de Python clásica** es unas **seis veces más lenta** que la función de Spark.
- Con **`useArrow=True`** (Spark 3.5+), la misma UDF fila a fila viaja en bloques Arrow: menos de la mitad
  de tiempo que la clásica.
- La **pandas UDF**, vectorizada, iguala a la función de Spark en este caso: el cálculo es una operación de
  numpy sobre el bloque entero.

Tus tiempos serán otros (dependen de la máquina y del calentamiento de la JVM), pero el orden se repite.
Con funciones más complejas, la distancia entre la función de Spark y cualquier UDF crece, porque el
optimizador sí puede hacer cosas con la primera.

:::defensa El orden de preferencia
1. Una función de `pyspark.sql.functions` (o una combinación de ellas). Casi siempre existe.
2. Una **pandas UDF**, si la lógica se expresa con operaciones de pandas/numpy sobre columnas.
3. Una UDF con `useArrow=True`, si la lógica es fila a fila y no se puede vectorizar.
4. La UDF clásica, solo si nada de lo anterior sirve.
:::

## Dos fallos que no avisan

**El tipo declarado manda.** Una UDF declarada como `int` que devuelve `4.2`:

```python
@F.udf("int")
def redondeo_mal(m):
    return round(m, 1)          # devuelve un float, pero se declaró int

t.select("mag", redondeo_mal("mag").alias("udf_int")).show(3)
```

```salida
+---+-------+
|mag|udf_int|
+---+-------+
|4.2|      4|
|4.0|      4|
|4.4|      4|
+---+-------+
```

Spark **convierte el resultado al tipo declarado sin decir nada**: 4,4 se queda en 4. (Versiones antiguas
de Spark devolvían `null` en este caso: igual de silencioso.) Declara siempre el tipo exacto y prueba la UDF
con valores reales (lección 21).

**Los nulos llegan como `None`.** Una UDF que no los espera revienta el trabajo entero:

```salida
UDF con un nulo: TypeError: unsupported operand type(s) for *: 'NoneType' and 'int'
```

Las funciones de Spark propagan los nulos solas (`null * 2` es `null`); en una UDF tienes que hacerlo tú:
`return None if m is None else m * 2`. En una pandas UDF los nulos llegan como `NaN` o `None` dentro de la
serie y pandas suele propagarlos solo.

## Por grupos con pandas: applyInPandas

A veces quieres hacer algo **por grupo** que es fácil en pandas y engorroso en Spark. `applyInPandas` manda
cada grupo a una función como un `DataFrame` de pandas y recoge lo que devuelva:

```python
def resumen_red(pdf: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({"red": [pdf["red"].iloc[0]], "n": [len(pdf)],
                         "mediana": [pdf["mag"].median()], "p90": [pdf["mag"].quantile(0.9)]})

(t.groupBy("red").applyInPandas(resumen_red, "red STRING, n LONG, mediana DOUBLE, p90 DOUBLE")
 .orderBy(F.desc("n")).show(4))
```

```salida
+---+-----+-------+-----------------+
|red|    n|mediana|              p90|
+---+-----+-------+-----------------+
| us|18139|    4.4|              5.0|
| ak|   82|    4.4|4.890000000000001|
| pr|   37|   4.15|4.981999999999998|
| nc|   20|   4.21|            4.655|
+---+-----+-------+-----------------+
```

:::cuidado Cada grupo tiene que caber en memoria
`applyInPandas` carga **un grupo entero** en la memoria de un proceso de Python. Con la red `us` (18.139
filas) no pasa nada; con un grupo de 500 millones de filas, el ejecutor se queda sin memoria. Úsalo con
grupos acotados. (Aquí, además, mediana y percentiles existen como funciones de Spark: `F.median`,
`F.percentile_approx`. Es solo un ejemplo.)
:::

Otras formas de usar pandas desde Spark: `mapInPandas` (bloques de filas, sin agrupar), y `toPandas()` para
traer un resultado **pequeño** al driver (lección 12: cuidado con `collect` y compañía).

:::ejercicio Una UDF que respeta los nulos
1. Escribe una pandas UDF `categoria(mag)` que devuelva `"leve"` (< 5), `"moderado"` (< 6), `"fuerte"`
   (< 7) o `"mayor"`, y aplícala a los terremotos de 2025.
2. Escribe lo mismo con `F.when` y comprueba que los recuentos por categoría coinciden.
3. Pásale un DataFrame con una magnitud nula. ¿Qué devuelve cada versión?

:::solucion
```python
import numpy as np

@F.pandas_udf("string")
def categoria(mag: pd.Series) -> pd.Series:
    etiquetas = np.select([mag < 5, mag < 6, mag < 7, mag >= 7], ["leve", "moderado", "fuerte", "mayor"], default=None)
    return pd.Series(etiquetas, dtype="object")

con_when = (F.when(F.col("mag") < 5, "leve").when(F.col("mag") < 6, "moderado")
             .when(F.col("mag") < 7, "fuerte").when(F.col("mag") >= 7, "mayor"))
t.groupBy(categoria("mag").alias("c")).count().orderBy("c").show()
t.groupBy(con_when.alias("c")).count().orderBy("c").show()
spark.createDataFrame([(4.5,), (None,)], "mag DOUBLE").select("mag", categoria("mag"), con_when).show()
```

SOLUCION_11
:::

```quiz
¿Por qué una UDF de Python clásica es más lenta que una función de Spark?
- Porque Python es un lenguaje interpretado y Spark no.
* Porque cada fila se serializa y viaja entre la JVM y un proceso de Python, y el optimizador no puede ver dentro.
- Porque las UDF se ejecutan en el driver.
> Las pandas UDF mandan bloques Arrow y trabajan con series enteras, lo que reduce mucho ese coste.
```

```quiz
Declaras una UDF como `int` y devuelve `4.7`. ¿Qué obtienes en Spark 4?
- Un error de tipos.
- `null`.
* `4`, sin aviso.
> Spark convierte al tipo declarado. Declara el tipo correcto y prueba la UDF.
```

```quiz
¿Cuándo es mala idea `applyInPandas`?
- Cuando el resultado tiene distinto número de filas que la entrada.
* Cuando un grupo es tan grande que no cabe en la memoria de un proceso de Python.
- Cuando se agrupa por más de una columna.
> Cada grupo se materializa entero como DataFrame de pandas.
```
