---
titulo: Leer y escribir - formatos, esquemas y particiones en disco
resumen: CSV, JSON y Parquet con los mismos datos. Inferir el esquema o darlo, modos de escritura, particionar por columnas y qué hacer con los registros corruptos.
bloque: Fundamentos
minutos: 25
practica: si
---

En la lección anterior leíste un GeoJSON. En el mundo real los datos llegan en muchos formatos, y elegir
bien el formato en el que **guardas** los tuyos cambia el tamaño, la velocidad y los errores que tendrás.

## Lectores y escritores

Todo se lee con `spark.read` y se escribe con `df.write`, con la misma forma:

```python
df = spark.read.format("csv").option("header", True).load("ruta")       # forma general
df = spark.read.option("header", True).csv("ruta")                       # atajo equivalente

df.write.format("parquet").mode("overwrite").save("ruta")                 # forma general
df.write.mode("overwrite").parquet("ruta")                                # atajo equivalente
```

Spark **no escribe un fichero, escribe una carpeta**: dentro hay un fichero por partición del DataFrame
(`part-00000-….parquet`), un fichero vacío `_SUCCESS` que indica que la escritura terminó bien y ficheros
`.crc` de control. Para leer, se da la carpeta.

## Los formatos

| Formato | Esquema | Tamaño | Cuándo |
| --- | --- | --- | --- |
| **CSV** | No tiene: todo es texto, o hay que inferirlo o darlo. | Medio | Intercambio con hojas de cálculo y sistemas antiguos. |
| **JSON** (una línea por registro, *JSON Lines*) | Se deduce de los datos; admite estructuras anidadas. | Grande: repite los nombres de campo en cada fila. | APIs, logs, mensajes. |
| **Parquet** | Guardado en el propio fichero, con tipos. | Pequeño: **por columnas** y comprimido. | Lo que Spark escribe para Spark. El estándar en analítica. |
| ORC, Avro | Con esquema | Pequeño | ORC en el mundo Hive; Avro en Kafka (curso de Kafka, lección 20). |

**Por columnas** significa que Parquet guarda juntos todos los valores de `mag`, luego todos los de
`lugar`… Eso comprime mucho mejor (valores parecidos seguidos) y permite leer solo las columnas que hacen
falta (lo verás en la lección 15).

## Míralo: el mismo catálogo, tres formatos

```python titulo="practicas/03_leer_escribir.py (extracto)"
t = terremotos_2025(spark).withColumn("mes", F.month("hora"))
for formato in ("csv", "json", "parquet"):
    escritor = t.write.mode("overwrite")
    if formato == "csv":
        escritor = escritor.option("header", True)
    escritor.format(formato).save(f"{SALIDA}/terremotos.{formato}")
```

```powershell pruebalo
spark-py 03_leer_escribir.py 2>$null
```

```salida
csv        1.95 MB en 1 fichero(s)
json       3.39 MB en 1 fichero(s)
parquet    0.80 MB en 1 fichero(s)
```

Los mismos 18.302 terremotos: Parquet ocupa **menos de la mitad que el CSV y una cuarta parte que el
JSON**. (Todo cabe en un fichero porque el DataFrame tiene una sola partición: viene de un único JSON que no
se puede repartir, lección 12.)

`SALIDA` es `/practicas/_salida`, que en tu ordenador es `curso-pyspark/practicas/_salida/`: puedes abrir
el CSV o el JSON con un editor.

## El esquema al leer

Al leer el CSV de vuelta, cada columna puede salir con un tipo distinto según cómo leas:

```python
sin_esquema = spark.read.option("header", True).csv(ruta)
inferido = spark.read.option("header", True).option("inferSchema", True).csv(ruta)
explicito = spark.read.option("header", True).schema(
    "id STRING, hora TIMESTAMP, mag DOUBLE, tipo_mag STRING, lugar STRING, red STRING, "
    "sentido INT, lon DOUBLE, lat DOUBLE, prof DOUBLE, mes INT").csv(ruta)
```

```salida
sin esquema  id:string, hora:string, mag:string, tipo_mag:string …
inferSchema  id:string, hora:timestamp, mag:double, tipo_mag:string …
esquema DDL  id:string, hora:timestamp, mag:double, tipo_mag:string …
parquet      id:string, hora:timestamp, mag:double, tipo_mag:string …
```

| Forma | Qué pasa |
| --- | --- |
| Sin esquema | Todo es `string`. `mag > 5` compararía textos. |
| `inferSchema` | Spark **lee el fichero entero una vez más** para adivinar los tipos. Con datos grandes es caro, y puede equivocarse (un código postal `08001` pasaría a número `8001`). |
| Esquema explícito | Ninguna lectura extra y ninguna sorpresa. Se da como texto DDL (`"id STRING, mag DOUBLE"`) o con `StructType`. |
| Parquet | El esquema va **dentro del fichero**: se lee tal cual, sin inferir nada. |

:::defensa La regla
En producción, **esquema explícito** para todo lo que no sea Parquet, ORC o Avro. Inferir está bien para
explorar; en un proceso que corre cada día, un tipo adivinado es un error esperando su momento.
:::

## Modos de escritura

| `mode` | Si la carpeta ya existe… |
| --- | --- |
| `errorifexists` (por defecto) | Falla. |
| `overwrite` | La borra y escribe de nuevo. |
| `append` | Añade ficheros nuevos a los que hay. |
| `ignore` | No hace nada. |

```salida
modo por defecto: [PATH_ALREADY_EXISTS] Path file:/practicas/_salida/terremotos.parquet already exists. Set mode as "overwrite"
tras append de 10 filas: 18312
```

:::cuidado append no comprueba nada
`append` no mira si las filas ya estaban: ejecutar dos veces un proceso que añade duplica los datos. Si un
proceso puede repetirse, escribe con `overwrite` sobre una carpeta concreta (por ejemplo, la del día), o
deduplica después.
:::

## Particionar en disco

`partitionBy` reparte la escritura en **subcarpetas** según el valor de una o varias columnas:

```python
t.write.mode("overwrite").partitionBy("mes").parquet(f"{SALIDA}/por_mes")
```

```salida
carpetas: ['._SUCCESS.crc', '_SUCCESS', 'mes=1', 'mes=10'] …
marzo: 1178 terremotos
en el plan: PartitionFilters: [isnotnull(mes#151), (mes#151 = 3)], PushedFilters:
```

Hay una carpeta `mes=1`, `mes=2`… y la columna `mes` **no se guarda dentro de los ficheros**: se deduce del
nombre de la carpeta. Al filtrar por `mes = 3`, el plan muestra un **filtro de partición**: Spark solo
abre la carpeta `mes=3` y ni siquiera mira las demás. Con años de datos particionados por día, una
consulta de un día lee una carpeta de miles.

No confundas esto con las **particiones de un DataFrame** (los trozos en memoria que se procesan en
paralelo, lección 12). Comparten nombre, pero esto es cómo se organiza en disco.

:::cuidado No particiones por una columna con muchos valores
Particionar por `id` crearía 18.302 carpetas con un fichero diminuto cada una. Se particiona por columnas
con **pocos valores** que se usan **mucho en filtros**: fecha, país, fuente. Lo verás en la lección 15
(el problema de los ficheros pequeños).
:::

## Registros corruptos

Un fichero con una línea rota (JSON cortado) y un valor de tipo equivocado (`"mag": "fuerte"`). Lo que
hace Spark depende de `mode` al **leer**:

```python
ESQ = "id STRING, mag DOUBLE, _corrupt_record STRING"
spark.read.schema(ESQ).option("mode", modo).json(f"{SALIDA}/sucio")
```

```salida
PERMISSIVE    [('a', 4.1, None), (None, None, '{"id": "b", "mag": 4.5'), ('c', None, '{"id": "c", "mag": "fuerte"}')]
DROPMALFORMED [('a', 4.1, None)]
FAILFAST      falla: [MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION] Malformed records are detected in record parsi
```

| Modo | Qué hace |
| --- | --- |
| `PERMISSIVE` (por defecto) | Pone `null` donde no puede y guarda la línea original en `_corrupt_record` (si está en el esquema). |
| `DROPMALFORMED` | Descarta las filas problemáticas **sin avisar**. |
| `FAILFAST` | Falla en la primera. |

Lo profesional suele ser `PERMISSIVE` con `_corrupt_record` y después separar: las filas buenas siguen;
las que tienen `_corrupt_record` van a una carpeta de errores para revisarlas. Es la misma idea que la cola
de mensajes fallidos del curso de Kafka. `DROPMALFORMED` es cómodo y peligroso: pierdes datos sin enterarte.

:::ejercicio Un CSV con tus propios tipos
1. Lee `_salida/terremotos.csv` con un esquema explícito en el que `sentido` sea `STRING`.
2. Escribe en Parquet solo los terremotos con `mag >= 6`, particionados por `red`.
3. ¿Cuántas carpetas se crean? Lee la carpeta de la red `us` y cuenta sus filas.

:::solucion
```python
esq = ("id STRING, hora TIMESTAMP, mag DOUBLE, tipo_mag STRING, lugar STRING, red STRING, "
       "sentido STRING, lon DOUBLE, lat DOUBLE, prof DOUBLE, mes INT")
csv = spark.read.option("header", True).schema(esq).csv(f"{SALIDA}/terremotos.csv")
csv.filter("mag >= 6").write.mode("overwrite").partitionBy("red").parquet(f"{SALIDA}/fuertes")
print(sorted(d for d in os.listdir(f"{SALIDA}/fuertes") if d.startswith("red=")))
print(spark.read.parquet(f"{SALIDA}/fuertes").filter("red = 'us'").count())
```

SOLUCION_03
:::

```quiz
¿Por qué Parquet ocupa mucho menos que JSON con los mismos datos?
* Porque guarda por columnas, con tipos y comprimido, y no repite los nombres de campo en cada fila.
- Porque descarta las columnas nulas.
- Porque Spark escribe JSON sin comprimir por error.
> Valores del mismo tipo seguidos comprimen muy bien, y el esquema se guarda una sola vez.
```

```quiz
Lees un CSV de 50 GB con `inferSchema`. ¿Qué coste extra tiene?
- Ninguno.
* Spark recorre los datos una vez más solo para deducir los tipos.
- Carga el fichero entero en el driver.
> Con un esquema explícito se ahorra esa pasada y se evitan tipos adivinados mal.
```

```quiz
Escribes con `partitionBy("fecha")` y consultas un solo día. ¿Qué lee Spark?
- Todos los ficheros, filtrando fila a fila.
* Solo la carpeta `fecha=` de ese día: el filtro de partición descarta las demás sin abrirlas.
- Solo el primer fichero.
> Es el filtro de partición (PartitionFilters) del plan.
```
