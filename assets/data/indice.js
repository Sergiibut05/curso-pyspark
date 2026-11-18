// Generado por build.py; no editar a mano.
window.CURSO = {"id": "curso-pyspark", "titulo": "PySpark en la práctica", "practica": "Con el laboratorio en marcha"};
window.INDICE = [
 {
  "numero": 1,
  "slug": "que-es-spark",
  "titulo": "Qué es Spark y qué hace aquí",
  "resumen": "Un motor para procesar datos en paralelo. Driver, ejecutores, modo local, el laboratorio del curso y el caso real que sirve de hilo.",
  "bloque": "Fundamentos",
  "minutos": 12,
  "practica": "si",
  "cuaderno": "01-que-es-spark.ipynb"
 },
 {
  "numero": 2,
  "slug": "dataframes",
  "titulo": "SparkSession y DataFrames",
  "resumen": "Leer el GeoJSON de 2025, entender el esquema que deduce Spark y aplanarlo en una tabla.",
  "bloque": "Fundamentos",
  "minutos": 20,
  "practica": "Con Docker, sin la pila",
  "cuaderno": "02-dataframes.ipynb"
 },
 {
  "numero": 3,
  "slug": "leer-y-escribir",
  "titulo": "Leer y escribir - formatos, esquemas y particiones en disco",
  "resumen": "CSV, JSON y Parquet con los mismos datos. Inferir el esquema o darlo, modos de escritura, particionar por columnas y qué hacer con los registros corruptos.",
  "bloque": "Fundamentos",
  "minutos": 25,
  "practica": "si",
  "cuaderno": "03-leer-y-escribir.ipynb"
 },
 {
  "numero": 4,
  "slug": "evaluacion-perezosa",
  "titulo": "Transformaciones, acciones y el plan",
  "resumen": "Spark no hace nada hasta que se lo pides. Qué es una acción, qué es un shuffle y cómo leer explain().",
  "bloque": "Fundamentos",
  "minutos": 18,
  "practica": "Con Docker, sin la pila",
  "cuaderno": "04-evaluacion-perezosa.ipynb"
 },
 {
  "numero": 5,
  "slug": "columnas-y-funciones",
  "titulo": "Columnas, funciones y nulos",
  "resumen": "withColumn, when, fechas, nulos que no se rellenan y JSON dentro de una columna, como en la fusión.",
  "bloque": "Transformar datos",
  "minutos": 20,
  "practica": "Con Docker, sin la pila",
  "cuaderno": "05-columnas-y-funciones.ipynb"
 },
 {
  "numero": 6,
  "slug": "tipos-complejos",
  "titulo": "Tipos complejos - struct, array y map",
  "resumen": "Datos anidados como vienen de las APIs. Navegarlos, construirlos, explotarlos, trabajar dentro de un array sin explotarlo y leer JSON con un esquema.",
  "bloque": "Transformar datos",
  "minutos": 25,
  "practica": "si",
  "cuaderno": "06-tipos-complejos.ipynb"
 },
 {
  "numero": 7,
  "slug": "agregaciones",
  "titulo": "Agregaciones y joins",
  "resumen": "groupBy con varias métricas, filtros por zona y uniones con una tabla pequeña (broadcast).",
  "bloque": "Transformar datos",
  "minutos": 18,
  "practica": "Con Docker, sin la pila",
  "cuaderno": "07-agregaciones.ipynb"
 },
 {
  "numero": 8,
  "slug": "joins",
  "titulo": "Joins a fondo",
  "resumen": "Los seis tipos de join con cada fila a la vista, los nulos en la clave, las claves repetidas que multiplican filas, las columnas duplicadas y cómo decide Spark entre broadcast y sort-merge.",
  "bloque": "Transformar datos",
  "minutos": 25,
  "practica": "si",
  "cuaderno": "08-joins.ipynb"
 },
 {
  "numero": 9,
  "slug": "ventanas",
  "titulo": "Funciones de ventana",
  "resumen": "row_number, lag y rankings por grupo. La ventana exacta que usa la fusión para quedarse con la última versión.",
  "bloque": "Transformar datos",
  "minutos": 20,
  "practica": "Con Docker, sin la pila",
  "cuaderno": "09-ventanas.ipynb"
 },
 {
  "numero": 10,
  "slug": "spark-sql",
  "titulo": "Spark SQL",
  "resumen": "Consultar DataFrames con SQL. Vistas temporales, por qué SQL y la API dan el mismo plan, CTE y ventanas, parámetros seguros y tablas guardadas en el catálogo.",
  "bloque": "Transformar datos",
  "minutos": 20,
  "practica": "si",
  "cuaderno": "10-spark-sql.ipynb"
 },
 {
  "numero": 11,
  "slug": "udfs",
  "titulo": "UDF - funciones propias en Python, y lo que cuestan",
  "resumen": "Cuándo hace falta una función propia, cuánto más lenta es que una de Spark, por qué las pandas UDF con Arrow lo arreglan casi del todo, los fallos silenciosos y applyInPandas.",
  "bloque": "Transformar datos",
  "minutos": 25,
  "practica": "si",
  "cuaderno": "11-udfs.ipynb"
 },
 {
  "numero": 12,
  "slug": "particiones-y-shuffle",
  "titulo": "Particiones, shuffle y el driver",
  "resumen": "Cómo se reparten los datos, por qué un fichero de una línea es un problema y qué cuesta collect().",
  "bloque": "Rendimiento",
  "minutos": 18,
  "practica": "Con Docker, sin la pila",
  "cuaderno": "12-particiones-y-shuffle.ipynb"
 },
 {
  "numero": 13,
  "slug": "cache-y-persistencia",
  "titulo": "Cache y persistencia",
  "resumen": "Por qué Spark repite el trabajo en cada acción, cómo guardarlo con cache o persist, qué significa que sea perezoso, los niveles de almacenamiento y cuándo no compensa.",
  "bloque": "Rendimiento",
  "minutos": 18,
  "practica": "si",
  "cuaderno": "13-cache-y-persistencia.ipynb"
 },
 {
  "numero": 14,
  "slug": "planes-y-spark-ui",
  "titulo": "Planes, ejecución adaptativa y la interfaz web",
  "resumen": "Leer explain en modo formatted, qué cambia AQE mientras la consulta corre, y la interfaz web de Spark (trabajos, etapas, tareas) en el navegador y por su API.",
  "bloque": "Rendimiento",
  "minutos": 25,
  "practica": "si",
  "cuaderno": "14-planes-y-spark-ui.ipynb"
 },
 {
  "numero": 15,
  "slug": "optimizar",
  "titulo": "Optimizar - leer menos, mover menos, repartir mejor",
  "resumen": "Filtros y columnas que Parquet se salta, el problema de los ficheros pequeños, el sesgo de datos, la técnica de la sal y cómo AQE parte un join sesgado.",
  "bloque": "Rendimiento",
  "minutos": 30,
  "practica": "si",
  "cuaderno": "15-optimizar.ipynb"
 },
 {
  "numero": 16,
  "slug": "structured-streaming",
  "titulo": "Structured Streaming - micro-lotes y checkpoint",
  "resumen": "Un flujo es una tabla que no deja de crecer. Disparadores, sumideros, foreachBatch y el directorio de checkpoint.",
  "bloque": "Streaming",
  "minutos": 22,
  "practica": "Con Docker, sin la pila",
  "cuaderno": "16-structured-streaming.ipynb"
 },
 {
  "numero": 17,
  "slug": "kafka-con-spark",
  "titulo": "Leer y escribir Kafka con Spark",
  "resumen": "Las columnas que da la fuente de Kafka, las opciones que importan y cómo se escribe de vuelta.",
  "bloque": "Streaming",
  "minutos": 22,
  "practica": "si",
  "cuaderno": "17-kafka-con-spark.ipynb"
 },
 {
  "numero": 18,
  "slug": "foreachbatch-y-estado",
  "titulo": "foreachBatch, estado e idempotencia",
  "resumen": "Leer Kafka en streaming con checkpoint, y por qué el estado de la fusión vive en PostgreSQL y no en Spark.",
  "bloque": "Streaming",
  "minutos": 25,
  "practica": "si",
  "cuaderno": "18-foreachbatch-y-estado.ipynb"
 },
 {
  "numero": 19,
  "slug": "estado-en-spark",
  "titulo": "Estado en Spark - ventanas de tiempo, watermark y duplicados",
  "resumen": "Agregaciones en streaming que recuerdan entre lotes. Tiempo del evento, ventanas, watermark, modos de salida, datos tardíos, duplicados y joins entre flujos.",
  "bloque": "Streaming",
  "minutos": 30,
  "practica": "si",
  "cuaderno": ""
 },
 {
  "numero": 22,
  "slug": "mini-fusion",
  "titulo": "Proyecto: la regla de la fusión, solo con Spark",
  "resumen": "Enero de 2025, USGS contra EMSC, ≤ 30 s y ≤ 100 km. Un join por rango sin comparar todo con todo, y por qué el resultado difiere en 8 del real.",
  "bloque": "Proyecto",
  "minutos": 30,
  "practica": "si",
  "cuaderno": "22-mini-fusion.ipynb"
 },
 {
  "numero": 23,
  "slug": "fusion-en-streaming",
  "titulo": "Proyecto: la fusión en streaming, de Kafka a Kafka",
  "resumen": "La regla de la lección 22 dentro de foreachBatch, leyendo los dos topics y escribiendo el resultado en Kafka. Checkpoint, reejecuciones y el fallo que aparece cuando las parejas caen en lotes distintos.",
  "bloque": "Proyecto",
  "minutos": 30,
  "practica": "si",
  "cuaderno": "23-fusion-en-streaming.ipynb"
 },
 {
  "numero": 24,
  "slug": "chuleta",
  "titulo": "Chuleta, errores frecuentes y glosario",
  "resumen": "La API que más se usa en una página, los fallos típicos de PySpark con su causa y las palabras clave.",
  "bloque": "Proyecto",
  "minutos": 10,
  "practica": "",
  "cuaderno": ""
 }
];
