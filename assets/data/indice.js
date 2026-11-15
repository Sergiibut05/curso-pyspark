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
  "practica": "si"
 },
 {
  "numero": 2,
  "slug": "dataframes",
  "titulo": "SparkSession y DataFrames",
  "resumen": "Leer el GeoJSON de 2025, entender el esquema que deduce Spark y aplanarlo en una tabla.",
  "bloque": "Fundamentos",
  "minutos": 20,
  "practica": "Con Docker, sin la pila"
 },
 {
  "numero": 3,
  "slug": "evaluacion-perezosa",
  "titulo": "Transformaciones, acciones y el plan",
  "resumen": "Spark no hace nada hasta que se lo pides. Qué es una acción, qué es un shuffle y cómo leer explain().",
  "bloque": "Fundamentos",
  "minutos": 18,
  "practica": "Con Docker, sin la pila"
 },
 {
  "numero": 4,
  "slug": "columnas-y-funciones",
  "titulo": "Columnas, funciones y nulos",
  "resumen": "withColumn, when, fechas, nulos que no se rellenan y JSON dentro de una columna, como en la fusión.",
  "bloque": "Transformar datos",
  "minutos": 20,
  "practica": "Con Docker, sin la pila"
 },
 {
  "numero": 5,
  "slug": "agregaciones-y-joins",
  "titulo": "Agregaciones y joins",
  "resumen": "groupBy con varias métricas, filtros por zona y uniones con una tabla pequeña (broadcast).",
  "bloque": "Transformar datos",
  "minutos": 18,
  "practica": "Con Docker, sin la pila"
 },
 {
  "numero": 6,
  "slug": "ventanas",
  "titulo": "Funciones de ventana",
  "resumen": "row_number, lag y rankings por grupo. La ventana exacta que usa la fusión para quedarse con la última versión.",
  "bloque": "Transformar datos",
  "minutos": 20,
  "practica": "Con Docker, sin la pila"
 },
 {
  "numero": 7,
  "slug": "particiones-y-shuffle",
  "titulo": "Particiones, shuffle y el driver",
  "resumen": "Cómo se reparten los datos, por qué un fichero de una línea es un problema y qué cuesta collect().",
  "bloque": "Rendimiento",
  "minutos": 18,
  "practica": "Con Docker, sin la pila"
 },
 {
  "numero": 8,
  "slug": "structured-streaming",
  "titulo": "Structured Streaming - micro-lotes y checkpoint",
  "resumen": "Un flujo es una tabla que no deja de crecer. Disparadores, sumideros, foreachBatch y el directorio de checkpoint.",
  "bloque": "Streaming",
  "minutos": 22,
  "practica": "Con Docker, sin la pila"
 },
 {
  "numero": 9,
  "slug": "kafka-con-spark",
  "titulo": "Leer y escribir Kafka con Spark",
  "resumen": "Las columnas que da la fuente de Kafka, las opciones que importan y cómo se escribe de vuelta.",
  "bloque": "Streaming",
  "minutos": 22,
  "practica": "si"
 },
 {
  "numero": 10,
  "slug": "foreachbatch-y-estado",
  "titulo": "foreachBatch, estado e idempotencia",
  "resumen": "Leer Kafka en streaming con checkpoint, y por qué el estado de la fusión vive en PostgreSQL y no en Spark.",
  "bloque": "Streaming",
  "minutos": 25,
  "practica": "si"
 },
 {
  "numero": 11,
  "slug": "mini-fusion",
  "titulo": "Proyecto: la regla de la fusión, solo con Spark",
  "resumen": "Enero de 2025, USGS contra EMSC, ≤ 30 s y ≤ 100 km. Un join por rango sin comparar todo con todo, y por qué el resultado difiere en 8 del real.",
  "bloque": "Proyecto",
  "minutos": 30,
  "practica": "si"
 },
 {
  "numero": 12,
  "slug": "fusion-en-streaming",
  "titulo": "Proyecto: la fusión en streaming, de Kafka a Kafka",
  "resumen": "La regla de la lección 11 dentro de foreachBatch, leyendo los dos topics y escribiendo el resultado en Kafka. Checkpoint, reejecuciones y el fallo que aparece cuando las parejas caen en lotes distintos.",
  "bloque": "Proyecto",
  "minutos": 30,
  "practica": "si"
 },
 {
  "numero": 13,
  "slug": "chuleta",
  "titulo": "Chuleta, errores frecuentes y glosario",
  "resumen": "La API que más se usa en una página, los fallos típicos de PySpark con su causa y las palabras clave.",
  "bloque": "Proyecto",
  "minutos": 10,
  "practica": ""
 }
];
