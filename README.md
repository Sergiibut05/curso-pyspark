# PySpark en la práctica

Curso corto de PySpark: DataFrames, evaluación perezosa, ventanas, particiones, Structured Streaming y
Kafka. El hilo es un problema real, fusionar los informes de terremotos de dos agencias (USGS y EMSC), que
el curso resuelve primero en lote (lección 22) y después en streaming de Kafka a Kafka (lección 23).

## Abrirlo

Doble clic en `index.html`. Funciona sin servidor (las lecciones están en `assets/data/*.js`). Necesita
conexión para las fuentes de Google y para `marked` y `highlight.js` (jsDelivr).

## Las prácticas

La carpeta es independiente: trae sus datos (`datos/`, 18 MB), sus scripts (`practicas/`, uno por lección)
y su laboratorio (`laboratorio/compose.yaml`): un broker de Kafka 4.1 y la imagen de las prácticas
(`docker/Dockerfile`: Python 3.12 + Java 17 + PySpark 4.2.1 + conector de Kafka). No hace falta instalar
Spark ni Java en Windows.

### Un solo comando

Con Docker Desktop en marcha, desde la raíz del repo (o doble clic en `curso.cmd`):

```powershell
.\curso.ps1                      # arranca Kafka, construye la imagen, carga los topics y abre el curso
.\curso.ps1 run 09_ventanas.py   # ejecuta una práctica (deja todo preparado si no lo estaba)
.\curso.ps1 down                 # para el laboratorio (los datos de Kafka se conservan)
.\curso.ps1 reset                # borra datos de Kafka, salidas y checkpoints para empezar de cero
```

La primera vez tarda unos minutos (construye la imagen); las siguientes, segundos. Es idempotente: se puede
repetir sin duplicar datos en Kafka.

### A mano

Lo que hace el script, por si quieres hacerlo paso a paso. En PowerShell, **desde la carpeta `laboratorio`**:

```powershell
docker compose up -d                                              # Kafka (kafka:19092 dentro de Docker)
function spark-py { docker compose run --rm spark python @args }  # la primera vez construye la imagen
spark-py 01_hola_spark.py 2>$null
spark-py 00_cargar_kafka.py 2>$null                               # antes de la lección 17, una vez
```

| Script | Necesita |
| --- | --- |
| `01`–`08`, `11` | Solo la imagen. |
| `00_cargar_kafka.py` | Kafka en marcha. Publica enero de 2025 de USGS (con revisiones) y EMSC en `eventos.usgs` y `eventos.emsc`. |
| `09`, `10`, `12` | Kafka con los topics cargados. |

`10` y `12` crean carpetas de checkpoint en `practicas/` (`_checkpoint-…`); bórralas para volver a empezar
desde el principio de los topics. `docker compose down -v` borra los datos de Kafka.

## Editarlo

Igual que el curso de Kafka: `contenido/NN-slug.md` y `python build.py` (ver `../curso-kafka/README.md` para
la sintaxis). Simuladores de este curso: `plan` y `microlotes`.
