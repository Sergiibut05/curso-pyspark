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
.\curso.ps1                      # arranca Kafka y Jupyter, construye la imagen, carga los topics y abre el curso
.\curso.ps1 run 09_ventanas.py   # ejecuta una práctica (deja todo preparado si no lo estaba)
.\curso.ps1 cuadernos            # abre JupyterLab con las lecciones como cuadernos
.\curso.ps1 down                 # para el laboratorio (los datos de Kafka se conservan)
.\curso.ps1 reset                # borra datos de Kafka, salidas y checkpoints para empezar de cero
```

La primera vez tarda unos minutos (construye la imagen); las siguientes, segundos. Es idempotente: se puede
repetir sin duplicar datos en Kafka.

### Ejecutar el código desde la lección

Con el laboratorio en marcha, cada bloque de Python de las lecciones tiene un botón **Ejecutar**. El código
no corre en el navegador (Spark necesita Java), sino en un núcleo de Jupyter dentro de Docker (servicio
`jupyter`, solo en `127.0.0.1:38888`). Cada lección abierta tiene su propia sesión de Spark: los bloques
comparten variables, como las celdas de un cuaderno, y si ejecutas uno sin los anteriores se ejecutan antes
los que falten. El código se puede editar (Mayús+Intro ejecuta) y la barra de la lección tiene «Ejecutar
todo», «Interrumpir», «Reiniciar sesión» y «Abrir como cuaderno».

- `curso.ps1` genera un token aleatorio la primera vez y lo guarda en `laboratorio/.env` (para Docker) y en
  `assets/js/laboratorio-local.js` (para la web). Los dos están en `.gitignore`. Sin ellos, la lección se ve
  igual que siempre y los botones explican cómo arrancar el laboratorio.
- Cuadernos: `build.py` genera uno por lección en `cuadernos/` (los originales). Jupyter los copia la primera
  vez a `mis-cuadernos/`, que es donde trabajas y se guardan tus cambios; nunca se pisan. Para recuperar el
  original de una lección, borra su copia en `mis-cuadernos/` y reinicia el laboratorio (`.\curso.ps1`).
- Los núcleos sin uso se cierran solos a los 20 minutos; al cambiar de lección o recargar, se empieza de cero.

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
la sintaxis). Simuladores de este curso: `plan` y `microlotes`. `build.py` regenera también `cuadernos/`.

Los bloques de Python son **ejecutables** por defecto y se ejecutan en orden en la misma sesión, así que cada
uno debe funcionar tras los anteriores. Dos marcas en la valla:

| Valla | Para qué |
| --- | --- |
| ` ```python preparacion ` | Lo que la lección da por hecho (imports, `spark = sesion(…)`, DataFrames de partida, funciones auxiliares del script). Uno por lección, antes del primer bloque ejecutable. En la web sale plegado y se ejecuta solo; en el cuaderno es la primera celda. |
| ` ```python fragmento ` | Ilustrativo, no se ejecuta: sintaxis con `…`, rutas genéricas, trozos de dentro de una función. |

Los bloques de las soluciones (`:::solucion`) se ejecutan en la web si se pulsan, pero no se ejecutan solos
antes de los bloques posteriores. En el cuaderno van plegados, tras una celda vacía para la solución propia.
Si un bloque va seguido de una `salida`, conviene que imprima lo mismo al ejecutarlo.
