# Imagen de las prácticas del curso de PySpark: Python 3.12 + Java 17 + PySpark 4.2.1 (con pandas y pyarrow)
# + pytest + conector de Kafka. La construye el laboratorio la primera vez que se usa:
#   cd laboratorio; docker compose run --rm spark python 01_hola_spark.py
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    SPARK_JARS_DIR=/opt/spark-jars

# Spark corre en la JVM; `procps` lo usan sus scripts de arranque.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# El paquete pesa 450 MB: con una conexión lenta, pip necesita más paciencia que la de serie (15 s, 5 reintentos).
# El extra [sql] trae pandas y pyarrow (pandas UDF, toPandas con Arrow); pytest, para la lección de pruebas.
RUN pip install --timeout 120 --retries 10 "pyspark[sql]==4.2.1" pytest==8.4.2

# Jupyter: el núcleo que ejecuta los bloques de las lecciones desde la web del curso y JupyterLab para los
# cuadernos (cuadernos/*.ipynb). En una capa aparte para no volver a bajar PySpark al cambiarla.
RUN pip install --timeout 120 --retries 10 jupyterlab==4.5.11 jupyterlab-language-pack-es-ES==4.5.post3 \
    && mkdir -p /usr/local/share/jupyter/lab/settings \
    && printf '%s\n' '{"@jupyterlab/translation-extension:plugin": {"locale": "es_ES"},' \
       ' "@jupyterlab/apputils-extension:notification": {"fetchNews": "false", "checkForUpdates": false}}' \
       > /usr/local/share/jupyter/lab/settings/overrides.json \
    && mkdir -p /etc/ipython \
    && printf '%s\n' 'c.IPKernelApp.capture_fd_output = False' \
       'c.IPKernelApp.exec_lines = [' \
       '    "import logging, warnings",' \
       '    "logging.getLogger(\"DataFrameQueryContextLogger\").setLevel(logging.CRITICAL)",' \
       '    "warnings.filterwarnings(\"ignore\", message=\"PySpark does not yet fully support pandas\")",' \
       ']' > /etc/ipython/ipython_kernel_config.py
# Lo que con los scripts se oculta con 2>$null tampoco ensucia las celdas: los avisos de arranque de la JVM y las
# barras de progreso de Spark (los escribe Java: van al log del contenedor), el log en JSON que repite cada error
# de análisis y el aviso de PySpark 4.2 sobre pandas 3.

# El conector de Kafka y sus dependencias se descargan aquí, una vez: las prácticas no bajan nada al arrancar.
COPY jars.py /tmp/jars.py
RUN python /tmp/jars.py && rm /tmp/jars.py

RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin alumno
USER alumno
WORKDIR /practicas
