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

# El conector de Kafka y sus dependencias se descargan aquí, una vez: las prácticas no bajan nada al arrancar.
COPY jars.py /tmp/jars.py
RUN python /tmp/jars.py && rm /tmp/jars.py

RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin alumno
USER alumno
WORKDIR /practicas
