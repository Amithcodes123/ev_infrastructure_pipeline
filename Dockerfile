FROM apache/airflow:2.7.0-python3.10
USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jdk && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r /requirements.txt
WORKDIR /opt/airflow
COPY --chown=airflow:root .env gcp-service-account.json profiles.yml ingest_static_sources.py ingest_tomtom_velocity.py /opt/airflow/
COPY --chown=airflow:root dags/ /opt/airflow/dags/
ENV PROJECT_DIR=/opt/airflow
ENV PYTHON_EXEC=python3