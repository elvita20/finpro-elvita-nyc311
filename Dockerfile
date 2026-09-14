FROM apache/airflow:2.11.2-python3.12


# ==========================================================
# FINPRO ELVITA - AIRFLOW IMAGE
# ==========================================================


USER root


# Directory used for mounted Google ADC credential
RUN mkdir -p /opt/airflow/gcp \
    && chown -R airflow:0 /opt/airflow/gcp


USER airflow


# Install project-specific Airflow dependencies
COPY requirements-airflow.txt /requirements-airflow.txt


RUN pip install --no-cache-dir \
    "apache-airflow==2.11.2" \
    -r /requirements-airflow.txt