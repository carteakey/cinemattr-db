FROM apache/airflow:latest-python3.10
USER 0
RUN apt-get update \
  && apt-get install -y --no-install-recommends gcc g++
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir airflow-provider-duckdb duckdb==0.7.1 -r /requirements.txt
