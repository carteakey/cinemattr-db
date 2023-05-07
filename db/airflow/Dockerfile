FROM apache/airflow:2.6.0
USER 0
RUN apt-get update \
  && apt-get install -y --no-install-recommends gcc g++
USER airflow
RUN pip install --no-cache-dir airflow-provider-duckdb duckdb