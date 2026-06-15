FROM apache/airflow:2.9.0

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir "dbt-core>=1.8.0,<2.0.0" "dbt-postgres>=1.8.0,<2.0.0"
