from __future__ import annotations
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

PG_CONN = (
    "postgresql+psycopg2://airflow:airflow@postgres:5432/weather"
)
DBT_DIR = "/opt/airflow/dbt_project"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def ingest_weather(**ctx):
    from ingestion.extract import run_ingestion
    n = run_ingestion(conn_str=PG_CONN)
    print(f"Ingested {n} records")
    ctx["ti"].xcom_push(key="rows_loaded", value=n)


def validate_raw(**ctx):
    import psycopg2
    with psycopg2.connect(PG_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM raw.weather_data
                WHERE date >= CURRENT_DATE - INTERVAL '1 day'
            """)
            count = cur.fetchone()[0]
            if count == 0:
                raise ValueError("No recent data in raw.weather_data!")
            print(f"Validation passed. Recent rows: {count}")


with DAG(
    dag_id="weather_etl_pipeline",
    description="Daily: ingest -> validate -> dbt run -> dbt test",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "weather", "dbt"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_weather_data",
        python_callable=ingest_weather,
    )

    validate = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
    )

    # Task order: ingest -> validate -> dbt run -> dbt test
    ingest >> validate >> dbt_run >> dbt_test