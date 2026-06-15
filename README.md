# Weather ETL Pipeline

An end-to-end data pipeline that ingests daily weather data for five Pakistani cities, transforms it with dbt, orchestrates everything with Apache Airflow, and visualises the results in a live Streamlit dashboard — all running in Docker.
<img width="3590" height="1675" alt="image" src="https://github.com/user-attachments/assets/db490bdc-a8f1-4e3f-a40d-fdfd63491682" />

## Architecture

```
Open-Meteo API
      │
      ▼
Python async ingestion (httpx + psycopg2)
      │
      ▼
PostgreSQL  ──  raw.weather_data
      │
      ▼
dbt  ──  staging.stg_weather_raw  ──►  marts.daily_weather_summary
                                   ──►  marts.city_weather_comparison
      │
      ▼
Apache Airflow (daily DAG orchestration)
      │
      ▼
Streamlit Dashboard (live pipeline status + weather charts)
```

**Pipeline tasks (run daily):**

| # | Task | What it does |
|---|------|--------------|
| 1 | `ingest_weather_data` | Async-fetches 7-day weather history for 5 cities from Open-Meteo |
| 2 | `validate_raw_data` | Asserts recent rows exist in `raw.weather_data` |
| 3 | `dbt_run` | Builds staging view + mart tables |
| 4 | `dbt_test` | Runs data-quality tests (e.g. temp range sanity check) |

## Cities tracked

Karachi · Lahore · Islamabad · Peshawar · Quetta . london . dubai .tokyo 


## Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.10+, httpx (async), psycopg2 |
| Storage | PostgreSQL 15 (3-layer schema: raw / staging / marts) |
| Transformation | dbt-core 1.8+, dbt-postgres |
| Orchestration | Apache Airflow 2.9 |
| Infrastructure | Docker Compose |
| Dashboard | Streamlit, Plotly, SQLAlchemy |

## Project Structure

```
weather-etl/
├── docker-compose.yml            # Postgres + Airflow services
├── init_db.sql                   # DB + schema bootstrap (raw/staging/marts)
├── .env                          # Secrets — not committed
├── requirements.txt
│
├── airflow/
│   ├── Dockerfile                # Airflow image with dbt installed
│   └── dags/
│       └── weather_etl_dag.py    # 4-task daily DAG
│
├── ingestion/
│   └── extract.py                # Async fetch → upsert into raw.weather_data
│
├── dbt_project/
│   ├── profiles.yml
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_weather_raw.sql          # Type casting, °C→°F, error flagging
│   │   └── marts/
│   │       ├── daily_weather_summary.sql    # 7-day rolling avg, monthly hottest-day rank
│   │       └── city_weather_comparison.sql  # Cross-city aggregates
│   └── tests/
│       └── assert_temp_range.sql            # Fails on temps outside −90°C / +60°C
│
└── dashboard/
    ├── app.py                    # Streamlit dashboard
    └── requirements.txt
```

## Data Model

| Schema | Object | Description |
|---|---|---|
| `raw` | `weather_data` | Raw daily readings, upserted on `(city, date)` |
| `staging` | `stg_weather_raw` | Cleaned types, unit conversions, `has_temp_error` flag |
| `marts` | `daily_weather_summary` | Daily metrics + 7-day rolling avg + monthly hottest-day rank |
| `marts` | `city_weather_comparison` | All-time aggregates per city (avg/max/min temp, rainfall, wind) |

## Setup

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running
- Python 3.10+
- Git

### 1. Clone the repo

```bash
git clone https://github.com/urwatulwusqa23/weather-etl.git
cd weather-etl
```

### 2. Create `.env`

Generate a Fernet key first:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Then create `.env` in the project root:

```env
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__FERNET_KEY=<paste your generated key>
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
```

### 3. Build and start the stack

```bash
docker compose build --no-cache
docker compose up -d
```

Wait for the webserver to be ready:

```bash
docker compose logs -f airflow-webserver
```

Look for `Listening at: 0.0.0.0:8080`, then press `Ctrl+C`.

> **Port note:** The Postgres container maps to host port `5434` (to avoid conflicts with any local Postgres instances on 5432/5433). Airflow UI is on `http://localhost:8081`.

### 4. Trigger the pipeline

Open **[http://localhost:8081](http://localhost:8081)** (login: `admin` / `admin`).

Find `weather_etl_pipeline`, toggle it **On**, and hit the ▶ button to trigger a manual run. All four task boxes should turn green.
<img width="3837" height="1860" alt="image" src="https://github.com/user-attachments/assets/fbeafa45-e8c8-4217-ae2b-25aae265da32" />

### 5. Run the dashboard

```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

Open **[http://localhost:8501](http://localhost:8501)**. The dashboard shows:

- Pipeline status table (last 5 DAG runs, colour-coded per task state)
- Task duration bar chart for the latest run
- Smart failure alerts (distinguishes root failures from blocked downstream tasks)
- Raw data overview metrics
- Temperature trends per city (max / min / avg tabs)
- City comparison charts (avg temps, total rainfall)
- Precipitation & wind speed dual-axis chart per city
  <img width="3570" height="1435" alt="image" src="https://github.com/user-attachments/assets/23b52403-8980-4531-a38e-f90f4da7da06" />

  <img width="3590" height="1675" alt="image" src="https://github.com/user-attachments/assets/1198675e-dab0-46fa-a462-60aa71c965d6" />

<img width="3680" height="1192" alt="image" src="https://github.com/user-attachments/assets/1c95f824-cb10-459e-be04-f3396ab74803" />

### 6. Query the data directly

```bash
docker exec -it weather-etl-postgres-1 psql -U airflow -d weather
```

```sql
-- Raw readings
SELECT city, date, temp_max, temp_min, precipitation
FROM raw.weather_data
ORDER BY date DESC
LIMIT 10;

-- City comparison (mart)
SELECT * FROM marts.city_weather_comparison;

-- Rolling 7-day average (mart)
SELECT city, date, temp_max_c, rolling_7d_avg_temp_max
FROM marts.daily_weather_summary
WHERE city = 'Karachi'
ORDER BY date DESC;
```

## Notes

- The DAG uses `catchup=False` and runs `@daily`. It fetches a 7-day rolling window from Open-Meteo and upserts on `(city, date)` — re-runs are safe.
- `stg_weather_raw` flags rows where `temp_max < temp_min` as `has_temp_error = TRUE`. Mart models filter these out.
- The Streamlit dashboard connects to both databases: `localhost:5434/airflow` for pipeline status and `localhost:5434/weather` for weather data. Override with `AIRFLOW_DSN` and `WEATHER_DSN` env vars if needed.
- dbt version must be `>=1.8.0,<2.0.0`. Version `1.7.x` has a `KeyError: 'javascript'` bug; `2.0.0-alpha.1` dropped the postgres adapter entirely.

## Stopping the stack

```bash
docker compose down          # stop and remove containers
docker compose down -v       # also wipe the Postgres data volume
```
