from __future__ import annotations
import asyncio
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import httpx
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)
BASE_URL = "https://api.open-meteo.com/v1/forecast"

CITIES: list[dict[str, Any]] = [
    {"name": "London",   "lat": 51.5074,  "lon": -0.1278},
    {"name": "New York", "lat": 40.7128,  "lon": -74.0060},
    {"name": "Tokyo",    "lat": 35.6762,  "lon": 139.6503},
    {"name": "Sydney",   "lat": -33.8688, "lon": 151.2093},
    {"name": "Dubai",    "lat": 25.2048,  "lon": 55.2708},
]

DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
    "weather_code",
]


@dataclass
class WeatherRecord:
    city: str
    latitude: float
    longitude: float
    date: date
    temp_max: float | None
    temp_min: float | None
    precipitation: float | None
    wind_speed_max: float | None
    weather_code: int | None


async def fetch_city(client, city, start_date, end_date):
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": ",".join(DAILY_VARS),
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "UTC",
    }
    try:
        r = await client.get(BASE_URL, params=params, timeout=15.0)
        r.raise_for_status()
        data = r.json()
    except httpx.HTTPError as e:
        logger.error("Error fetching %s: %s", city["name"], e)
        return []

    daily = data.get("daily", {})
    return [
        WeatherRecord(
            city=city["name"],
            latitude=city["lat"],
            longitude=city["lon"],
            date=date.fromisoformat(d),
            temp_max=daily.get("temperature_2m_max", [None])[i],
            temp_min=daily.get("temperature_2m_min", [None])[i],
            precipitation=daily.get("precipitation_sum", [None])[i],
            wind_speed_max=daily.get("wind_speed_10m_max", [None])[i],
            weather_code=daily.get("weather_code", [None])[i],
        )
        for i, d in enumerate(daily.get("time", []))
    ]


async def fetch_all(days_back=7):
    end = date.today().isoformat()
    start = (date.today() - timedelta(days=days_back)).isoformat()
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(
            *[fetch_city(client, c, start, end) for c in CITIES]
        )
    return [r for city_rows in results for r in city_rows]


def load_to_postgres(records, conn_str):
    if not records:
        logger.warning("No records to load.")
        return 0
    rows = [
        (r.city, r.latitude, r.longitude, r.date,
         r.temp_max, r.temp_min, r.precipitation,
         r.wind_speed_max, r.weather_code)
        for r in records
    ]
    sql = """
        INSERT INTO raw.weather_data
            (city, latitude, longitude, date, temp_max, temp_min,
             precipitation, wind_speed_max, weather_code)
        VALUES %s
        ON CONFLICT (city, date) DO UPDATE SET
            temp_max       = EXCLUDED.temp_max,
            temp_min       = EXCLUDED.temp_min,
            precipitation  = EXCLUDED.precipitation,
            wind_speed_max = EXCLUDED.wind_speed_max,
            weather_code   = EXCLUDED.weather_code,
            fetched_at     = NOW();
    """
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
    logger.info("Loaded %d records", len(rows))
    return len(rows)


def run_ingestion(conn_str, days_back=7):
    """Entry point called by Airflow."""
    records = asyncio.run(fetch_all(days_back=days_back))
    return load_to_postgres(records, conn_str)