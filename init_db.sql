CREATE DATABASE weather;

\c weather;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.weather_data (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(100) NOT NULL,
    latitude        NUMERIC(8,4) NOT NULL,
    longitude       NUMERIC(8,4) NOT NULL,
    fetched_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    date            DATE NOT NULL,
    temp_max        NUMERIC(5,2),
    temp_min        NUMERIC(5,2),
    precipitation   NUMERIC(6,2),
    wind_speed_max  NUMERIC(6,2),
    weather_code    INTEGER,
    UNIQUE (city, date)
);