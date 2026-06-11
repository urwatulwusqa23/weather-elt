WITH source AS (
    SELECT * FROM raw.weather_data
),
cleaned AS (
    SELECT
        id, city,
        latitude::FLOAT   AS latitude,
        longitude::FLOAT  AS longitude,
        date, fetched_at,

        temp_max::NUMERIC(5,2)  AS temp_max_c,
        temp_min::NUMERIC(5,2)  AS temp_min_c,

        ROUND((temp_max * 9.0/5.0 + 32)::NUMERIC, 2) AS temp_max_f,
        ROUND((temp_min * 9.0/5.0 + 32)::NUMERIC, 2) AS temp_min_f,
        ROUND((temp_max - temp_min)::NUMERIC, 2)       AS temp_range_c,

        precipitation::NUMERIC(6,2)   AS precipitation_mm,
        wind_speed_max::NUMERIC(6,2)  AS wind_speed_max_kmh,
        weather_code,

        CASE WHEN temp_max < temp_min
             THEN TRUE ELSE FALSE END AS has_temp_error
    FROM source
    WHERE temp_max IS NOT NULL
      AND temp_min IS NOT NULL
)
SELECT * FROM cleaned