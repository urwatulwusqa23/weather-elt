WITH stg AS (
    SELECT * FROM {{ ref('stg_weather_raw') }}
    WHERE has_temp_error = FALSE
)
SELECT
    city,
    COUNT(DISTINCT date)              AS days_of_data,
    ROUND(AVG(temp_max_c), 2)         AS avg_temp_max_c,
    ROUND(AVG(temp_min_c), 2)         AS avg_temp_min_c,
    MAX(temp_max_c)                   AS all_time_high_c,
    MIN(temp_min_c)                   AS all_time_low_c,
    ROUND(AVG(precipitation_mm), 2)   AS avg_daily_rain_mm,
    SUM(precipitation_mm)             AS total_rain_mm,
    ROUND(AVG(wind_speed_max_kmh), 2) AS avg_wind_kmh,
    MAX(wind_speed_max_kmh)           AS peak_wind_kmh
FROM stg
GROUP BY city
ORDER BY avg_temp_max_c DESC