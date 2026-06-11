WITH stg AS (
    SELECT * FROM {{ ref('stg_weather_raw') }}
    WHERE has_temp_error = FALSE
)
SELECT
    city, date,
    temp_max_c, temp_min_c,
    temp_max_f, temp_min_f,
    temp_range_c,
    precipitation_mm,
    wind_speed_max_kmh,
    weather_code,

    AVG(temp_max_c) OVER (
        PARTITION BY city ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg_temp_max,

    RANK() OVER (
        PARTITION BY city, DATE_TRUNC('month', date)
        ORDER BY temp_max_c DESC
    ) AS hottest_day_rank_in_month

FROM stg
ORDER BY city, date