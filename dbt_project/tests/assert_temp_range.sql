-- dbt test: fails if any temperature is physically impossible.
-- Any rows returned = test failure.
SELECT *
FROM {{ ref('stg_weather_raw') }}
WHERE temp_max_c > 60
   OR temp_min_c < -90