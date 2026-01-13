{{ config(materialized='table') }}

SELECT
    location_id,
    date,
    temp_mean,
    temp_max,
    temp_min,
    precipitation,
    rain,
    snowfall,
    wind_speed,
    wind_direction,
    daylight_duration
FROM {{ref('stg_fact_weather_daily')}}

