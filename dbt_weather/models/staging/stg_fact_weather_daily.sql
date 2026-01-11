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
FROM {{ source('raw', 'fact_weather_daily') }}