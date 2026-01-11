SELECT
    location_id,
    date_time,
    temperature,
    feels_like,
    temp_min,
    temp_max,
    pressure,
    humidity,
    wind_speed,
    wind_direction,
    cloud_cover,
    weather_description
FROM {{ref('stg_fact_weather_current')}}
