SELECT
    location_id,
    city,
    longitude,
    latitude,
    country,
    date_time AS event_ts,
    DATE_TRUNC('hour', date_time) AS event_hour,
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
FROM {{ source('raw', 'fact_weather_current') }}