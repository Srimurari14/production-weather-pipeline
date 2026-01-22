{{ 
  config(
    materialized = 'incremental',
    unique_key = ['location_id', 'event_ts'],
    on_schema_change = 'append_new_columns'
  ) 
}}

SELECT
    location_id,
    event_ts,
    event_hour,
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
FROM {{ ref('stg_fact_weather_current') }}

{% if is_incremental() %}
WHERE event_ts > (
    SELECT COALESCE(MAX(event_ts), TO_TIMESTAMP('1900-01-01'))
    FROM {{ this }}
)
{% endif %}
