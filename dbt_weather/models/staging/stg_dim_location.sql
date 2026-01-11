SELECT
    location_id,
    city,
    country,
    latitude,
    longitude
FROM {{ source('raw', 'dim_location') }}
