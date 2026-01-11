SELECT
    location_id,
    city,
    country,
    latitude,
    longitude
FROM {{ref('stg_dim_location')}}
