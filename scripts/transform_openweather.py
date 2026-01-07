import json
import pandas as pd

with open('data/raw/openweather/2026-01-06/washingtondc.json') as f:
    raw = json.load(f)

data = pd.json_normalize(raw)

data['weather_description'] = data['weather'].apply(
    lambda x: x[0]['description'] if isinstance(x, list) and len(x) > 0 else None
)

data.rename(columns={
    'dt': 'date_time',
    'name': 'city',
    'coord.lon': 'longitude',
    'coord.lat': 'latitude',
    'main.temp': 'temperature',
    'main.feels_like': 'feels_like',
    'main.temp_min': 'temp_min',
    'main.temp_max': 'temp_max',
    'main.pressure': 'pressure',
    'main.humidity': 'humidity',
    'wind.speed': 'wind_speed',
    'wind.deg': 'wind_direction',
    'clouds.all': 'cloud_cover',
    'sys.country': 'country'
}, inplace=True)

data['location_id'] = (
    data['latitude'].astype(str) + "_" + data['longitude'].astype(str)
)

data['date_time'] = pd.to_datetime(data['date_time'], unit='s', utc=True)

data = data[[
    'location_id',
    'city',
    'longitude',
    'latitude',
    'country',
    'date_time',
    'temperature',
    'feels_like',
    'temp_min',
    'temp_max',
    'pressure',
    'humidity',
    'wind_speed',
    'wind_direction',
    'cloud_cover',
    'weather_description'
]]

data.to_csv('data/transformed/openweather.csv', index=False)