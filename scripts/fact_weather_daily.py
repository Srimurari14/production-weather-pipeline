import pandas as pd
from configs.cities import CITIES

all_daily = []

for city_cfg in CITIES:
    df = pd.read_csv(
        city_cfg["historical_file"],
        skip_blank_lines=True,
        skiprows=2
    )

    df.rename(columns={
        'time': 'date',
        'temperature_2m_mean (°C)': 'temp_mean',
        'temperature_2m_max (°C)': 'temp_max',
        'temperature_2m_min (°C)': 'temp_min',
        'precipitation_sum (mm)': 'precipitation',
        'rain_sum (mm)': 'rain',
        'snowfall_sum (cm)': 'snowfall',
        'wind_speed_10m_max (km/h)': 'wind_speed',
        'wind_direction_10m_dominant (°)': 'wind_direction',
        'daylight_duration (s)': 'daylight_duration'
    }, inplace=True)

    df['date'] = pd.to_datetime(df['date'])
    df['location_id'] = city_cfg["location_id"]

    df = df[
    [
        'location_id',
        'date',
        'temp_mean',
        'temp_max',
        'temp_min',
        'precipitation',
        'rain',
        'snowfall',
        'wind_speed',
        'wind_direction',
        'daylight_duration'
    ]
    ]

    all_daily.append(df)

fact_weather_daily = pd.concat(all_daily, ignore_index=True)

fact_weather_daily.to_csv('data/transformed/fact_weather_daily.csv', index=False)