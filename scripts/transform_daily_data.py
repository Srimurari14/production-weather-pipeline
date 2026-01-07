import pandas as pd

location_df = pd.read_csv(
    "data/raw/historical/washingtondc_2020_2025_daily.csv",
    nrows=1
)

location_df['location_id'] = (
    location_df['latitude'].astype(str) + "_" +
    location_df['longitude'].astype(str)
)

location_id = location_df.loc[0, 'location_id']




daily_df = pd.read_csv(
    "data/raw/historical/washingtondc_2020_2025_daily.csv",
    skip_blank_lines=True,
    skiprows=2
)

daily_df.rename(columns={
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


daily_df = daily_df[
    [
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


daily_df['date'] = pd.to_datetime(daily_df['date'])


daily_df['location_id'] = location_id


daily_df = daily_df[
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

daily_df.to_csv('data/transformed/DC_daily.csv', index=False)