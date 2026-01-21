def run_fact_weather_current():
    import json
    import pandas as pd
    from configs.cities import CITIES
    from scripts.validations import validate_fact_weather_current, validate_dim_location

    all_current = []

    for city_cfg in CITIES:
        with open(city_cfg["current_file"]) as f:
            raw = json.load(f)

        df = pd.json_normalize(raw)

        df['weather_description'] = df['weather'].apply(
            lambda x: x[0]['description'] if isinstance(x, list) and len(x) > 0 else None
        )

        df.rename(columns={
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

        df['location_id'] = city_cfg["location_id"]
        df['date_time'] = pd.to_datetime(df['date_time'], unit='s', utc=True)

        df = df[
            [
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
            ]
        ]

        all_current.append(df)

    fact_weather_current = pd.concat(all_current, ignore_index=True)

    dim_location = (
        fact_weather_current[
            ['location_id', 'city', 'country', 'latitude', 'longitude']
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    validate_dim_location(dim_location)
    validate_fact_weather_current(fact_weather_current, dim_location)

    print(fact_weather_current.date_time.max())

    fact_weather_current.to_csv('data/transformed/fact_weather_current.csv', index=False)
    dim_location.to_csv("data/transformed/dim_location.csv", index=False)
