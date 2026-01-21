import os
import json
import pandas as pd
from datetime import datetime
from configs.cities import CITIES
from scripts.validations import (
    validate_fact_weather_current,
    validate_dim_location
)

def fact_weather_current():
    all_current = []

    run_date = datetime.utcnow().strftime("%Y-%m-%d")

    for city_cfg in CITIES:
        city = city_cfg["city"]
        location_id = city_cfg["location_id"]

        city_slug = city.lower().replace(" ", "").replace(".", "")
        file_path = f"/opt/airflow/data/raw/{city_slug}.json"

        print(f"Reading weather file: {file_path}")

        with open(file_path) as f:
            raw = json.load(f)

        df = pd.json_normalize(raw)

        df["weather_description"] = df["weather"].apply(
            lambda x: x[0]["description"]
            if isinstance(x, list) and len(x) > 0
            else None
        )

        df.rename(
            columns={
                "dt": "date_time",
                "name": "city",
                "coord.lon": "longitude",
                "coord.lat": "latitude",
                "main.temp": "temperature",
                "main.feels_like": "feels_like",
                "main.temp_min": "temp_min",
                "main.temp_max": "temp_max",
                "main.pressure": "pressure",
                "main.humidity": "humidity",
                "wind.speed": "wind_speed",
                "wind.deg": "wind_direction",
                "clouds.all": "cloud_cover",
                "sys.country": "country",
            },
            inplace=True,
        )

        df["location_id"] = location_id
        df["date_time"] = pd.to_datetime(df["date_time"], unit="s", utc=True)

        df = df[
            [
                "location_id",
                "city",
                "longitude",
                "latitude",
                "country",
                "date_time",
                "temperature",
                "feels_like",
                "temp_min",
                "temp_max",
                "pressure",
                "humidity",
                "wind_speed",
                "wind_direction",
                "cloud_cover",
                "weather_description",
            ]
        ]

        all_current.append(df)

    fact_weather_current_df = pd.concat(all_current, ignore_index=True)

    dim_location_df = (
        fact_weather_current_df[
            ["location_id", "city", "country", "latitude", "longitude"]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    validate_dim_location(dim_location_df)
    validate_fact_weather_current(fact_weather_current_df, dim_location_df)

    output_dir = "/opt/airflow/data/transformed"
    os.makedirs(output_dir, exist_ok=True)

    fact_weather_current_path = f"{output_dir}/fact_weather_current.csv"
    dim_location_path = f"{output_dir}/dim_location.csv"

    fact_weather_current_df.to_csv(fact_weather_current_path, index=False)
    dim_location_df.to_csv(dim_location_path, index=False)

    print(f"Saved fact_weather_current to {fact_weather_current_path}")
    print(fact_weather_current_df.date_time.max())
    print(f"Saved dim_location to {dim_location_path}")


if __name__ == "__main__":
    fact_weather_current()
