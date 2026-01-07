import pandas as pd

current_df = pd.read_csv("data/transformed/fact_weather_current.csv")

dim_location = (
    current_df[
        ['location_id', 'city', 'country', 'latitude', 'longitude']
    ]
    .drop_duplicates()
    .reset_index(drop=True)
)

dim_location.to_csv("data/transformed/dim_location.csv", index=False)
