import pandas as pd

def validate_dim_location(df):
    assert df['location_id'].notnull().all(), "location_id has nulls"
    assert df['city'].notnull().all(), "city has nulls"
    assert df['country'].notnull().all(), "country has nulls"

    assert df['location_id'].is_unique, "location_id is not unique"

    assert df['latitude'].between(-90, 90).all(), "invalid latitude"
    assert df['longitude'].between(-180, 180).all(), "invalid longitude"



def validate_fact_weather_current(df, dim_location_df):
    assert df['location_id'].notnull().all(), "location_id nulls"
    assert df['date_time'].notnull().all(), "date_time nulls"
    assert df['temperature'].notnull().all(), "temperature nulls"

    assert not df.duplicated(
        subset=['location_id', 'date_time']
    ).any(), "duplicate (location_id, date_time)"

    assert df['location_id'].isin(
        dim_location_df['location_id']
    ).all(), "invalid location_id in facts"

    assert df['temperature'].between(-50, 60).all(), "bad temperature"
    assert df['humidity'].between(0, 100).all(), "bad humidity"
    assert df['cloud_cover'].between(0, 100).all(), "bad cloud cover"
    assert df['wind_speed'].ge(0).all(), "bad wind speed"

    assert (df['date_time'] <= pd.Timestamp.utcnow()).all(), "future timestamp"


def validate_fact_weather_daily(df):
    assert df['location_id'].notnull().all(), "location_id nulls"
    assert df['date'].notnull().all(), "date nulls"

    assert not df.duplicated(
        subset=['location_id', 'date']
    ).any(), "duplicate (location_id, date)"

    assert df['precipitation'].ge(0).all(), "bad precipitation"
    assert df['rain'].ge(0).all(), "bad rain"
    assert df['snowfall'].ge(0).all(), "bad snowfall"
    assert df['wind_speed'].ge(0).all(), "bad wind speed"





