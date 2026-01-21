def copy_into_raw(conn):
    cursor = conn.cursor()

    mappings = {
        "fact_weather_current.csv": "FACT_WEATHER_CURRENT",
        "fact_weather_daily.csv": "FACT_WEATHER_DAILY",
        "dim_location.csv": "DIM_LOCATION",
    }

    for file, table in mappings.items():
        cursor.execute(f"""
            COPY INTO WEATHER_DB.RAW.{table}
            FROM @WEATHER_DB.RAW.WEATHER_STAGE/{file}
            FILE_FORMAT = (FORMAT_NAME = WEATHER_DB.RAW.CSV_FORMAT)
            ON_ERROR = 'FAIL';
        """)

    cursor.close()
