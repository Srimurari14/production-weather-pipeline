from pathlib import Path

def upload_to_stage(conn, local_dir):
    cursor = conn.cursor()

    files = list(Path(local_dir).glob("*.csv"))
    if not files:
        raise ValueError(f"No CSV files found in {local_dir}")

    for file in files:
        cursor.execute(f"""
            PUT file://{file.resolve()}
            @WEATHER_DB.RAW.WEATHER_STAGE
            OVERWRITE = TRUE
            AUTO_COMPRESS = FALSE;
        """)
    
        print(f'{file} Upload To Stage Successful')

    cursor.close()
