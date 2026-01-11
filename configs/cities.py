from datetime import datetime

CITIES = [
    {
        "city": "Washington D.C.",
        "country": "US",
        "location_id": "LOC_001",
        "historical_file": "data/raw/historical/washingtondc_2020_2025_daily.csv",
        "current_file": f"data/raw/openweather/{datetime.utcnow().strftime("%Y-%m-%d")}/washingtondc.json"
    }
]
