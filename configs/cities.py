from datetime import datetime

RUN_DATE = datetime.utcnow().strftime('%Y-%m-%d')

CITIES = [
    {
        "city": "Washington D.C.",
        "country": "US",
        "location_id": "LOC_001",
        "historical_file": "data/raw/historical/washingtondc_2020_2025_daily.csv",
        "current_file": f"data/raw/openweather/{RUN_DATE}/washingtondc.json"
    },
    {
        "city": "New Delhi",
        "country": "IN",
        "location_id": "LOC_002",
        "historical_file": "data/raw/historical/newdelhi_2020_2025_daily.csv",
        "current_file": f"data/raw/openweather/{RUN_DATE}/newdelhi.json"
    },
    {
        "city": "Tokyo",
        "country": "JP",
        "location_id": "LOC_003",
        "historical_file": "data/raw/historical/tokyo_2020_2025_daily.csv",
        "current_file": f"data/raw/openweather/{RUN_DATE}/tokyo.json"
    },
    {
        "city": "London",
        "country": "GB",
        "location_id": "LOC_004",
        "historical_file": "data/raw/historical/london_2020_2025_daily.csv",
        "current_file": f"data/raw/openweather/{RUN_DATE}/london.json"
    },
    {
        "city": "Sydney",
        "country": "AU",
        "location_id": "LOC_005",
        "historical_file": "data/raw/historical/sydney_2020_2025_daily.csv",
        "current_file": f"data/raw/openweather/{RUN_DATE}/sydney.json"
    },
    {
        "city": "Rio de Janeiro",
        "country": "BR",
        "location_id": "LOC_006",
        "historical_file": "data/raw/historical/rio_2020_2025_daily.csv",
        "current_file": f"data/raw/openweather/{RUN_DATE}/riodejaneiro.json"
    },
    {
        "city": "Cape Town",
        "country": "ZA",
        "location_id": "LOC_007",
        "historical_file": "data/raw/historical/capetown_2020_2025_daily.csv",
        "current_file": f"data/raw/openweather/{RUN_DATE}/capetown.json"
    },
]
