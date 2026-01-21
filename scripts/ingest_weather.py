import os
import json
import requests
from datetime import datetime
from configs.cities import CITIES
from dotenv import load_dotenv

load_dotenv()

def ingest_weather():
    API_KEY = os.getenv("OPENWEATHER_API_KEY")
    if not API_KEY:
        raise ValueError("OPENWEATHER_API_KEY not found in environment")

    BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
    RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")

    for city_cfg in CITIES:
        city = city_cfg["city"]
        country = city_cfg["country"]

        print(f"Ingesting current weather for {city}")

        params = {
            "q": f"{city},{country}",
            "appid": API_KEY,
            "units": "metric"
        }

        response = requests.get(BASE_URL, params=params, timeout=10)

        if response.status_code != 200:
            raise Exception(
                f"API call failed for {city}: {response.status_code} - {response.text}"
            )

        data = response.json()


        output_dir = "/opt/airflow/data/raw"
        os.makedirs(output_dir, exist_ok=True)

        # output_dir = f"data/raw/openweather/{RUN_DATE}"
        # os.makedirs(output_dir, exist_ok=True)

        city_slug = city.lower().replace(" ", "").replace(".", "")
        output_path = f"{output_dir}/{city_slug}.json"

        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)
        
        print(output_path)
        
    print("Raw weather ingestion completed")
    
if __name__ == "__main__":
    ingest_weather()
