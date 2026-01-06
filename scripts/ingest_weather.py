import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("OPENWEATHER_API_KEY not found in environment")

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

params = {
    "q": "Washington DC,US",
    "appid": API_KEY,
    "units": "metric"
}

response = requests.get(BASE_URL, params=params, timeout=10)

if response.status_code != 200:
    raise Exception(
        f"API call failed: {response.status_code} - {response.text}"
    )

data = response.json()

today = datetime.utcnow().strftime("%Y-%m-%d")
output_dir = f"data/raw/openweather/{today}"
os.makedirs(output_dir, exist_ok=True)

output_path = f"{output_dir}/washingtondc.json"

with open(output_path, "w") as f:
    json.dump(data, f, indent=2)

print(f"Raw weather data saved to {output_path}")