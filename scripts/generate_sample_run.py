"""
Generate a sample raw-extract payload (mimicking WeatherExtractor.extract_all
output) and run it through the full ETL pipeline. Useful for demos and
environments without outbound API access.
"""

import os
import random
import sys
from datetime import datetime, timedelta, timezone

# Ensure the project root (parent of this scripts/ directory) is importable,
# regardless of the current working directory the script is run from.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from load.loader import WeatherLoader
from transform.transformer import WeatherTransformer
from utils.config import LOCATIONS, FORECAST_DAYS
from validate.validator import DataValidator


def generate_sample_extracts():
    """Build a realistic raw-extract structure for all configured locations."""
    random.seed(42)
    extracts = []
    start = datetime(2026, 6, 13, 0, 0)
    hours = FORECAST_DAYS * 24

    for loc in LOCATIONS:
        times, temps, hums, precs, winds, codes, pressures = [], [], [], [], [], [], []
        for h in range(hours):
            t = start + timedelta(hours=h)
            times.append(t.strftime("%Y-%m-%dT%H:%M"))
            temps.append(round(random.uniform(18, 33), 1))
            hums.append(round(random.uniform(40, 95), 1))
            precs.append(round(random.choice([0, 0, 0, 0.2, 1.5, 5.0]), 1))
            winds.append(round(random.uniform(2, 25), 1))
            codes.append(random.choice([0, 1, 2, 3, 61, 80, 95]))
            pressures.append(round(random.uniform(1005, 1020), 1))

        extracts.append({
            "city": loc["city"],
            "country": loc["country"],
            "latitude": loc["latitude"],
            "longitude": loc["longitude"],
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "raw_response": {
                "hourly": {
                    "time": times,
                    "temperature_2m": temps,
                    "relative_humidity_2m": hums,
                    "precipitation": precs,
                    "wind_speed_10m": winds,
                    "weather_code": codes,
                    "surface_pressure": pressures,
                },
                "daily": {},
            },
        })
    return extracts


def main():
    raw_extracts = generate_sample_extracts()

    transformer = WeatherTransformer()
    df = transformer.transform(raw_extracts)

    validator = DataValidator()
    validator.validate(df)

    with WeatherLoader() as loader:
        loader.create_tables()
        loader.load_warehouse(df)

    print(f"Loaded {len(df)} rows into the warehouse.")
    print(df.head())


if __name__ == "__main__":
    main()
