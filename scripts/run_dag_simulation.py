"""
Run the full Airflow DAG task chain (extract -> transform -> validate -> load)
using realistic sample data, for environments without outbound API access.

This mirrors exactly what dags/weather_etl_dag.py does, task by task, using
the same WeatherTransformer / DataValidator / WeatherLoader classes -
only the extraction step is replaced with locally-generated sample data
(since this sandbox cannot reach api.open-meteo.com).
"""

import os
import sys

# Ensure the project root (parent of this scripts/ directory) is importable,
# regardless of the current working directory the script is run from.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from scripts.generate_sample_run import generate_sample_extracts
from transform.transformer import WeatherTransformer
from validate.validator import DataValidator
from load.loader import WeatherLoader
from utils.logger import get_logger

logger = get_logger("dag_demo_run")


def main():
    print("=" * 70)
    print("AIRFLOW DAG SIMULATION: weather_etl_pipeline")
    print("=" * 70)

    # ---- Task 1: extract_weather_data ----
    print("\n[TASK 1/4] extract_weather_data ... ", end="")
    raw_extracts = generate_sample_extracts()
    print(f"SUCCESS ({len(raw_extracts)} locations extracted)")

    # ---- Task 2: transform_weather_data ----
    print("[TASK 2/4] transform_weather_data ... ", end="")
    transformer = WeatherTransformer()
    df = transformer.transform(raw_extracts)
    print(f"SUCCESS ({len(df)} rows produced)")

    # ---- Task 3: validate_weather_data ----
    print("[TASK 3/4] validate_weather_data ... ", end="")
    validator = DataValidator()
    validator.validate(df)
    print("SUCCESS (all data quality checks passed)")

    # ---- Task 4: load_weather_data ----
    print("[TASK 4/4] load_weather_data ... ", end="")
    with WeatherLoader() as loader:
        loader.create_tables()
        loader.load_warehouse(df)
    print("SUCCESS (loaded into star schema)")

    print("\n" + "=" * 70)
    print("DAG RUN COMPLETE - all 4 tasks succeeded")
    print("=" * 70)


if __name__ == "__main__":
    main()
