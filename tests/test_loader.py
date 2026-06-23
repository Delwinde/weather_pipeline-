"""Unit tests for load.loader.WeatherLoader (uses a temporary SQLite DB)."""

import os
import tempfile
from datetime import datetime, timezone

import pandas as pd
import pytest

from load.loader import WeatherLoader
from utils.exceptions import DatabaseConnectionError


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "city": ["Kumasi", "Accra"],
            "country": ["Ghana", "Ghana"],
            "latitude": [6.6885, 5.6037],
            "longitude": [-1.6244, -0.1870],
            "observation_time": pd.to_datetime(
                ["2026-06-13T00:00", "2026-06-13T01:00"]
            ),
            "extracted_at": pd.to_datetime(
                [datetime.now(timezone.utc), datetime.now(timezone.utc)], utc=True
            ),
            "temperature_2m": [24.5, 28.0],
            "temperature_fahrenheit": [76.1, 82.4],
            "relative_humidity_2m": [88.0, 70.0],
            "precipitation": [0.0, 0.2],
            "is_raining": [False, True],
            "wind_speed_10m": [5.4, 10.0],
            "weather_code": [3, 0],
            "surface_pressure": [1012.3, 1010.0],
            "date": [
                pd.Timestamp("2026-06-13").date(),
                pd.Timestamp("2026-06-13").date(),
            ],
            "hour": [0, 1],
            "weather_category": ["Cloudy", "Clear"],
        }
    )


@pytest.fixture
def temp_db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    os.remove(path)


class TestWeatherLoaderConnection:
    def test_create_tables_requires_connection(self, temp_db_path):
        loader = WeatherLoader(db_path=temp_db_path)
        with pytest.raises(DatabaseConnectionError):
            loader.create_tables()

    def test_connect_and_close(self, temp_db_path):
        loader = WeatherLoader(db_path=temp_db_path)
        loader.connect()
        assert loader.conn is not None
        loader.close()
        assert loader.conn is None


class TestWeatherLoaderEndToEnd:
    def test_full_load_creates_expected_rows(self, temp_db_path, sample_df):
        with WeatherLoader(db_path=temp_db_path) as loader:
            loader.create_tables()
            loader.load_warehouse(sample_df)

            cur = loader.conn.cursor()

            n_locations = cur.execute("SELECT COUNT(*) FROM dim_location").fetchone()[0]
            n_dates = cur.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
            n_conditions = cur.execute(
                "SELECT COUNT(*) FROM dim_weather_condition"
            ).fetchone()[0]
            n_facts = cur.execute(
                "SELECT COUNT(*) FROM fact_weather_observations"
            ).fetchone()[0]

            assert n_locations == 2
            assert n_dates == 1
            assert n_conditions == 2
            assert n_facts == 2

    def test_load_is_idempotent(self, temp_db_path, sample_df):
        """Loading the same data twice should not create duplicate fact rows."""
        with WeatherLoader(db_path=temp_db_path) as loader:
            loader.create_tables()
            loader.load_warehouse(sample_df)
            loader.load_warehouse(sample_df)

            n_facts = loader.conn.execute(
                "SELECT COUNT(*) FROM fact_weather_observations"
            ).fetchone()[0]
            assert n_facts == 2

    def test_load_staging(self, temp_db_path, sample_df):
        with WeatherLoader(db_path=temp_db_path) as loader:
            loader.create_tables()
            loader.load_staging(sample_df)

            n_staging = loader.conn.execute(
                "SELECT COUNT(*) FROM staging_weather_raw"
            ).fetchone()[0]
            assert n_staging == 2
