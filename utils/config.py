"""
Central configuration for the Weather ETL/ELT pipeline.
All paths, API settings, and DB settings are defined here so that
other modules can import a single source of truth.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
DATA_DIR = BASE_DIR / "data"
SQL_DIR = BASE_DIR / "sql"

LOG_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# API configuration (Open-Meteo)
# ---------------------------------------------------------------------------
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"

# Cities to extract weather data for: (name, country, latitude, longitude)
LOCATIONS = [
    {"city": "Kumasi", "country": "Ghana", "latitude": 6.6885, "longitude": -1.6244},
    {"city": "Accra", "country": "Ghana", "latitude": 5.6037, "longitude": -0.1870},
    {"city": "Lagos", "country": "Nigeria", "latitude": 6.5244, "longitude": 3.3792},
    {"city": "Nairobi", "country": "Kenya", "latitude": -1.2921, "longitude": 36.8219},
    {"city": "London", "country": "United Kingdom", "latitude": 51.5074, "longitude": -0.1278},
]

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
    "surface_pressure",
]

DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
]

FORECAST_DAYS = 3
TIMEZONE = "auto"

API_TIMEOUT_SECONDS = 30
API_MAX_RETRIES = 3
API_RETRY_BACKOFF_SECONDS = 5

# ---------------------------------------------------------------------------
# Database configuration
# ---------------------------------------------------------------------------
# Default: local SQLite database (zero-setup, ideal for grading/demo).
# Can be overridden with environment variables to point at Postgres etc.
DB_TYPE = os.getenv("WEATHER_DB_TYPE", "sqlite")  # "sqlite" or "postgres"

SQLITE_DB_PATH = str(DATA_DIR / "weather_warehouse.db")

POSTGRES_CONFIG = {
    "host": os.getenv("WEATHER_DB_HOST", "localhost"),
    "port": os.getenv("WEATHER_DB_PORT", "5432"),
    "dbname": os.getenv("WEATHER_DB_NAME", "weather_warehouse"),
    "user": os.getenv("WEATHER_DB_USER", "postgres"),
    "password": os.getenv("WEATHER_DB_PASSWORD", "postgres"),
}

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
LOG_FILE = str(LOG_DIR / "pipeline.log")
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
