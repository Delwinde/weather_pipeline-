"""
Transformation module.

Responsible for cleaning, reshaping, and enriching the raw weather
data extracted from the Open-Meteo API into a "long" tabular format
suitable for loading into fact and dimension tables.
"""

import re
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from utils.exceptions import TransformationError
from utils.logger import get_logger

logger = get_logger(__name__)

# Reasonable physical bounds used to flag implausible sensor readings.
VALID_RANGES = {
    "temperature_2m": (-90.0, 60.0),       # degrees C
    "relative_humidity_2m": (0.0, 100.0),  # percent
    "precipitation": (0.0, 500.0),         # mm
    "wind_speed_10m": (0.0, 150.0),        # km/h
    "surface_pressure": (800.0, 1100.0),   # hPa
}

REQUIRED_COLUMNS = [
    "city",
    "country",
    "latitude",
    "longitude",
    "observation_time",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
    "surface_pressure",
]


class WeatherTransformer:
    """
    Transforms raw extracted weather data into a clean, validated
    pandas DataFrame ready for loading into the star schema.
    """

    @staticmethod
    def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names: lowercase, strip whitespace,
        replace spaces/hyphens with underscores.
        """
        df = df.copy()
        df.columns = [
            re.sub(r"[\s\-]+", "_", col.strip().lower()) for col in df.columns
        ]
        return df

    @staticmethod
    def standardize_location_names(df: pd.DataFrame) -> pd.DataFrame:
        """Standardize city/country casing and trim whitespace."""
        df = df.copy()
        if "city" in df.columns:
            df["city"] = df["city"].astype(str).str.strip().str.title()
        if "country" in df.columns:
            df["country"] = df["country"].astype(str).str.strip().str.title()
        return df

    @staticmethod
    def convert_datetime_fields(df: pd.DataFrame) -> pd.DataFrame:
        """Convert the observation_time column to proper datetime."""
        df = df.copy()
        if "observation_time" in df.columns:
            df["observation_time"] = pd.to_datetime(
                df["observation_time"], errors="coerce"
            )
        if "extracted_at" in df.columns:
            df["extracted_at"] = pd.to_datetime(
                df["extracted_at"], errors="coerce", utc=True
            )
        return df

    @staticmethod
    def convert_numeric_fields(df: pd.DataFrame) -> pd.DataFrame:
        """Convert measurement columns to numeric dtypes."""
        df = df.copy()
        numeric_cols = [
            "latitude",
            "longitude",
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
            "weather_code",
            "surface_pressure",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    @staticmethod
    def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values in important fields.

        - Rows missing a valid observation_time, city, or temperature
          are dropped (these are unusable for analytics).
        - Other numeric fields are imputed with their column median
          (per-city), which preserves overall distribution better than
          a global fill for a multi-location dataset.
        """
        df = df.copy()

        before = len(df)
        df = df.dropna(subset=["observation_time", "city", "temperature_2m"])
        dropped = before - len(df)
        if dropped:
            logger.warning(
                "Dropped %s rows with missing observation_time/city/temperature",
                dropped,
            )

        fillable_cols = [
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
            "surface_pressure",
        ]
        for col in fillable_cols:
            if col in df.columns and df[col].isna().any():
                df[col] = df.groupby("city")[col].transform(
                    lambda s: s.fillna(s.median())
                )
                # Fallback for cases where an entire city group is NaN
                df[col] = df[col].fillna(df[col].median())

        # precipitation missing usually means "no rain" -> 0
        if "precipitation" in df.columns:
            df["precipitation"] = df["precipitation"].fillna(0.0)

        return df

    @staticmethod
    def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate rows based on city + observation_time."""
        df = df.copy()
        before = len(df)
        df = df.drop_duplicates(subset=["city", "observation_time"], keep="last")
        removed = before - len(df)
        if removed:
            logger.info("Removed %s duplicate rows", removed)
        return df

    @staticmethod
    def validate_measurements(df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate weather measurements against physically plausible ranges.

        Out-of-range values are set to NaN (and later dropped/handled),
        rather than silently kept, to avoid corrupting downstream
        analytics.
        """
        df = df.copy()
        for col, (low, high) in VALID_RANGES.items():
            if col not in df.columns:
                continue
            invalid_mask = ~df[col].between(low, high) & df[col].notna()
            n_invalid = int(invalid_mask.sum())
            if n_invalid:
                logger.warning(
                    "Found %s out-of-range values in '%s'; setting to NaN",
                    n_invalid,
                    col,
                )
                df.loc[invalid_mask, col] = pd.NA
                df[col] = pd.to_numeric(df[col], errors="coerce")

                # Impute newly-created NaNs in place (per-city median, with
                # global median fallback) instead of dropping the row -
                # an out-of-range sensor reading shouldn't discard an
                # otherwise valid observation.
                if df[col].isna().any():
                    df[col] = df.groupby("city")[col].transform(
                        lambda s: s.fillna(s.median())
                    )
                    df[col] = df[col].fillna(df[col].median())

        return df

    @staticmethod
    def create_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived/enriched fields useful for analytics:
            - date (calendar date of the observation)
            - hour (hour of day, 0-23)
            - temperature_fahrenheit
            - is_raining (bool flag)
            - weather_category (simple bucketed description from weather_code)
        """
        df = df.copy()
        df["date"] = df["observation_time"].dt.date
        df["hour"] = df["observation_time"].dt.hour
        df["temperature_fahrenheit"] = (df["temperature_2m"] * 9 / 5) + 32
        df["is_raining"] = df["precipitation"] > 0.0
        df["weather_category"] = df["weather_code"].apply(
            WeatherTransformer._categorize_weather_code
        )
        return df

    @staticmethod
    def _categorize_weather_code(code: Any) -> str:
        """Map WMO weather codes to a human-readable category."""
        try:
            code = int(code)
        except (ValueError, TypeError):
            return "Unknown"

        if code == 0:
            return "Clear"
        if code in (1, 2, 3):
            return "Cloudy"
        if code in (45, 48):
            return "Fog"
        if code in range(51, 68):
            return "Drizzle/Rain"
        if code in range(71, 78):
            return "Snow"
        if code in range(80, 87):
            return "Rain Showers"
        if code in range(95, 100):
            return "Thunderstorm"
        return "Other"

    @staticmethod
    def validate_required_columns(df: pd.DataFrame) -> None:
        """
        Ensure that all required columns are present.

        Raises:
            TransformationError: if any required column is missing.
        """
        missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            raise TransformationError(
                f"Missing required columns after transformation: {missing}"
            )

    @staticmethod
    def flatten_raw_extracts(raw_extracts: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Flatten the list of raw extraction dicts (one per location, each
        containing nested hourly arrays from the API) into a single
        "long" DataFrame: one row per (location, observation_time).

        Args:
            raw_extracts: output of WeatherExtractor.extract_all()

        Returns:
            A flat pandas DataFrame.
        """
        rows = []

        for extract in raw_extracts:
            city = extract["city"]
            country = extract["country"]
            latitude = extract["latitude"]
            longitude = extract["longitude"]
            extracted_at = extract["extracted_at"]
            hourly = extract["raw_response"].get("hourly", {})

            times = hourly.get("time", [])
            n = len(times)

            for i in range(n):
                row = {
                    "city": city,
                    "country": country,
                    "latitude": latitude,
                    "longitude": longitude,
                    "extracted_at": extracted_at,
                    "observation_time": times[i],
                }
                for var in (
                    "temperature_2m",
                    "relative_humidity_2m",
                    "precipitation",
                    "wind_speed_10m",
                    "weather_code",
                    "surface_pressure",
                ):
                    values = hourly.get(var, [])
                    row[var] = values[i] if i < len(values) else None

                rows.append(row)

        if not rows:
            raise TransformationError("No hourly records found in raw extracts.")

        return pd.DataFrame(rows)

    def transform(self, raw_extracts: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Run the full transformation pipeline on raw extracted data.

        Args:
            raw_extracts: output of WeatherExtractor.extract_all()

        Returns:
            A cleaned, validated, enriched pandas DataFrame ready for loading.

        Raises:
            TransformationError: if transformation fails at any stage.
        """
        try:
            logger.info("Starting transformation of %s raw extracts", len(raw_extracts))

            df = self.flatten_raw_extracts(raw_extracts)
            df = self.clean_column_names(df)
            df = self.standardize_location_names(df)
            df = self.convert_datetime_fields(df)
            df = self.convert_numeric_fields(df)
            df = self.handle_missing_values(df)
            df = self.remove_duplicates(df)
            df = self.validate_measurements(df)
            df = self.create_derived_fields(df)

            self.validate_required_columns(df)

            logger.info("Transformation complete: %s rows produced", len(df))
            return df

        except TransformationError:
            raise
        except Exception as exc:
            logger.error("Unexpected error during transformation: %s", exc)
            raise TransformationError(f"Transformation failed: {exc}") from exc


if __name__ == "__main__":
    # Simple smoke test with a tiny fake payload
    sample = [
        {
            "city": "kumasi",
            "country": "ghana",
            "latitude": 6.6885,
            "longitude": -1.6244,
            "extracted_at": datetime.utcnow().isoformat(),
            "raw_response": {
                "hourly": {
                    "time": ["2026-06-13T00:00", "2026-06-13T01:00"],
                    "temperature_2m": [24.5, 24.1],
                    "relative_humidity_2m": [88, 90],
                    "precipitation": [0.0, 0.2],
                    "wind_speed_10m": [5.4, 6.1],
                    "weather_code": [3, 61],
                    "surface_pressure": [1012.3, 1012.1],
                }
            },
        }
    ]
    transformer = WeatherTransformer()
    result_df = transformer.transform(sample)
    print(result_df)
