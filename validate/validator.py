"""
Validation module.

Provides data quality checks that run after transformation and before
loading into the database. Centralizing validation here keeps quality
rules consistent across both the ETL and ELT workflows.
"""

from typing import List

import pandas as pd

from utils.exceptions import DataValidationError
from utils.logger import get_logger

logger = get_logger(__name__)

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


class DataValidator:
    """Runs a suite of data quality checks on a transformed DataFrame."""

    def __init__(self, required_columns: List[str] = None):
        self.required_columns = required_columns or REQUIRED_COLUMNS

    def check_not_empty(self, df: pd.DataFrame) -> None:
        """Ensure the DataFrame contains at least one row."""
        if df.empty:
            raise DataValidationError("Validation failed: DataFrame is empty.")

    def check_required_columns(self, df: pd.DataFrame) -> None:
        """Ensure all required columns are present."""
        missing = [c for c in self.required_columns if c not in df.columns]
        if missing:
            raise DataValidationError(
                f"Validation failed: missing required columns {missing}"
            )

    def check_no_nulls_in_keys(self, df: pd.DataFrame) -> None:
        """Ensure key columns used for joins/keys contain no nulls."""
        key_cols = ["city", "country", "observation_time"]
        for col in key_cols:
            if df[col].isna().any():
                raise DataValidationError(
                    f"Validation failed: column '{col}' contains null values."
                )

    def check_value_ranges(self, df: pd.DataFrame) -> None:
        """Spot-check that core measurements are within plausible ranges."""
        checks = {
            "temperature_2m": (-90, 60),
            "relative_humidity_2m": (0, 100),
            "wind_speed_10m": (0, 150),
        }
        for col, (low, high) in checks.items():
            if col not in df.columns:
                continue
            out_of_range = ~df[col].between(low, high)
            if out_of_range.any():
                raise DataValidationError(
                    f"Validation failed: column '{col}' has "
                    f"{int(out_of_range.sum())} values outside [{low}, {high}]."
                )

    def check_no_duplicate_records(self, df: pd.DataFrame) -> None:
        """Ensure no duplicate (city, observation_time) combinations remain."""
        duplicates = df.duplicated(subset=["city", "observation_time"]).sum()
        if duplicates:
            raise DataValidationError(
                f"Validation failed: {duplicates} duplicate "
                "(city, observation_time) records found."
            )

    def validate(self, df: pd.DataFrame) -> bool:
        """
        Run all validation checks on the given DataFrame.

        Returns:
            True if all checks pass.

        Raises:
            DataValidationError: on the first failed check.
        """
        logger.info("Running data validation on %s rows", len(df))

        self.check_not_empty(df)
        self.check_required_columns(df)
        self.check_no_nulls_in_keys(df)
        self.check_value_ranges(df)
        self.check_no_duplicate_records(df)

        logger.info("Data validation passed.")
        return True
