"""Unit tests for validate.validator.DataValidator."""

import pandas as pd
import pytest

from validate.validator import DataValidator
from utils.exceptions import DataValidationError


def _valid_df():
    return pd.DataFrame(
        {
            "city": ["Kumasi", "Accra"],
            "country": ["Ghana", "Ghana"],
            "latitude": [6.6885, 5.6037],
            "longitude": [-1.6244, -0.1870],
            "observation_time": pd.to_datetime(
                ["2026-06-13T00:00", "2026-06-13T01:00"]
            ),
            "temperature_2m": [24.5, 28.0],
            "relative_humidity_2m": [88.0, 70.0],
            "precipitation": [0.0, 0.2],
            "wind_speed_10m": [5.4, 10.0],
            "weather_code": [3, 0],
            "surface_pressure": [1012.3, 1010.0],
        }
    )


class TestDataValidator:
    def setup_method(self):
        self.validator = DataValidator()

    def test_valid_dataframe_passes(self):
        assert self.validator.validate(_valid_df()) is True

    def test_empty_dataframe_fails(self):
        df = _valid_df().iloc[0:0]
        with pytest.raises(DataValidationError):
            self.validator.validate(df)

    def test_missing_required_column_fails(self):
        df = _valid_df().drop(columns=["temperature_2m"])
        with pytest.raises(DataValidationError):
            self.validator.validate(df)

    def test_null_in_key_column_fails(self):
        df = _valid_df()
        df.loc[0, "city"] = None
        with pytest.raises(DataValidationError):
            self.validator.validate(df)

    def test_out_of_range_value_fails(self):
        df = _valid_df()
        df.loc[0, "temperature_2m"] = 999.0
        with pytest.raises(DataValidationError):
            self.validator.validate(df)

    def test_duplicate_records_fail(self):
        df = _valid_df()
        df.loc[1, "city"] = df.loc[0, "city"]
        df.loc[1, "observation_time"] = df.loc[0, "observation_time"]
        with pytest.raises(DataValidationError):
            self.validator.validate(df)
