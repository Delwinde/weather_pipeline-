"""Unit tests for transform.transformer.WeatherTransformer."""

from datetime import datetime, timezone

import pandas as pd
import pytest

from transform.transformer import WeatherTransformer
from utils.exceptions import TransformationError


@pytest.fixture
def sample_raw_extracts():
    """A minimal raw extraction payload for two locations with overlap/dupes."""
    return [
        {
            "city": "kumasi",
            "country": "ghana",
            "latitude": 6.6885,
            "longitude": -1.6244,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "raw_response": {
                "hourly": {
                    "time": [
                        "2026-06-13T00:00",
                        "2026-06-13T01:00",
                        "2026-06-13T01:00",  # duplicate
                    ],
                    "temperature_2m": [24.5, 24.1, 24.1],
                    "relative_humidity_2m": [88, None, None],
                    "precipitation": [0.0, 0.2, 0.2],
                    "wind_speed_10m": [5.4, 6.1, 6.1],
                    "weather_code": [3, 61, 61],
                    "surface_pressure": [1012.3, 1012.1, 1012.1],
                }
            },
        },
        {
            "city": "  accra ",
            "country": "ghana",
            "latitude": 5.6037,
            "longitude": -0.1870,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "raw_response": {
                "hourly": {
                    "time": ["2026-06-13T00:00"],
                    "temperature_2m": [999],  # out-of-range, should be cleaned
                    "relative_humidity_2m": [70],
                    "precipitation": [0.0],
                    "wind_speed_10m": [10.0],
                    "weather_code": [0],
                    "surface_pressure": [1010.0],
                }
            },
        },
    ]


class TestCleanColumnNames:
    def test_lowercases_and_strips(self):
        df = pd.DataFrame({" City Name ": [1], "Wind-Speed": [2]})
        result = WeatherTransformer.clean_column_names(df)
        assert "city_name" in result.columns
        assert "wind_speed" in result.columns


class TestStandardizeLocationNames:
    def test_title_cases_and_strips(self):
        df = pd.DataFrame({"city": ["  accra "], "country": ["ghana"]})
        result = WeatherTransformer.standardize_location_names(df)
        assert result["city"].iloc[0] == "Accra"
        assert result["country"].iloc[0] == "Ghana"


class TestConvertDatetimeFields:
    def test_converts_observation_time(self):
        df = pd.DataFrame({"observation_time": ["2026-06-13T00:00"]})
        result = WeatherTransformer.convert_datetime_fields(df)
        assert pd.api.types.is_datetime64_any_dtype(result["observation_time"])


class TestConvertNumericFields:
    def test_converts_strings_to_numeric(self):
        df = pd.DataFrame({"temperature_2m": ["24.5"], "wind_speed_10m": ["6.1"]})
        result = WeatherTransformer.convert_numeric_fields(df)
        assert pd.api.types.is_numeric_dtype(result["temperature_2m"])
        assert pd.api.types.is_numeric_dtype(result["wind_speed_10m"])


class TestHandleMissingValues:
    def test_drops_rows_missing_core_fields(self):
        df = pd.DataFrame(
            {
                "city": ["Kumasi", None],
                "observation_time": pd.to_datetime(["2026-06-13", "2026-06-13"]),
                "temperature_2m": [24.0, 25.0],
                "relative_humidity_2m": [80.0, 80.0],
                "precipitation": [0.0, 0.0],
                "wind_speed_10m": [5.0, 5.0],
                "surface_pressure": [1010.0, 1010.0],
            }
        )
        result = WeatherTransformer.handle_missing_values(df)
        assert len(result) == 1

    def test_fills_missing_humidity_with_median(self):
        df = pd.DataFrame(
            {
                "city": ["Kumasi", "Kumasi", "Kumasi"],
                "observation_time": pd.to_datetime(["2026-06-13"] * 3),
                "temperature_2m": [24.0, 25.0, 26.0],
                "relative_humidity_2m": [80.0, None, 90.0],
                "precipitation": [0.0, 0.0, 0.0],
                "wind_speed_10m": [5.0, 5.0, 5.0],
                "surface_pressure": [1010.0, 1010.0, 1010.0],
            }
        )
        result = WeatherTransformer.handle_missing_values(df)
        assert not result["relative_humidity_2m"].isna().any()


class TestRemoveDuplicates:
    def test_removes_duplicate_city_time_rows(self):
        df = pd.DataFrame(
            {
                "city": ["Kumasi", "Kumasi"],
                "observation_time": pd.to_datetime(["2026-06-13", "2026-06-13"]),
                "temperature_2m": [24.0, 24.0],
            }
        )
        result = WeatherTransformer.remove_duplicates(df)
        assert len(result) == 1


class TestValidateMeasurements:
    def test_flags_out_of_range_temperature(self):
        df = pd.DataFrame(
            {
                "city": ["Accra", "Accra"],
                "observation_time": pd.to_datetime(["2026-06-13T00:00", "2026-06-13T01:00"]),
                "temperature_2m": [999.0, 30.0],
                "relative_humidity_2m": [70.0, 70.0],
                "precipitation": [0.0, 0.0],
                "wind_speed_10m": [10.0, 10.0],
                "surface_pressure": [1010.0, 1010.0],
            }
        )
        result = WeatherTransformer.validate_measurements(df)
        # Out-of-range value should be replaced (median of remaining valid values = 30.0)
        assert (result["temperature_2m"] <= 60).all()


class TestCreateDerivedFields:
    def test_creates_expected_columns(self):
        df = pd.DataFrame(
            {
                "observation_time": pd.to_datetime(["2026-06-13T05:00"]),
                "temperature_2m": [25.0],
                "precipitation": [1.5],
                "weather_code": [61],
            }
        )
        result = WeatherTransformer.create_derived_fields(df)
        assert "date" in result.columns
        assert "hour" in result.columns
        assert result["hour"].iloc[0] == 5
        assert result["temperature_fahrenheit"].iloc[0] == pytest.approx(77.0)
        assert bool(result["is_raining"].iloc[0]) is True
        assert result["weather_category"].iloc[0] == "Drizzle/Rain"

    def test_weather_category_mapping(self):
        codes = pd.Series([0, 2, 45, 61, 75, 82, 95, 9999])
        categories = codes.apply(WeatherTransformer._categorize_weather_code)
        expected = [
            "Clear",
            "Cloudy",
            "Fog",
            "Drizzle/Rain",
            "Snow",
            "Rain Showers",
            "Thunderstorm",
            "Other",
        ]
        assert categories.tolist() == expected


class TestFlattenRawExtracts:
    def test_flattens_into_long_format(self, sample_raw_extracts):
        df = WeatherTransformer.flatten_raw_extracts(sample_raw_extracts)
        # 3 rows from kumasi + 1 from accra = 4 rows
        assert len(df) == 4
        assert set(["city", "observation_time", "temperature_2m"]).issubset(df.columns)

    def test_raises_on_empty_input(self):
        with pytest.raises(TransformationError):
            WeatherTransformer.flatten_raw_extracts([])


class TestFullTransform:
    def test_transform_end_to_end(self, sample_raw_extracts):
        transformer = WeatherTransformer()
        result = transformer.transform(sample_raw_extracts)

        # Duplicate row removed: 4 raw rows -> 3 unique rows
        assert len(result) == 3

        # All required columns present
        for col in [
            "city",
            "country",
            "observation_time",
            "temperature_2m",
            "date",
            "hour",
            "weather_category",
            "temperature_fahrenheit",
            "is_raining",
        ]:
            assert col in result.columns

        # Location names standardized
        assert "Accra" in result["city"].values
        assert "Kumasi" in result["city"].values

        # Out-of-range temperature was cleaned (no value > 60)
        assert (result["temperature_2m"] <= 60).all()
