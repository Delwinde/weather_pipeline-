"""Unit tests for extract.extractor.WeatherExtractor (API calls mocked)."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from extract.extractor import WeatherExtractor
from utils.exceptions import APIConnectionError, APIResponseError


SAMPLE_LOCATION = {
    "city": "Kumasi",
    "country": "Ghana",
    "latitude": 6.6885,
    "longitude": -1.6244,
}

SAMPLE_PAYLOAD = {
    "hourly": {
        "time": ["2026-06-13T00:00"],
        "temperature_2m": [24.5],
        "relative_humidity_2m": [88],
        "precipitation": [0.0],
        "wind_speed_10m": [5.4],
        "weather_code": [3],
        "surface_pressure": [1012.3],
    },
    "daily": {
        "time": ["2026-06-13"],
        "temperature_2m_max": [28.0],
        "temperature_2m_min": [22.0],
        "precipitation_sum": [0.0],
        "wind_speed_10m_max": [10.0],
    },
}


@pytest.fixture
def extractor():
    return WeatherExtractor(locations=[SAMPLE_LOCATION], max_retries=2, retry_backoff=0)


class TestExtractLocation:
    @patch("extract.extractor.requests.get")
    def test_successful_extraction(self, mock_get, extractor):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = SAMPLE_PAYLOAD
        mock_get.return_value = mock_response

        result = extractor.extract_location(SAMPLE_LOCATION)

        assert result["city"] == "Kumasi"
        assert result["raw_response"] == SAMPLE_PAYLOAD
        assert "extracted_at" in result

    @patch("extract.extractor.requests.get")
    def test_non_200_status_raises_api_response_error(self, mock_get, extractor):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_get.return_value = mock_response

        with pytest.raises(APIResponseError):
            extractor.extract_location(SAMPLE_LOCATION)

    @patch("extract.extractor.requests.get")
    def test_missing_keys_raises_api_response_error(self, mock_get, extractor):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"unexpected": "payload"}
        mock_get.return_value = mock_response

        with pytest.raises(APIResponseError):
            extractor.extract_location(SAMPLE_LOCATION)

    @patch("extract.extractor.requests.get")
    def test_connection_error_raises_after_retries(self, mock_get, extractor):
        mock_get.side_effect = requests.exceptions.ConnectionError("boom")

        with pytest.raises(APIConnectionError):
            extractor.extract_location(SAMPLE_LOCATION)

        assert mock_get.call_count == extractor.max_retries


class TestExtractAll:
    @patch("extract.extractor.requests.get")
    def test_extract_all_success(self, mock_get, extractor):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = SAMPLE_PAYLOAD
        mock_get.return_value = mock_response

        results = extractor.extract_all()

        assert len(results) == 1
        assert results[0]["city"] == "Kumasi"

    @patch("extract.extractor.requests.get")
    def test_extract_all_raises_when_all_locations_fail(self, mock_get, extractor):
        mock_get.side_effect = requests.exceptions.ConnectionError("boom")

        with pytest.raises(APIConnectionError):
            extractor.extract_all()
