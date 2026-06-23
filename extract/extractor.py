"""
Extraction module.

Responsible for connecting to the Open-Meteo Weather API and
retrieving raw weather data (hourly + daily forecast) for a list
of configured locations.
"""

import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests

from utils.config import (
    API_MAX_RETRIES,
    API_RETRY_BACKOFF_SECONDS,
    API_TIMEOUT_SECONDS,
    DAILY_VARIABLES,
    HOURLY_VARIABLES,
    FORECAST_DAYS,
    LOCATIONS,
    OPEN_METEO_BASE_URL,
    TIMEZONE,
)
from utils.exceptions import APIConnectionError, APIResponseError
from utils.logger import get_logger

logger = get_logger(__name__)


class WeatherExtractor:
    """
    Handles extraction of weather data from the Open-Meteo API.

    The extractor is intentionally stateless between calls (no shared
    mutable state besides configuration), making it easy to reuse and
    unit test.
    """

    def __init__(
        self,
        base_url: str = OPEN_METEO_BASE_URL,
        locations: List[Dict[str, Any]] = None,
        timeout: int = API_TIMEOUT_SECONDS,
        max_retries: int = API_MAX_RETRIES,
        retry_backoff: int = API_RETRY_BACKOFF_SECONDS,
    ):
        self.base_url = base_url
        self.locations = locations if locations is not None else LOCATIONS
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff

    def _build_params(self, location: Dict[str, Any]) -> Dict[str, Any]:
        """Build the query parameters for a single location request."""
        return {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "hourly": ",".join(HOURLY_VARIABLES),
            "daily": ",".join(DAILY_VARIABLES),
            "forecast_days": FORECAST_DAYS,
            "timezone": TIMEZONE,
        }

    def _request_with_retries(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform a GET request against the Open-Meteo API with retry logic.

        Raises:
            APIConnectionError: if all retry attempts fail to connect.
            APIResponseError: if the API returns a non-200 status or
                a malformed/unexpected JSON body.
        """
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(
                    self.base_url, params=params, timeout=self.timeout
                )

                if response.status_code != 200:
                    raise APIResponseError(
                        f"Unexpected status code {response.status_code} "
                        f"from Open-Meteo API: {response.text[:200]}"
                    )

                payload = response.json()

                # Validate the response has the expected top-level keys
                if "hourly" not in payload or "daily" not in payload:
                    raise APIResponseError(
                        "API response missing expected 'hourly'/'daily' keys: "
                        f"{list(payload.keys())}"
                    )

                return payload

            except requests.exceptions.ConnectionError as exc:
                last_exception = exc
                logger.warning(
                    "Connection error on attempt %s/%s: %s",
                    attempt,
                    self.max_retries,
                    exc,
                )
            except requests.exceptions.Timeout as exc:
                last_exception = exc
                logger.warning(
                    "Timeout on attempt %s/%s: %s", attempt, self.max_retries, exc
                )
            except APIResponseError:
                # Don't retry on a bad response - it's a deterministic error
                raise

            if attempt < self.max_retries:
                time.sleep(self.retry_backoff)

        raise APIConnectionError(
            f"Failed to connect to Open-Meteo API after {self.max_retries} "
            f"attempts: {last_exception}"
        )

    def extract_location(self, location: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract raw weather data for a single location.

        Args:
            location: dict with keys city, country, latitude, longitude.

        Returns:
            A dict combining the API payload with location metadata and
            an extraction timestamp.
        """
        params = self._build_params(location)

        logger.info(
            "Extracting weather data for %s, %s (lat=%s, lon=%s)",
            location["city"],
            location["country"],
            location["latitude"],
            location["longitude"],
        )

        payload = self._request_with_retries(params)

        record = {
            "city": location["city"],
            "country": location["country"],
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "raw_response": payload,
        }

        logger.info("Successfully extracted data for %s", location["city"])
        return record

    def extract_all(self) -> List[Dict[str, Any]]:
        """
        Extract weather data for every configured location.

        Locations that fail extraction are logged and skipped so a
        single failing location does not abort the entire run.

        Returns:
            A list of extraction result dicts (one per successful location).
        """
        results = []
        for location in self.locations:
            try:
                result = self.extract_location(location)
                results.append(result)
            except (APIConnectionError, APIResponseError) as exc:
                logger.error(
                    "Failed to extract data for %s: %s", location["city"], exc
                )
                continue

        if not results:
            raise APIConnectionError(
                "Extraction failed for all configured locations."
            )

        logger.info(
            "Extraction complete: %s/%s locations successful",
            len(results),
            len(self.locations),
        )
        return results


if __name__ == "__main__":
    extractor = WeatherExtractor()
    data = extractor.extract_all()
    print(f"Extracted data for {len(data)} locations")
