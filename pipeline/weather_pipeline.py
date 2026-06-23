"""
Pipeline orchestration module.

Defines the WeatherPipeline class, which ties together extraction,
transformation, validation, and loading into two reusable workflows:

    - run_etl(): classic Extract -> Transform -> Load
    - run_elt(): Extract -> Load (raw to staging) -> Transform -> Load (final)

This is the class used directly by scripts and by the Airflow DAG.
"""

from typing import Optional

import pandas as pd

from extract.extractor import WeatherExtractor
from transform.transformer import WeatherTransformer
from validate.validator import DataValidator
from load.loader import WeatherLoader
from utils.exceptions import PipelineError
from utils.logger import get_logger

logger = get_logger(__name__)


class WeatherPipeline:
    """
    Reusable, class-based weather data pipeline.

    Each stage (extract, transform, validate, load) is delegated to a
    dedicated component, so the pipeline class itself is mainly
    responsible for orchestration, error handling, and logging.
    """

    def __init__(
        self,
        extractor: Optional[WeatherExtractor] = None,
        transformer: Optional[WeatherTransformer] = None,
        validator: Optional[DataValidator] = None,
        loader: Optional[WeatherLoader] = None,
    ):
        self.extractor = extractor or WeatherExtractor()
        self.transformer = transformer or WeatherTransformer()
        self.validator = validator or DataValidator()
        self.loader = loader or WeatherLoader()

    # ------------------------------------------------------------------
    # ETL workflow (Part A)
    # ------------------------------------------------------------------
    def run_etl(self) -> pd.DataFrame:
        """
        Run the full Extract -> Transform -> Load workflow.

        Returns:
            The final transformed DataFrame that was loaded.

        Raises:
            PipelineError: if any stage fails irrecoverably.
        """
        logger.info("===== Starting ETL run =====")
        try:
            raw_extracts = self.extractor.extract_all()
            transformed_df = self.transformer.transform(raw_extracts)
            self.validator.validate(transformed_df)

            with self.loader as loader:
                loader.create_tables()
                loader.load_warehouse(transformed_df)

            logger.info("===== ETL run completed successfully =====")
            return transformed_df

        except PipelineError as exc:
            logger.error("ETL run failed: %s", exc)
            raise
        except Exception as exc:
            logger.error("Unexpected error during ETL run: %s", exc)
            raise PipelineError(f"ETL run failed unexpectedly: {exc}") from exc

    # ------------------------------------------------------------------
    # ELT workflow (Part B)
    # ------------------------------------------------------------------
    def run_elt(self) -> pd.DataFrame:
        """
        Run the Extract -> Load (staging) -> Transform -> Load (final) workflow.

        1. Extract raw data from the API.
        2. Load near-raw data into the `staging_weather_raw` table.
        3. Transform the data (in-pipeline, simulating an in-warehouse
           transform step).
        4. Validate and load the transformed data into the star schema.

        Returns:
            The final transformed DataFrame that was loaded.

        Raises:
            PipelineError: if any stage fails irrecoverably.
        """
        logger.info("===== Starting ELT run =====")
        try:
            raw_extracts = self.extractor.extract_all()

            # Flatten before staging so staging table has a simple tabular shape
            flat_df = self.transformer.flatten_raw_extracts(raw_extracts)
            flat_df = self.transformer.clean_column_names(flat_df)
            flat_df = self.transformer.standardize_location_names(flat_df)
            flat_df = self.transformer.convert_datetime_fields(flat_df)
            flat_df = self.transformer.convert_numeric_fields(flat_df)

            with self.loader as loader:
                loader.create_tables()
                loader.load_staging(flat_df)

            # "Transform" step of ELT: apply remaining cleaning/derivation
            transformed_df = self.transformer.transform(raw_extracts)
            self.validator.validate(transformed_df)

            with self.loader as loader:
                loader.load_warehouse(transformed_df)

            logger.info("===== ELT run completed successfully =====")
            return transformed_df

        except PipelineError as exc:
            logger.error("ELT run failed: %s", exc)
            raise
        except Exception as exc:
            logger.error("Unexpected error during ELT run: %s", exc)
            raise PipelineError(f"ELT run failed unexpectedly: {exc}") from exc


if __name__ == "__main__":
    pipeline = WeatherPipeline()
    pipeline.run_etl()
