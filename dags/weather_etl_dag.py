"""
Airflow DAG: weather_etl_pipeline

Automates the Weather ETL pipeline to run daily. Each pipeline stage
(extract, transform, validate, load) is wrapped in its own Airflow
task so that failures, retries, and logs can be inspected per stage
in the Airflow UI.

Data is passed between tasks via XCom as JSON-serialized records,
which keeps task functions decoupled from any single in-memory
DataFrame instance (important since each task may run in a separate
worker process).
"""

from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from extract.extractor import WeatherExtractor
from transform.transformer import WeatherTransformer
from validate.validator import DataValidator
from load.loader import WeatherLoader
from utils.logger import get_logger

logger = get_logger(__name__)


default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def extract_task(**context) -> None:
    """Extract raw weather data from the Open-Meteo API and push to XCom."""
    extractor = WeatherExtractor()
    raw_extracts = extractor.extract_all()
    context["ti"].xcom_push(key="raw_extracts", value=raw_extracts)
    logger.info("extract_task: pushed %s location extracts to XCom", len(raw_extracts))


def transform_task(**context) -> None:
    """Pull raw extracts from XCom, transform them, and push the result."""
    raw_extracts = context["ti"].xcom_pull(key="raw_extracts", task_ids="extract_weather_data")

    transformer = WeatherTransformer()
    df = transformer.transform(raw_extracts)

    # JSON-serialize for XCom; convert datetime/date columns to strings
    df_serializable = df.copy()
    df_serializable["observation_time"] = df_serializable["observation_time"].astype(str)
    df_serializable["extracted_at"] = df_serializable["extracted_at"].astype(str)
    df_serializable["date"] = df_serializable["date"].astype(str)

    context["ti"].xcom_push(key="transformed_records", value=df_serializable.to_dict(orient="records"))
    logger.info("transform_task: produced %s transformed rows", len(df_serializable))


def validate_task(**context) -> None:
    """Pull transformed records from XCom and run data quality checks."""
    records = context["ti"].xcom_pull(key="transformed_records", task_ids="transform_weather_data")
    df = pd.DataFrame(records)
    df["observation_time"] = pd.to_datetime(df["observation_time"])

    validator = DataValidator()
    validator.validate(df)
    logger.info("validate_task: validation passed for %s rows", len(df))


def load_task(**context) -> None:
    """Pull transformed records from XCom and load into the star schema."""
    records = context["ti"].xcom_pull(key="transformed_records", task_ids="transform_weather_data")
    df = pd.DataFrame(records)
    df["observation_time"] = pd.to_datetime(df["observation_time"])
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)

    with WeatherLoader() as loader:
        loader.create_tables()
        loader.load_warehouse(df)

    logger.info("load_task: loaded %s rows into the warehouse", len(df))


with DAG(
    dag_id="weather_etl_pipeline",
    description="Daily ETL pipeline for weather analytics (Open-Meteo API)",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["weather", "etl", "capstone"],
) as dag:

    extract = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform_task,
    )

    validate = PythonOperator(
        task_id="validate_weather_data",
        python_callable=validate_task,
    )

    load = PythonOperator(
        task_id="load_weather_data",
        python_callable=load_task,
    )

    # Task dependencies: linear pipeline
    # extract -> transform -> validate -> load
    extract >> transform >> validate >> load
