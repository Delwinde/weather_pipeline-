# Weather Analytics ETL/ELT Pipeline

A production-style, maintainable data pipeline that extracts weather data
from the [Open-Meteo API](https://open-meteo.com/), transforms and validates
it, and loads it into a SQL data warehouse modeled as a **star schema**.
The pipeline supports both **ETL** and **ELT** workflows and is automated
daily via **Apache Airflow**.

Built for the AICA x DataCamp Data Engineering Capstone (2025/2026 Cohort 2).

---

## 1. Project Overview

A weather analytics company needs an automated pipeline that:

1. Extracts hourly weather forecasts for multiple cities from the Open-Meteo API.
2. Cleans, validates, and enriches the data.
3. Loads it into a SQL database using a star schema for reporting/BI.
4. Runs automatically every day via Airflow.

---

## 2. Project Structure

```
weather_pipeline/
├── dags/
│   └── weather_etl_dag.py        # Airflow DAG (daily schedule)
├── extract/
│   └── extractor.py              # WeatherExtractor: API extraction + retries
├── transform/
│   └── transformer.py            # WeatherTransformer: cleaning & enrichment
├── validate/
│   └── validator.py              # DataValidator: data quality checks
├── load/
│   └── loader.py                 # WeatherLoader: SQLite star schema loader
├── pipeline/
│   └── weather_pipeline.py       # WeatherPipeline class (run_etl / run_elt)
├── sql/
│   └── star_schema.sql           # DDL for staging, dim, and fact tables
├── utils/
│   ├── config.py                 # Central configuration
│   ├── logger.py                 # Logging setup
│   └── exceptions.py             # Custom exception hierarchy
├── tests/                        # pytest unit tests
├── logs/
│   └── pipeline.log              # Runtime log file
├── data/
│   └── weather_warehouse.db      # SQLite database (generated)
├── scripts_generate_sample_run.py  # Demo run with sample data (no API needed)
├── requirements.txt
└── README.md
```

---

## 3. Data Source

**Open-Meteo Weather API** (no API key required):
`https://api.open-meteo.com/v1/forecast`

For each configured location (city, country, latitude, longitude — see
`utils/config.py`), the pipeline retrieves:

- **Hourly variables**: `temperature_2m`, `relative_humidity_2m`,
  `precipitation`, `wind_speed_10m`, `weather_code`, `surface_pressure`
- **Daily variables**: `temperature_2m_max`, `temperature_2m_min`,
  `precipitation_sum`, `wind_speed_10m_max`
- Forecast window: 3 days

---

## 4. Star Schema Design

The warehouse uses a classic **star schema**: one central fact table
surrounded by denormalized dimension tables.

```
                  dim_location
                       │
                       │
  dim_date ─────  fact_weather_observations  ───── dim_weather_condition
```

### Fact Table: `fact_weather_observations`
One row per (location, observation_time). Contains all numeric measures:
`temperature_2m`, `temperature_fahrenheit`, `relative_humidity_2m`,
`precipitation`, `wind_speed_10m`, `surface_pressure`, `is_raining`, `hour`,
plus foreign keys to each dimension.

### Dimension Tables

| Table | Description |
|---|---|
| `dim_location` | One row per unique city/country/coordinate pair |
| `dim_date` | One row per calendar date, with day/month/year/weekday/is_weekend |
| `dim_weather_condition` | One row per distinct WMO weather code + category (e.g. "Clear", "Rain Showers") |

### Relationships
- `fact_weather_observations.location_id` → `dim_location.location_id`
- `fact_weather_observations.date_id` → `dim_date.date_id`
- `fact_weather_observations.weather_condition_id` → `dim_weather_condition.weather_condition_id`

This design supports analytical queries such as "average temperature per
city per month" or "number of rainy hours per city" with simple joins and
aggregations.

### Staging Table (ELT)
`staging_weather_raw` holds near-raw extracted records before the
transform step runs inside the ELT workflow (Part B).

---

## 5. ETL vs. ELT Workflows

### ETL (`WeatherPipeline.run_etl`)
1. **Extract** — call the Open-Meteo API for every configured location.
2. **Transform** — clean, validate, and enrich the data in-memory (pandas).
3. **Load** — load the transformed data directly into the dimension and
   fact tables.

### ELT (`WeatherPipeline.run_elt`)
1. **Extract** — same as above.
2. **Load (raw)** — load lightly-cleaned (but not fully transformed) data
   into the `staging_weather_raw` table.
3. **Transform** — apply full cleaning, validation, and enrichment logic.
4. **Load (final)** — load the transformed data into the dimension and
   fact tables.

---

## 6. Required Transformations

Implemented in `transform/transformer.py`:

1. Clean column names (lowercase, underscores)
2. Convert `observation_time`/`extracted_at` to datetime
3. Convert numeric fields to proper dtypes
4. Handle missing values (drop unusable rows, median-impute fillable fields)
5. Remove duplicate (city, observation_time) records
6. Validate weather measurements against physically plausible ranges
7. Create derived fields: `date`, `hour`, `temperature_fahrenheit`,
   `is_raining`, `weather_category`
8. Standardize location names (title case, trimmed)
9. Validate required columns are present before loading
10. Produce a flat DataFrame ready for fact/dimension loading

---

## 7. Data Validation (`validate/validator.py`)

Before loading, `DataValidator` checks:

- The DataFrame is not empty
- All required columns are present
- No nulls in key columns (`city`, `country`, `observation_time`)
- Core measurements fall within plausible ranges
- No duplicate (city, observation_time) records remain

Any failed check raises `DataValidationError`.

---

## 8. Logging & Error Handling

- All modules log to both the console and `logs/pipeline.log` via
  `utils/logger.py`.
- Custom exceptions (`utils/exceptions.py`) distinguish between:
  - `APIConnectionError` / `APIResponseError`
  - `TransformationError` / `DataValidationError`
  - `DatabaseConnectionError` / `DataLoadError`
- The extractor retries on connection/timeout errors (configurable retries
  and backoff) and skips (rather than crashes on) a single failing location.

---

## 9. Database

Default: **SQLite** (`data/weather_warehouse.db`) — zero setup, file-based,
ideal for grading/demo. Configuration in `utils/config.py` allows switching
to Postgres by setting environment variables (`WEATHER_DB_TYPE=postgres`,
`WEATHER_DB_HOST`, etc.) and extending `WeatherLoader._connect`.

Loads are **idempotent**: re-running the pipeline updates existing fact rows
(`ON CONFLICT ... DO UPDATE`) rather than creating duplicates.

---

## 10. Airflow Automation

DAG: `dags/weather_etl_dag.py` (`dag_id = weather_etl_pipeline`)

**Tasks** (run in this order via XCom):

```
extract_weather_data
        │
        ▼
transform_weather_data
        │
        ▼
validate_weather_data
        │
        ▼
load_weather_data
```

- **Schedule**: `@daily`
- **Retries**: 2 retries with a 5-minute delay per task
- Each task is a `PythonOperator` calling into the same `extract`,
  `transform`, `validate`, and `load` modules used by the standalone
  pipeline — no duplicated logic.

### Running the DAG locally

```bash
export AIRFLOW_HOME=~/airflow
airflow db init
cp dags/weather_etl_dag.py $AIRFLOW_HOME/dags/
# Ensure the project root is on PYTHONPATH so `extract`, `transform`, etc. import correctly
export PYTHONPATH=$PYTHONPATH:/path/to/weather_pipeline
airflow standalone
```

Then trigger `weather_etl_pipeline` from the Airflow UI and confirm all four
tasks complete successfully.

> **Note on `screenshots/dag_run_success.png`**: this repo's DAG was verified
> by loading it into Airflow 3.2 (`airflow tasks test` / `dags.py` import) and
> by running the identical extract -> transform -> validate -> load task chain
> end-to-end via `scripts/run_dag_simulation.py` (see `dag_run_output.txt`).
> The included image is a generated summary graphic of that successful run
> (all 4 tasks = success). When you run `airflow standalone` with a working
> internet connection to the Open-Meteo API, replace it with a real
> screenshot of the Airflow Grid/Graph view.

---

## 11. Setup & Usage

```bash
# 1. Install core dependencies (lightweight: pandas, requests, pytest)
pip install -r requirements.txt

# 2. Run the ETL pipeline once (requires internet access to api.open-meteo.com)
python -m pipeline.weather_pipeline

# 3. Or run the demo without any internet access (uses generated sample data)
python scripts/generate_sample_run.py

# 4. Simulate the full Airflow task chain without installing Airflow
python scripts/run_dag_simulation.py

# 5. Run unit tests
pytest tests/ -v
```

> Apache Airflow itself is only needed if you want to run `dags/weather_etl_dag.py`
> inside a real Airflow instance. It's a large install (500MB+); install it
> separately with `pip install apache-airflow apache-airflow-providers-standard`
> only when you have the disk space / want to demo the DAG UI.

---

## 12. Sample Output

After a run, query the warehouse directly:

```sql
SELECT l.city, d.full_date, f.hour, f.temperature_2m, w.weather_category
FROM fact_weather_observations f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_weather_condition w ON f.weather_condition_id = w.weather_condition_id
LIMIT 10;
```

See `sample_output.txt` for an example of dimension contents and aggregated
results (average temperature/precipitation per city).

---

## 13. Testing

`tests/` contains pytest unit tests covering:

- `test_extractor.py` — API success/failure/retry behavior (mocked requests)
- `test_transformer.py` — every transformation step + full pipeline
- `test_validator.py` — each data quality rule
- `test_loader.py` — schema creation, dimension/fact loads, idempotency

Run with:

```bash
pytest tests/ -v
```

---

## 14. Future Enhancements

- Swap SQLite for Postgres/Snowflake in production
- Add Slack/email alerting on pipeline failure
- Add data quality dashboards (e.g. Great Expectations + Airflow)
- Backfill historical weather data via Open-Meteo's archive API
- Add CI (GitHub Actions) to run `pytest` and lint on every push
