"""
Load module.

Responsible for connecting to the SQL database, creating tables from
the star schema DDL, and loading transformed weather data into the
staging table (ELT) and the dimension/fact tables (ETL and ELT).
"""

import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

from utils.config import SQL_DIR, SQLITE_DB_PATH
from utils.exceptions import DatabaseConnectionError, DataLoadError
from utils.logger import get_logger

logger = get_logger(__name__)

SCHEMA_FILE = SQL_DIR / "star_schema.sql"


class WeatherLoader:
    """
    Handles all database interactions for the weather pipeline:
    connection management, schema creation, staging loads, and
    dimension/fact loads.

    Uses SQLite by default (file-based, zero external dependencies),
    which keeps the project easy to run and grade. The class is
    structured so swapping to another DB-API 2.0 compliant database
    (e.g. Postgres via psycopg2) would only require changing
    `_connect`.
    """

    def __init__(self, db_path: str = SQLITE_DB_PATH, schema_file: Path = SCHEMA_FILE):
        self.db_path = db_path
        self.schema_file = schema_file
        self.conn: Optional[sqlite3.Connection] = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------
    def connect(self) -> sqlite3.Connection:
        """
        Open a connection to the SQLite database.

        Raises:
            DatabaseConnectionError: if the connection cannot be established.
        """
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON;")
            logger.info("Connected to database at %s", self.db_path)
            return self.conn
        except sqlite3.Error as exc:
            logger.error("Failed to connect to database: %s", exc)
            raise DatabaseConnectionError(f"Could not connect to database: {exc}") from exc

    def close(self) -> None:
        """Close the database connection if open."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed.")

    def __enter__(self) -> "WeatherLoader":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Schema creation
    # ------------------------------------------------------------------
    def create_tables(self) -> None:
        """
        Create all tables (staging, dimensions, fact) defined in
        sql/star_schema.sql if they do not already exist.

        Raises:
            DatabaseConnectionError: if no connection is open.
            DataLoadError: if schema creation fails.
        """
        if self.conn is None:
            raise DatabaseConnectionError("No active database connection.")

        try:
            with open(self.schema_file, "r", encoding="utf-8") as f:
                schema_sql = f.read()

            self.conn.executescript(schema_sql)
            self.conn.commit()
            logger.info("Database tables created/verified successfully.")
        except (sqlite3.Error, OSError) as exc:
            logger.error("Failed to create tables: %s", exc)
            raise DataLoadError(f"Failed to create tables: {exc}") from exc

    # ------------------------------------------------------------------
    # ELT: staging load
    # ------------------------------------------------------------------
    def load_staging(self, df: pd.DataFrame) -> None:
        """
        Load a DataFrame of (mostly) raw weather data into the
        staging_weather_raw table. Used by the ELT workflow.

        Raises:
            DatabaseConnectionError: if no connection is open.
            DataLoadError: if the insert fails.
        """
        if self.conn is None:
            raise DatabaseConnectionError("No active database connection.")

        staging_cols = [
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
            "extracted_at",
        ]

        try:
            staging_df = df[staging_cols].copy()
            staging_df["observation_time"] = staging_df["observation_time"].astype(str)
            staging_df["extracted_at"] = staging_df["extracted_at"].astype(str)

            staging_df.to_sql(
                "staging_weather_raw", self.conn, if_exists="append", index=False
            )
            self.conn.commit()
            logger.info("Loaded %s rows into staging_weather_raw", len(staging_df))
        except (sqlite3.Error, KeyError) as exc:
            logger.error("Failed to load staging data: %s", exc)
            raise DataLoadError(f"Failed to load staging data: {exc}") from exc

    # ------------------------------------------------------------------
    # Dimension loads
    # ------------------------------------------------------------------
    def load_dim_location(self, df: pd.DataFrame) -> None:
        """Upsert unique locations into dim_location."""
        if self.conn is None:
            raise DatabaseConnectionError("No active database connection.")

        try:
            locations = df[["city", "country", "latitude", "longitude"]].drop_duplicates()
            cur = self.conn.cursor()
            for _, row in locations.iterrows():
                cur.execute(
                    """
                    INSERT INTO dim_location (city, country, latitude, longitude)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (city, country, latitude, longitude) DO NOTHING
                    """,
                    (row["city"], row["country"], row["latitude"], row["longitude"]),
                )
            self.conn.commit()
            logger.info("dim_location upsert complete (%s candidate rows)", len(locations))
        except sqlite3.Error as exc:
            logger.error("Failed to load dim_location: %s", exc)
            raise DataLoadError(f"Failed to load dim_location: {exc}") from exc

    def load_dim_date(self, df: pd.DataFrame) -> None:
        """Upsert unique dates into dim_date with derived attributes."""
        if self.conn is None:
            raise DatabaseConnectionError("No active database connection.")

        try:
            dates = pd.to_datetime(df["date"].astype(str)).drop_duplicates()
            cur = self.conn.cursor()
            for d in dates:
                cur.execute(
                    """
                    INSERT INTO dim_date (full_date, day, month, year, day_of_week, is_weekend)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (full_date) DO NOTHING
                    """,
                    (
                        d.strftime("%Y-%m-%d"),
                        d.day,
                        d.month,
                        d.year,
                        d.strftime("%A"),
                        1 if d.weekday() >= 5 else 0,
                    ),
                )
            self.conn.commit()
            logger.info("dim_date upsert complete (%s candidate rows)", len(dates))
        except sqlite3.Error as exc:
            logger.error("Failed to load dim_date: %s", exc)
            raise DataLoadError(f"Failed to load dim_date: {exc}") from exc

    def load_dim_weather_condition(self, df: pd.DataFrame) -> None:
        """Upsert unique (weather_code, weather_category) pairs."""
        if self.conn is None:
            raise DatabaseConnectionError("No active database connection.")

        try:
            conditions = df[["weather_code", "weather_category"]].drop_duplicates()
            cur = self.conn.cursor()
            for _, row in conditions.iterrows():
                cur.execute(
                    """
                    INSERT INTO dim_weather_condition (weather_code, weather_category)
                    VALUES (?, ?)
                    ON CONFLICT (weather_code) DO NOTHING
                    """,
                    (int(row["weather_code"]), row["weather_category"]),
                )
            self.conn.commit()
            logger.info(
                "dim_weather_condition upsert complete (%s candidate rows)",
                len(conditions),
            )
        except sqlite3.Error as exc:
            logger.error("Failed to load dim_weather_condition: %s", exc)
            raise DataLoadError(f"Failed to load dim_weather_condition: {exc}") from exc

    # ------------------------------------------------------------------
    # Fact load
    # ------------------------------------------------------------------
    def load_fact_weather_observations(self, df: pd.DataFrame) -> None:
        """
        Load weather observations into the fact table, resolving
        foreign keys against the dimension tables.

        Assumes dimension tables have already been populated via
        load_dim_location, load_dim_date, and load_dim_weather_condition.
        """
        if self.conn is None:
            raise DatabaseConnectionError("No active database connection.")

        try:
            cur = self.conn.cursor()

            # Build lookup maps for surrogate keys
            location_map = {
                (row[0], row[1], row[2], row[3]): row[4]
                for row in cur.execute(
                    "SELECT city, country, latitude, longitude, location_id FROM dim_location"
                ).fetchall()
            }
            date_map = {
                row[0]: row[1]
                for row in cur.execute("SELECT full_date, date_id FROM dim_date").fetchall()
            }
            condition_map = {
                row[0]: row[1]
                for row in cur.execute(
                    "SELECT weather_code, weather_condition_id FROM dim_weather_condition"
                ).fetchall()
            }

            inserted = 0
            for _, row in df.iterrows():
                loc_key = (row["city"], row["country"], row["latitude"], row["longitude"])
                location_id = location_map.get(loc_key)
                date_id = date_map.get(str(row["date"]))
                weather_condition_id = condition_map.get(int(row["weather_code"]))

                if not all([location_id, date_id, weather_condition_id]):
                    logger.warning(
                        "Skipping row for %s at %s: missing dimension key(s)",
                        row["city"],
                        row["observation_time"],
                    )
                    continue

                cur.execute(
                    """
                    INSERT INTO fact_weather_observations (
                        location_id, date_id, weather_condition_id, hour,
                        observation_time, temperature_2m, temperature_fahrenheit,
                        relative_humidity_2m, precipitation, is_raining,
                        wind_speed_10m, surface_pressure, extracted_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (location_id, observation_time) DO UPDATE SET
                        temperature_2m = excluded.temperature_2m,
                        temperature_fahrenheit = excluded.temperature_fahrenheit,
                        relative_humidity_2m = excluded.relative_humidity_2m,
                        precipitation = excluded.precipitation,
                        is_raining = excluded.is_raining,
                        wind_speed_10m = excluded.wind_speed_10m,
                        surface_pressure = excluded.surface_pressure,
                        loaded_at = datetime('now')
                    """,
                    (
                        location_id,
                        date_id,
                        weather_condition_id,
                        int(row["hour"]),
                        str(row["observation_time"]),
                        float(row["temperature_2m"]),
                        float(row["temperature_fahrenheit"]),
                        float(row["relative_humidity_2m"]) if pd.notna(row["relative_humidity_2m"]) else None,
                        float(row["precipitation"]) if pd.notna(row["precipitation"]) else None,
                        1 if bool(row["is_raining"]) else 0,
                        float(row["wind_speed_10m"]) if pd.notna(row["wind_speed_10m"]) else None,
                        float(row["surface_pressure"]) if pd.notna(row["surface_pressure"]) else None,
                        str(row["extracted_at"]),
                    ),
                )
                inserted += 1

            self.conn.commit()
            logger.info("Loaded %s rows into fact_weather_observations", inserted)
        except sqlite3.Error as exc:
            logger.error("Failed to load fact_weather_observations: %s", exc)
            raise DataLoadError(f"Failed to load fact_weather_observations: {exc}") from exc

    # ------------------------------------------------------------------
    # Convenience: full warehouse load (used by ETL workflow)
    # ------------------------------------------------------------------
    def load_warehouse(self, df: pd.DataFrame) -> None:
        """
        Load a fully-transformed DataFrame into all dimension tables and
        the fact table. This is the main entry point for the ETL workflow.
        """
        self.load_dim_location(df)
        self.load_dim_date(df)
        self.load_dim_weather_condition(df)
        self.load_fact_weather_observations(df)
