-- =============================================================================
-- WEATHER ANALYTICS STAR SCHEMA
-- =============================================================================
-- This schema supports both the ETL workflow (Part A) and the ELT workflow
-- (Part B). Raw data lands in `staging_weather_raw`, is transformed, and is
-- then loaded into the dimension tables and the fact table below.
--
-- SCHEMA OVERVIEW
-- ---------------
-- dim_location   : One row per unique city/country/coordinate combination.
-- dim_date       : One row per calendar date, with useful date attributes.
-- dim_weather_condition : One row per distinct weather_code / category.
-- fact_weather_observations : One row per (location, date, hour) observation,
--                              referencing the three dimension tables above
--                              via surrogate keys, plus all numeric measures.
--
-- This is a classic star schema: a single central fact table surrounded by
-- denormalized dimension tables, optimized for analytical (OLAP) queries
-- such as "average temperature per city per month" or "rainy days per city".
-- =============================================================================

-- -----------------------------------------------------------------------
-- STAGING TABLE (used by the ELT workflow - Part B)
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_weather_raw (
    staging_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    city            TEXT NOT NULL,
    country         TEXT NOT NULL,
    latitude        REAL NOT NULL,
    longitude       REAL NOT NULL,
    observation_time TEXT NOT NULL,
    temperature_2m  REAL,
    relative_humidity_2m REAL,
    precipitation   REAL,
    wind_speed_10m  REAL,
    weather_code    REAL,
    surface_pressure REAL,
    extracted_at    TEXT NOT NULL,
    loaded_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

-- -----------------------------------------------------------------------
-- DIMENSION: dim_location
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_location (
    location_id INTEGER PRIMARY KEY AUTOINCREMENT,
    city        TEXT NOT NULL,
    country     TEXT NOT NULL,
    latitude    REAL NOT NULL,
    longitude   REAL NOT NULL,
    UNIQUE (city, country, latitude, longitude)
);

-- -----------------------------------------------------------------------
-- DIMENSION: dim_date
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    full_date   TEXT NOT NULL UNIQUE,   -- YYYY-MM-DD
    day         INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    year        INTEGER NOT NULL,
    day_of_week TEXT NOT NULL,          -- e.g. 'Monday'
    is_weekend  INTEGER NOT NULL        -- 0 / 1
);

-- -----------------------------------------------------------------------
-- DIMENSION: dim_weather_condition
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_weather_condition (
    weather_condition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    weather_code         INTEGER NOT NULL UNIQUE,
    weather_category     TEXT NOT NULL   -- e.g. 'Clear', 'Rain Showers'
);

-- -----------------------------------------------------------------------
-- FACT: fact_weather_observations
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_weather_observations (
    observation_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id      INTEGER NOT NULL,
    date_id          INTEGER NOT NULL,
    weather_condition_id INTEGER NOT NULL,
    hour              INTEGER NOT NULL,
    observation_time  TEXT NOT NULL,
    temperature_2m    REAL NOT NULL,
    temperature_fahrenheit REAL NOT NULL,
    relative_humidity_2m REAL,
    precipitation     REAL,
    is_raining        INTEGER NOT NULL,
    wind_speed_10m    REAL,
    surface_pressure  REAL,
    extracted_at      TEXT NOT NULL,
    loaded_at         TEXT NOT NULL DEFAULT (datetime('now')),

    FOREIGN KEY (location_id) REFERENCES dim_location (location_id),
    FOREIGN KEY (date_id) REFERENCES dim_date (date_id),
    FOREIGN KEY (weather_condition_id)
        REFERENCES dim_weather_condition (weather_condition_id),

    UNIQUE (location_id, observation_time)
);

-- -----------------------------------------------------------------------
-- Helpful indexes for analytical queries
-- -----------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_location ON fact_weather_observations (location_id);
CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_weather_observations (date_id);
CREATE INDEX IF NOT EXISTS idx_fact_weather_condition ON fact_weather_observations (weather_condition_id);
