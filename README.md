# Production Weather Analytics Pipeline

A production-grade, end-to-end data pipeline that ingests real-time weather data from an external API, performs validation and transformation using Python, loads data into Snowflake, and builds analytics-ready tables using dbt.
The entire workflow is automated, scheduled, and CI-safe using GitHub Actions.

This project demonstrates real-world data engineering practices including API ingestion, data quality validation, cloud data warehousing, analytics modeling, and CI/CD orchestration.

---

## Project Overview

The pipeline is designed for **repeatable, scheduled ingestion of real-time data**, which mirrors how production analytics systems operate.

Key capabilities:

* Real-time API ingestion
* Data validation and transformation
* Cloud-native loading into Snowflake
* Analytics modeling using dbt
* Fully automated CI/CD execution
* Secure handling of credentials and secrets

---

## Architecture

**Data Flow**

1. OpenWeather API
2. Raw JSON ingestion using Python
3. Transformation and validation using Pandas
4. CSV upload to Snowflake internal stage
5. RAW tables in Snowflake
6. dbt models in the ANALYTICS schema

---

## Technology Stack

* **Language:** Python 3.10
* **Data Processing:** Pandas
* **Data Warehouse:** Snowflake
* **Transformation Layer:** dbt (dbt-snowflake)
* **Orchestration and Scheduling:** GitHub Actions
* **CI/CD:** GitHub Actions
* **Secrets Management:** GitHub Repository Secrets

---

## Data Sources

### OpenWeather API (Real-Time)

* Source: [https://openweathermap.org/api](https://openweathermap.org/api)
* Used for scheduled ingestion of current weather data
* Provides real-time city-level weather observations including temperature, humidity, pressure, wind, and weather conditions

### Open-Meteo Historical API (Out of Pipeline)

* Source: [https://open-meteo.com/en/docs/historical-weather-api](https://open-meteo.com/en/docs/historical-weather-api)
* Used only for exploration and schema design
* Not part of the automated pipeline, as historical backfills are typically handled separately in production systems

---

## Repository Structure

```
production-weather-pipeline/
│
├── scripts/
│   ├── ingest_weather.py              # Real-time API ingestion
│   ├── fact_weather_current.py        # Current weather transformation
│   ├── upload_to_stage.py             # Upload CSVs to Snowflake stage
│   ├── copy_into_raw.py               # Load data into RAW tables
│   ├── get_snowflake_connection.py    # Snowflake connection handler
│   └── run_pipeline.py                # Pipeline orchestrator
│
├── configs/
│   └── cities.py                      # City-level configuration
│
├── data/
│   ├── raw/                           # Raw API responses (JSON)
│   └── transformed/                  # Generated CSV outputs
│
├── dbt_weather/
│   ├── models/
│   │   ├── staging/
│   │   └── analytics/
│   ├── dbt_project.yml
│
├── .github/workflows/
│   └── pipeline.yml                  # GitHub Actions workflow
│
├── requirements.txt
└── README.md
```

---

## Pipeline Execution Flow

### Real-Time Data Ingestion

* Fetches current weather data for multiple cities using the OpenWeather API
* Stores raw JSON responses partitioned by execution date

### Data Transformation and Validation

* Flattens nested JSON into structured tabular format
* Generates:

  * `fact_weather_current`
  * `dim_location`
* Applies validation checks to ensure data quality and schema integrity

### Snowflake Load (RAW Layer)

* Uploads transformed CSV files to a Snowflake internal stage
* Loads data into RAW tables using `COPY INTO`
* Ensures idempotent loads by truncating tables before insertion

### Analytics Modeling (dbt)

* dbt models transform RAW tables into analytics-ready datasets
* Models are materialized in the `ANALYTICS` schema
* dbt tests validate correctness and consistency

---

## Scheduling and Automation

The pipeline is fully automated using GitHub Actions:

* Scheduled execution every 6 hours
* Manual execution via `workflow_dispatch`
* CI-safe execution using ephemeral runners

---

## Secrets Management

All sensitive credentials are stored securely using GitHub Actions Secrets:

* `OPENWEATHER_API_KEY`
* `SNOWFLAKE_ACCOUNT`
* `SNOWFLAKE_USER`
* `SNOWFLAKE_PASSWORD`
* `SNOWFLAKE_ROLE`
* `SNOWFLAKE_WAREHOUSE`
* `SNOWFLAKE_DATABASE`
* `SNOWFLAKE_SCHEMA`

No secrets are committed to the repository.

---

## Engineering Highlights

* CI-safe directory creation without reliance on local state
* Hardened Snowflake connections with retries and timeouts
* Clear separation between ingestion, transformation, and analytics layers
* Modular and testable Python scripts
* dbt integration without committing profiles or credentials
* Production-oriented orchestration design

---

## Local Development (Optional)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m scripts.run_pipeline
```

Local execution requires environment variables equivalent to those used in CI.

---

## Future Enhancements

* Incremental dbt models
* Data freshness and volume tests
* Alerting on pipeline failures
* Partitioned Snowflake tables
* Migration to Airflow or Prefect for advanced orchestration


