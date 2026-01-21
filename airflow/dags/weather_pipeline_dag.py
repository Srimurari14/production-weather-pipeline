from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# PROJECT_ROOT = Path("/opt/airflow/project")
# sys.path.append(str(PROJECT_ROOT))

from scripts.ingest_weather import ingest_weather
from scripts.fact_weather_current import fact_weather_current


def put_files_to_stage():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_weather")

    hook.run("""
        PUT file:///opt/airflow/data/transformed/fact_weather_current.csv
        @WEATHER_DB.RAW.WEATHER_STAGE
        AUTO_COMPRESS=TRUE
        OVERWRITE=TRUE;
    """)

    hook.run("""
        PUT file:///opt/airflow/data/transformed/dim_location.csv
        @WEATHER_DB.RAW.WEATHER_STAGE
        AUTO_COMPRESS=TRUE
        OVERWRITE=TRUE;
    """)

def copy_stage_to_raw():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_weather")

    hook.run("TRUNCATE TABLE WEATHER_DB.RAW.fact_weather_current;")

    hook.run("""
        COPY INTO WEATHER_DB.RAW.fact_weather_current
        FROM @WEATHER_DB.RAW.WEATHER_STAGE/fact_weather_current.csv.gz
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
        )
        ON_ERROR = 'CONTINUE';
    """)

    hook.run("TRUNCATE TABLE WEATHER_DB.RAW.dim_location;")

    hook.run("""
        COPY INTO WEATHER_DB.RAW.dim_location
        FROM @WEATHER_DB.RAW.WEATHER_STAGE/dim_location.csv.gz
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
        )
        ON_ERROR = 'CONTINUE';
    """)


default_args = {
    "owner": "srimurari",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="weather_pipeline",
    description="Daily weather ETL (API → CSV → Snowflake → dbt)",
    default_args=default_args,
    start_date=datetime(2026, 1, 19),
    schedule_interval="0 */6 * * *",
    catchup=False,
    tags=["weather", "etl", "snowflake", "dbt"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_weather",
        python_callable=ingest_weather,
    )

    transform = PythonOperator(
        task_id="transform_weather",
        python_callable=fact_weather_current,
    )

    upload_stage = PythonOperator(
        task_id="upload_to_stage",
        python_callable=put_files_to_stage,
    )

    copy_raw = PythonOperator(
        task_id="copy_stage_to_raw",
        python_callable=copy_stage_to_raw,
    )

    run_dbt = BashOperator(
    task_id="run_dbt_models",
    bash_command="""
    export SNOWFLAKE_ACCOUNT="{{ conn.snowflake_weather.extra_dejson.account }}"
    export SNOWFLAKE_USER="{{ conn.snowflake_weather.login }}"
    export SNOWFLAKE_PASSWORD="{{ conn.snowflake_weather.password }}"
    export SNOWFLAKE_ROLE="{{ conn.snowflake_weather.extra_dejson.role }}"

    cd /opt/airflow/project/dbt_weather &&
    dbt run --select fact_weather_current --profiles-dir .
    """
    )

    run_dbt_test = BashOperator(
        task_id="run_dbt_tests",
        bash_command="""
        export SNOWFLAKE_ACCOUNT="{{ conn.snowflake_weather.extra_dejson.account }}"
        export SNOWFLAKE_USER="{{ conn.snowflake_weather.login }}"
        export SNOWFLAKE_PASSWORD="{{ conn.snowflake_weather.password }}"
        export SNOWFLAKE_ROLE="{{ conn.snowflake_weather.extra_dejson.role }}"

        cd /opt/airflow/project/dbt_weather &&
        dbt test --profiles-dir .
        """
    )

    ingest >> transform >> upload_stage >> copy_raw >> run_dbt >> run_dbt_test
