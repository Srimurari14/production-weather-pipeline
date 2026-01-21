from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def test_snowflake():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_weather")
    result = hook.get_first("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();")
    print(result)

with DAG(
    dag_id="test_snowflake_connection",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    test_task = PythonOperator(
        task_id="test_snowflake",
        python_callable=test_snowflake
    )
