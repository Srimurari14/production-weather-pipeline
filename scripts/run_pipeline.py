import logging
from scripts.ingest_weather import run_ingest_weather
from scripts.fact_weather_current import run_fact_weather_current
from scripts.fact_weather_daily import run_fact_weather_daily
from scripts.upload_to_stage import upload_to_stage
from scripts.copy_into_raw import copy_into_raw
from scripts.get_snowflake_connection import get_snowflake_connection

from dotenv import load_dotenv
load_dotenv()



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_pipeline():
    logger.info("Pipeline started")

    run_ingest_weather()
    logger.info("JSON ingestion complete")

    run_fact_weather_current()
    run_fact_weather_daily()
    logger.info("CSV transformations complete")

    conn = get_snowflake_connection()

    logger.info("Snowflake connection established successfully")

    try:
        upload_to_stage(conn, local_dir="data/transformed")
        copy_into_raw(conn)
        logger.info("Snowflake load complete")

    except Exception:
        logger.exception("Pipeline failed")
        raise

    finally:
        conn.close()

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    run_pipeline()
