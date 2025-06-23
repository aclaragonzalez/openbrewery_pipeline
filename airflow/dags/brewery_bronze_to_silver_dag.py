from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipelines.bronze_to_silver.main import BreweryProcessor
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from pipelines.utils import send_email_with_log
from airflow.utils.trigger_rule import TriggerRule
import traceback
import logging


def run_processor():
    try:
        BreweryProcessor().run()
    except Exception as e:
        logging.error(f"Error: {e}\n{traceback.format_exc()}")
        raise


with DAG(
    dag_id="brewery_bronze_to_silver",
    start_date=datetime(2025, 6, 24, 0, 0, 0),
    schedule_interval="@monthly",
    catchup=False,
) as dag:
    wait_for_extraction = ExternalTaskSensor(
        task_id="wait_extraction",
        external_dag_id="brewery_source_to_bronze",
        external_task_id="brewery_extractor",
        mode="poke",
        timeout=600,
        poke_interval=60,
    )
    processor_task = PythonOperator(
        task_id="brewery_processor", python_callable=run_processor
    )
    send_email_task = PythonOperator(
        task_id="send_failure_email_with_log",
        python_callable=send_email_with_log,
        trigger_rule=TriggerRule.ONE_FAILED,
        provide_context=True,
    )

wait_for_extraction >> processor_task >> send_email_task
