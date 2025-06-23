from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipelines.silver_to_gold.main import BreweryRefiner
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from pipelines.utils import send_email_with_log
from airflow.utils.trigger_rule import TriggerRule
import traceback
import logging


def run_refiner():
    try:
        BreweryRefiner().run()
    except Exception as e:
        logging.error(f"Error: {e}\n{traceback.format_exc()}")
        raise


with DAG(
    dag_id="brewery_silver_to_gold",
    start_date=datetime(2025, 6, 24, 0, 0, 0),
    schedule_interval="@monthly",
    catchup=False,
) as dag:
    wait_for_processor = ExternalTaskSensor(
        task_id="wait_processing",
        external_dag_id="brewery_bronze_to_silver",
        external_task_id="brewery_processor",
        mode="poke",
        timeout=600,
        poke_interval=60,
    )

    refiner_task = PythonOperator(
        task_id="brewery_refiner", python_callable=run_refiner
    )

    send_email_task = PythonOperator(
        task_id="send_failure_email_with_log",
        python_callable=send_email_with_log,
        trigger_rule=TriggerRule.ONE_FAILED,
        provide_context=True,
    )

wait_for_processor >> refiner_task >> send_email_task
