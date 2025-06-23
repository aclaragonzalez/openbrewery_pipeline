from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from pipelines.source_to_bronze.main import main
from datetime import datetime, timedelta
from pipelines.utils import send_email_with_log

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 22),
    "email": ["gonzalez.anaclara@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="brewery_source_to_bronze",
    start_date=datetime(2025, 6, 24, 0, 0, 0),
    schedule_interval="@monthly",
    catchup=False,
) as dag:
    extraction_task = PythonOperator(task_id="brewery_extractor", python_callable=main)
    send_email_task = PythonOperator(
        task_id="send_failure_email_with_log",
        python_callable=send_email_with_log,
        trigger_rule=TriggerRule.ONE_FAILED,
        provide_context=True,
    )

extraction_task >> send_email_task
