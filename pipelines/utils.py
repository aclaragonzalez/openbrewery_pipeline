import logging
from pyspark.sql import SparkSession
import yaml
from pyspark.sql.types import StructType, StructField
import pyspark.sql.types as T
from typing import List, Dict, Any
import os

# Basic logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

recipients = os.getenv("AIRFLOW_RECIPIENT_EMAIL")


def get_paginated_data(extractor, **kwargs) -> List[Dict[str, Any]]:
    """
    Fetches all paginated data from the provided extractor.

    Parameters
    ----------
    extractor : object
        An object with a `fetch(page: int)` method that returns data for each page.
    **kwargs : dict
        Additional query parameters supported by the API (e.g., by_city, by_state, etc.).

    Returns
    -------
    list of dict
        A list containing all data retrieved from all pages.
    """
    all_data = []
    page = 1

    logging.info("Starting paginated data extraction.")

    while True:
        try:
            page_data = extractor.fetch(page, **kwargs)
        except Exception as e:
            logging.error(f"Failed to fetch data for page {page}: {e}", exc_info=True)
            break
        if not page_data:
            logging.info("No more data returned by the API.")
            break

        all_data.extend(page_data)
        logging.info(f"Fetched page {page} with {len(page_data)} records.")
        page += 1

    logging.info(f"Total records fetched: {len(all_data)}")
    return all_data


def get_spark_session(app_name: str) -> SparkSession:
    """
    Creates and returns a SparkSession with the specified application name.

    Parameters
    ----------
    app_name : str
        Name of the Spark application.

    Returns
    -------
    SparkSession
        An active Spark session.
    """
    try:
        spark = (
            SparkSession.builder.appName(app_name)
            .master("local[2]")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.maxResultSize", "1g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.jars", "file:///opt/airflow/jars/deequ-2.0.7-spark-3.5.jar")
            .getOrCreate()
        )
        logging.info(f"SparkSession created for application: {app_name}")
        return spark
    except Exception as e:
        logging.error(f"Failed to create SparkSession: {e}", exc_info=True)
        raise


def load_schema(layer: str, table_name: str) -> StructType:
    """
    Loads a schema definition from the tables.yaml configuration file.

    Parameters
    ----------
    layer : str
        The data layer (e.g., 'bronze', 'silver', 'gold').

    table_name : str
        The name of the table to load the schema for.

    Returns
    -------
    StructType
        A PySpark StructType object defining the schema.
    """
    try:
        with open("config/tables.yaml", "r") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logging.error("Configuration file 'tables.yaml' not found.")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML file: {e}", exc_info=True)
        raise

    try:
        fields = config[layer][table_name]["fields"]

        schema = StructType(
            [
                StructField(field, getattr(T, f"{dtype.capitalize()}Type")(), True)
                for field, dtype in fields.items()
            ]
        )
        return schema
    except KeyError as e:
        logging.error(f"Missing key in schema configuration: {e}", exc_info=True)
        raise
    except AttributeError as e:
        logging.error(f"Invalid data type in schema configuration: {e}", exc_info=True)
        raise


def send_email_with_log(**context: Dict[str, Any]) -> None:
    """
    Sends an email notification containing the log content of a failed task.

    This function is intended to be used in Airflow's `on_failure_callback` to
    automatically send failure logs when a task fails.

    Parameters
    ----------
    context : dict[str, Any]
        Airflow context dictionary automatically passed to callbacks.

    Raises
    ------
    Exception
        If the log file exists but an unexpected error occurs while reading or sending the email.
    """
    from airflow.utils.email import send_email

    try:
        task_instance = context["ti"]

        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        run_id = task_instance.run_id
        try_number = task_instance.try_number
        execution_date = context.get("execution_date", "Unknown execution date")

        log_path = f"/opt/airflow/logs/{dag_id}/{task_id}/{run_id}/{try_number}.log"
        log_content: str

        try:
            with open(log_path, "r") as f:
                log_content = f.read()
                logging.info(f"Log file successfully read from: {log_path}")
        except FileNotFoundError:
            log_content = "Log file not found. Please verify the path or check if remote log storage is enabled."
            logging.warning(f"Log file not found at: {log_path}")

        except Exception as e:
            logging.error(
                f"Unexpected error while reading log file: {e}", exc_info=True
            )
            raise

        subject = f"[ERROR] DAG {dag_id} failed"
        html_content = f"""
            <h3>Task <b>{task_instance}</b> failed!</h3>
            <p><b>Execution:</b> {execution_date}</p>
            <pre>{log_content}</pre>
        """

        send_email(to=recipients, subject=subject, html_content=html_content)
        logging.info(
            f"Failure notification email sent for DAG {dag_id}, Task {task_id}"
        )

    except Exception as e:
        logging.error("Failed to send failure email notification.", exc_info=True)
        raise e
