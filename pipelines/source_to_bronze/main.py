import logging
from pipelines.source_to_bronze.extract import BreweryExtractor
from pipelines.utils import get_paginated_data
from pipelines.load import save_json_data

# Basic logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def main():
    """
    Main function to orchestrate data extraction and loading.
    """
    try:
        logging.info("Starting brewery data extraction pipeline.")

        extractor = BreweryExtractor()
        data = get_paginated_data(extractor)
        if data:
            save_json_data(
                data=data,
                layer="bronze",
                path="/opt/airflow/datalake/bronze/brewery",
                filename="brewery",
                format="json",
            )
            logging.info("Data successfully saved to datalake/bronze.")
        else:
            logging.warning("No data retrieved. Skipping save step.")
            raise ValueError()

    except Exception as e:
        logging.error(f"An error occurred in the pipeline: {e}", exc_info=True)
        raise e
