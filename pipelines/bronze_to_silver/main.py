import logging
from pyspark.sql import DataFrame
from pipelines.transform import BaseTransformer
from pipelines.bronze_to_silver.brewery_transform import BreweryTransformer
from pipelines.utils import get_spark_session, load_schema
from pipelines.read import get_latest_extraction_path
from pipelines.bronze_to_silver.brewery_validate import BreweryValidator

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class BreweryProcessor(BaseTransformer):
    """
    ETL processor class for brewery data from bronze to silver layer.
    Orchestrates extraction, transformation, validation, and loading.
    """

    def __init__(self):
        """
        Initialize Spark session and transformer instance.
        """
        self.spark = get_spark_session(app_name="BronzeToSilverProcessor")
        self.transformer = BreweryTransformer()

    def extract(self) -> DataFrame:
        """
        Extract the latest brewery JSON data from the bronze layer,
        applying the silver layer schema.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame loaded from the latest bronze JSON file.
        """
        latest_file_extraction = get_latest_extraction_path(
            path="/opt/airflow/datalake/bronze/brewery", filename="brewery.json"
        )
        schema = load_schema("silver", "brewery")
        df = (
            self.spark.read.schema(schema)
            .option("multiline", "true")
            .json(latest_file_extraction)
        )
        logging.info(f"Data extracted from {latest_file_extraction}")
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply data transformations using BreweryTransformer.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            Input DataFrame to transform.

        Returns
        -------
        pyspark.sql.DataFrame
            Transformed DataFrame.
        """
        logging.info("Starting transformation step.")
        transformed_df = self.transformer.apply(df)
        logging.info("Transformation step completed.")
        return transformed_df

    def validate(self, df: DataFrame) -> None:
        """
        Validate the transformed DataFrame using BreweryValidator.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            DataFrame to validate.

        Raises
        ------
        AssertionError
            If validation fails.
        """
        logging.info("Starting validation step.")
        validator = BreweryValidator(df, self.spark)
        validator.run()
        logging.info("Validation step completed successfully.")

    def load(self, df: DataFrame) -> None:
        """
        Write the validated DataFrame to the silver layer in Parquet format,
        partitioned by 'country' and 'state'. Stops Spark session afterward.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            DataFrame to load.
        """
        logging.info("Starting load step.")
        df.write.mode("overwrite").partitionBy("country", "state").parquet(
            "/opt/airflow/datalake/silver/brewery"
        )
        logging.info("Data written to datalake/silver/brewery successfully.")
        self.spark.stop()
        logging.info("Spark session stopped.")
