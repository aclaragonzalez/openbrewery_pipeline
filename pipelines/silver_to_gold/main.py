import logging
from pyspark.sql import DataFrame
from pipelines.transform import BaseTransformer
from pipelines.silver_to_gold.brewery_transform import BreweryTransformer
from pipelines.silver_to_gold.brewery_validate import BreweryValidator
from pipelines.utils import get_spark_session, load_schema

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class BreweryRefiner(BaseTransformer):
    """
    Refiner class for processing brewery data from the silver to gold layer.
    Executes extraction, transformation, validation, and load operations.
    """

    def __init__(self):
        """
        Initializes the Spark session and transformation logic.
        """
        self.spark = get_spark_session(app_name="SilverToGoldProcessor")
        self.transformer = BreweryTransformer()

    def extract(self) -> DataFrame:
        """
        Extracts data from the silver layer in Parquet format using the gold schema.

        Returns
        -------
        pyspark.sql.DataFrame
            The extracted DataFrame.
        """
        path = "/opt/airflow/datalake/silver/brewery"
        schema = load_schema("gold", "brewery")
        df = self.spark.read.schema(schema).parquet(path)
        logging.info(f"Data extracted from silver layer at: {path}")
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transforms the extracted DataFrame using BreweryTransformer.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            The DataFrame to transform.

        Returns
        -------
        pyspark.sql.DataFrame
            The transformed DataFrame.
        """
        logging.info("Starting transformation step.")
        df_transformed = self.transformer.apply(df)
        logging.info("Transformation completed.")
        return df_transformed

    def validate(self, df: DataFrame) -> None:
        """
        Validates the transformed DataFrame using BreweryValidator.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            The DataFrame to validate.

        Raises
        ------
        AssertionError
            If any validation constraints fail.
        """
        logging.info("Starting validation step.")
        validator = BreweryValidator(df, self.spark)
        validator.run()
        logging.info("Validation successful.")

    def load(self, df: DataFrame) -> None:
        """
        Loads the validated DataFrame into the gold layer in Parquet format.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            The DataFrame to load.
        """
        output_path = "/opt/airflow/datalake/gold/brewery"
        logging.info(f"Writing data to: {output_path}")
        df.write.mode("overwrite").parquet(output_path)
        logging.info("Data written to gold layer successfully.")
        self.spark.stop()
        logging.info("Spark session stopped.")
