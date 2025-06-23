import os
import logging

os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql import DataFrame, SparkSession
from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
from pydeequ.verification import VerificationSuite

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class BreweryValidator:
    """
    Validator class that uses PyDeequ to run data quality checks
    on a brewery DataFrame during the Bronze-to-Silver layer transition.
    """

    def __init__(self, df: DataFrame, spark: SparkSession) -> None:
        """
        Initialize the validator with a Spark DataFrame and Spark session.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            DataFrame containing brewery data to validate.
        spark : SparkSession
            Active Spark session.
        """
        self.df = df
        self.spark = spark

    def run(self) -> None:
        """
        Runs a set of data quality checks on the brewery DataFrame.

        Checks completeness, uniqueness, data types, and valid coordinate ranges.

        Raises
        ------
        AssertionError
            If one or more constraints fail during validation.
        Exception
            If an unexpected error occurs during validation.
        """
        try:
            check = Check(
                self.spark, CheckLevel.Error, "Brewery Bronze-to-Silver Validation"
            )

            check = (
                check.isComplete("id")
                .isUnique("id")
                .isComplete("state")
                .isComplete("country")
                .isComplete("brewery_type")
                .hasDataType("id", ConstrainableDataTypes("String"))
                .hasDataType("state", ConstrainableDataTypes("String"))
                .hasDataType("country", ConstrainableDataTypes("String"))
                .hasDataType("brewery_type", ConstrainableDataTypes("String"))
                .hasDataType("latitude", ConstrainableDataTypes("Numeric"))
                .hasDataType("longitude", ConstrainableDataTypes("Numeric"))
                .satisfies(
                    "latitude >= -90 AND latitude <= 90",
                    "Latitude within range",
                    lambda col: col >= -90 and col <= 90,
                )
                .satisfies(
                    "longitude >= -180 AND longitude <= 180",
                    "Longitude within range",
                    lambda col: col >= -180 and col <= 180,
                )
            )

            results = (
                VerificationSuite(self.spark).onData(self.df).addCheck(check).run()
            )

            has_failure = False
            for result in results.checkResults:
                if result["constraint_status"] == "Failure":
                    has_failure = True
                    logging.error(
                        f"Error on constraint '{result['constraint']}': {result['constraint_message']}"
                    )

            if has_failure:
                raise AssertionError(
                    "One or more constraints failed during validation."
                )

            logging.info("All validation checks passed successfully.")

        except Exception as e:
            logging.error(f"Unexpected error during validation: {e}", exc_info=True)
            raise
