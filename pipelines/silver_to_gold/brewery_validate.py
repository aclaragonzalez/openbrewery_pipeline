import os
import logging

os.environ["SPARK_VERSION"] = "3.5"

from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
from pydeequ.verification import VerificationSuite
from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class BreweryValidator:
    """
    Validator class that uses PyDeequ to perform data quality checks
    on a brewery DataFrame during the Silver-to-Gold layer transition.
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

        Checks completeness and data types of relevant columns.

        Raises
        ------
        AssertionError
            If one or more constraints fail during validation.
        Exception
            If an unexpected error occurs during validation.
        """
        try:
            check = Check(
                self.spark, CheckLevel.Error, "Brewery Silver-to-Gold Validation"
            )

            check = (
                check.isComplete("state")
                .isComplete("country")
                .isComplete("brewery_type")
                .hasDataType("state", ConstrainableDataTypes("String"))
                .hasDataType("country", ConstrainableDataTypes("String"))
                .hasDataType("brewery_type", ConstrainableDataTypes("String"))
                .hasDataType("brewery_count", ConstrainableDataTypes("Numeric"))
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

        except AssertionError:
            raise
        except Exception as e:
            logging.error(f"Unexpected error during validation: {e}", exc_info=True)
            raise
