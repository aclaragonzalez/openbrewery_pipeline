import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as f

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class BreweryTransformer:
    """
    Transformer class to aggregate brewery data by country, state, and brewery type,
    calculating distinct brewery counts.
    """

    def apply(self, df: DataFrame) -> DataFrame:
        """
        Aggregates the input DataFrame grouping by 'country', 'state', and 'brewery_type',
        counting distinct 'id' values as 'brewery_count'.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            Input DataFrame with brewery data.

        Returns
        -------
        pyspark.sql.DataFrame
            Aggregated DataFrame with brewery counts.

        Raises
        ------
        ValueError
            If required columns are missing from the input DataFrame.
        Exception
            For any unexpected errors during transformation.
        """
        required_columns = {"country", "state", "brewery_type", "id"}
        missing_cols = required_columns - set(df.columns)
        if missing_cols:
            error_msg = f"Input DataFrame is missing required columns: {missing_cols}"
            logging.error(error_msg)
            raise ValueError(error_msg)

        try:
            df_refined = df.groupBy("country", "state", "brewery_type").agg(
                f.countDistinct("id").alias("brewery_count")
            )

            return df_refined
        except Exception as e:
            logging.error(
                f"Error during aggregation transformation: {e}", exc_info=True
            )
            raise
