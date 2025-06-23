import logging
import pyspark.sql.functions as f
from pyspark.sql import DataFrame

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class BreweryTransformer:
    """
    Transformer class to clean and standardize brewery data in a PySpark DataFrame.

    This includes correcting inverted coordinates, validating coordinate ranges,
    and normalizing 'state' and 'country' fields.
    """

    def apply(self, df: DataFrame) -> DataFrame:
        """
        Applies transformation rules to the input DataFrame.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            Input DataFrame with brewery data containing latitude, longitude, state, and country columns.

        Returns
        -------
        pyspark.sql.DataFrame
            Transformed DataFrame with corrected coordinates and cleaned text fields.

        Raises
        ------
        ValueError
            If required columns are missing from the DataFrame.
        """
        required_columns = {"latitude", "longitude", "state", "country"}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            error_msg = (
                f"Input DataFrame is missing required columns: {missing_columns}"
            )
            logging.error(error_msg)
            raise ValueError(error_msg)
        try:
            # 1. Detect inverted coordinates
            cond1 = ((f.col("latitude") < -90) | (f.col("latitude") > 90)) & (
                f.col("longitude").between(-90, 90)
            )
            cond2 = ((f.col("longitude") < -180) | (f.col("longitude") > 180)) & (
                f.col("latitude").between(-180, 180)
            )
            cond_invertida = cond1 | cond2

            # 2. Correct inverted coordinates
            latitude_corrigida = f.when(cond_invertida, f.col("longitude")).otherwise(
                f.col("latitude")
            )
            longitude_corrigida = f.when(cond_invertida, f.col("latitude")).otherwise(
                f.col("longitude")
            )

            # 3. Validate ranges after correction; set to NULL if invalid
            latitude_final = f.when(
                (latitude_corrigida >= -90) & (latitude_corrigida <= 90),
                latitude_corrigida,
            ).otherwise(f.lit(None))
            longitude_final = f.when(
                (longitude_corrigida >= -180) & (longitude_corrigida <= 180),
                longitude_corrigida,
            ).otherwise(f.lit(None))

            # 4. Apply transformations and clean 'state' and 'country'
            df_transformed = (
                df.withColumn("latitude", latitude_final)
                .withColumn("longitude", longitude_final)
                .withColumn(
                    "state",
                    f.initcap(  # Capitalize each word
                        f.regexp_replace(
                            "state", r"[^a-zA-Z0-9\s]", ""
                        )  # Remove special characters
                    ),
                )
                .withColumn("state", f.regexp_replace(f.trim("state"), " +", ""))
                .withColumn("country", f.regexp_replace(f.trim("country"), " +", ""))
                .withColumn("country", f.coalesce("country", f.lit("unknown")))
                .withColumn("state", f.coalesce("state", f.lit("unknown")))
            )

            logging.info("Transformation applied successfully.")
            return df_transformed

        except Exception as e:
            logging.error(f"Error applying transformation: {e}", exc_info=True)
            raise
