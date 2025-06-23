import logging
from abc import ABC, abstractmethod

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class BaseTransformer(ABC):
    """
    Abstract base class for ETL transformers defining
    the extraction, transformation, validation, and loading steps.
    """

    def run(self):
        """
        Runs the ETL pipeline: extract, transform, validate, and load data.
        """
        try:
            df = self.extract()
            df = self.transform(df)
            self.validate(df)
            self.load(df)

            logging.info("ETL pipeline completed successfully.")

        except Exception as e:
            logging.error(f"ETL pipeline failed: {e}", exc_info=True)
            raise

    @abstractmethod
    def extract(self):
        """Extract data"""
        pass

    @abstractmethod
    def transform(self, df):
        """Transform data"""
        pass

    @abstractmethod
    def validate(self, df):
        """Validate data"""
        pass

    @abstractmethod
    def load(self, df):
        """Load data to the target system"""
        pass
