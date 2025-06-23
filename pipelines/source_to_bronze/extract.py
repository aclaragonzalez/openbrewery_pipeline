import logging
import requests
import backoff
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException

# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class BreweryExtractor:
    """
    A class to extract brewery data from the Open Brewery DB API.
    """

    BASE_URL = "https://api.openbrewerydb.org/v1/breweries"

    @backoff.on_exception(
        backoff.expo,
        (HTTPError, ConnectionError, Timeout, RequestException),
        max_tries=5,
        jitter=backoff.full_jitter,
        on_backoff=lambda details: logging.warning(
            f"Backing off {details['wait']:0.1f}s after {details['tries']} tries "
            f"calling function {details['target'].__name__} with args {details['args']} and kwargs {details['kwargs']}"
        ),
    )
    def fetch(self, page: int, per_page: int = 200, **kwargs):
        """
        Fetches brewery data from the API for a specific page.

        Parameters
        ----------
        page : int
            The page number to fetch.
        per_page : int, optional
            Number of records to fetch per page (default is 200).
        **kwargs : dict
            Additional query parameters supported by the API (e.g., by_city, by_state, etc.).

        Returns
        -------
        list or None
            A list of brewery data in JSON format if the request is successful,
            or None if an error occurs.
        """
        params = {"page": page, "per_page": per_page, **kwargs}
        try:
            response = requests.get(url=self.BASE_URL, params=params, timeout=(10, 30))

            response.raise_for_status()

            try:
                return response.json()
            except ValueError as e:
                logging.error("Failed to parse response as JSON.")
                raise e

        except HTTPError as http_err:
            logging.error(f"HTTP error occurred while accessing the API: {http_err}")
            raise http_err
        except ConnectionError:
            logging.error(
                "Connection error. Please check your internet connection or the API URL."
            )
            raise
        except Timeout:
            logging.error("The request timed out.")
            raise
        except RequestException as req_err:
            logging.error(f"An unexpected error occurred during the request: {req_err}")
            raise
