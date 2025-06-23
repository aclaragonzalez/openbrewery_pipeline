import unittest
from unittest.mock import patch, MagicMock
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import sys
import os

# Add the project root to sys.path to allow module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pipelines.source_to_bronze.extract import (
    BreweryExtractor,
)


class TestBreweryExtractor(unittest.TestCase):
    """
    Unit tests for the BreweryExtractor class which fetches data from the Open Brewery API.
    """

    def setUp(self):
        """
        Set up a BreweryExtractor instance before each test.
        """
        self.extractor = BreweryExtractor()

    @patch("requests.get")
    def test_fetch_success(self, mock_get):
        """
        Test successful API call and correct parsing of JSON response.
        """
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = [{"id": 1, "name": "Brewery 1"}]
        mock_get.return_value = mock_response

        result = self.extractor.fetch(page=1)
        self.assertIsInstance(result, list)
        self.assertEqual(result[0]["name"], "Brewery 1")
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_fetch_json_parse_error(self, mock_get):
        """
        Test handling of ValueError when JSON parsing fails.
        """
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response

        with self.assertRaises(ValueError):
            self.extractor.fetch(page=1)

    @patch("requests.get")
    def test_fetch_http_error(self, mock_get):
        """
        Test handling of HTTPError when raise_for_status fails.
        """
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = HTTPError("404 Client Error")
        mock_get.return_value = mock_response

        with self.assertRaises(HTTPError):
            self.extractor.fetch(page=1)

    @patch("requests.get", side_effect=ConnectionError("Connection lost"))
    def test_fetch_connection_error(self, mock_get):
        """
        Test handling of ConnectionError during the API request.
        """
        with self.assertRaises(ConnectionError):
            self.extractor.fetch(page=1)

    @patch("requests.get", side_effect=Timeout("Timeout occurred"))
    def test_fetch_timeout_error(self, mock_get):
        """
        Test handling of Timeout error during the API request.
        """
        with self.assertRaises(Timeout):
            self.extractor.fetch(page=1)

    @patch("requests.get", side_effect=RequestException("Request exception"))
    def test_fetch_request_exception(self, mock_get):
        """
        Test handling of generic RequestException during the API request.
        """
        with self.assertRaises(RequestException):
            self.extractor.fetch(page=1)


if __name__ == "__main__":
    # Run all unit tests
    unittest.main()
