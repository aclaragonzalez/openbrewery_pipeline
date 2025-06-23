import unittest
from unittest.mock import patch, MagicMock, mock_open
import yaml
import sys
import os

# Add the project root to sys.path to allow module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pipelines.utils import (
    get_paginated_data,
    load_schema,
)


class TestUtils(unittest.TestCase):
    """
    Unit tests for utility functions in pipelines.utils: get_paginated_data and load_schema.
    """

    @patch("logging.info")
    @patch("logging.error")
    def test_get_paginated_data_success(self, mock_log_error, mock_log_info):
        """
        Test that get_paginated_data correctly fetches and concatenates paginated results.
        """
        mock_extractor = MagicMock()
        mock_extractor.fetch.side_effect = [
            [{"id": 1}, {"id": 2}],
            [{"id": 3}],
            [],
        ]

        result = get_paginated_data(mock_extractor)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["id"], 1)
        mock_extractor.fetch.assert_called_with(3)

    @patch("logging.error")
    def test_get_paginated_data_exception(self, mock_log_error):
        """
        Test that get_paginated_data logs an error and returns an empty list on exception.
        """
        mock_extractor = MagicMock()
        mock_extractor.fetch.side_effect = Exception("API error")

        result = get_paginated_data(mock_extractor)
        self.assertEqual(result, [])
        mock_log_error.assert_called()

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=yaml.dump(
            {"bronze": {"my_table": {"fields": {"name": "string", "age": "integer"}}}}
        ),
    )
    def test_load_schema_success(self, mock_file):
        """
        Test that load_schema correctly loads a schema with valid fields and types.
        """
        schema = load_schema("bronze", "my_table")
        self.assertEqual(len(schema.fields), 2)
        self.assertEqual(schema.fields[0].name, "age")

    @patch("builtins.open", side_effect=FileNotFoundError)
    @patch("logging.error")
    def test_load_schema_file_not_found(self, mock_log_error, mock_open_fn):
        """
        Test that load_schema raises and logs FileNotFoundError when YAML file is missing.
        """
        with self.assertRaises(FileNotFoundError):
            load_schema("bronze", "nonexistent")
        mock_log_error.assert_called_with("Configuration file 'tables.yaml' not found.")

    @patch("builtins.open", new_callable=mock_open, read_data="invalid_yaml: [")
    @patch("yaml.safe_load", side_effect=yaml.YAMLError("bad format"))
    @patch("logging.error")
    def test_load_schema_yaml_error(self, mock_log_error, _, mock_file):
        """
        Test that load_schema raises and logs a YAMLError when the file has invalid syntax.
        """
        with self.assertRaises(yaml.YAMLError):
            load_schema("bronze", "my_table")
        self.assertIn("Error parsing YAML file", mock_log_error.call_args[0][0])

    @patch("builtins.open", new_callable=mock_open, read_data=yaml.dump({"bronze": {}}))
    @patch("logging.error")
    def test_load_schema_key_error(self, mock_log_error, mock_file):
        """
        Test that load_schema raises and logs a KeyError when a table is not found in config.
        """
        with self.assertRaises(KeyError):
            load_schema("bronze", "missing_table")
        self.assertIn(
            "Missing key in schema configuration", mock_log_error.call_args[0][0]
        )

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=yaml.dump(
            {"bronze": {"my_table": {"fields": {"unknown": "unknown"}}}}
        ),
    )
    @patch("logging.error")
    def test_load_schema_invalid_dtype(self, mock_log_error, mock_file):
        """
        Test that load_schema raises and logs an AttributeError when data type is invalid.
        """
        with self.assertRaises(AttributeError):
            load_schema("bronze", "my_table")
        self.assertIn(
            "Invalid data type in schema configuration", mock_log_error.call_args[0][0]
        )


if __name__ == "__main__":
    # Run all unit tests
    unittest.main()
