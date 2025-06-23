import unittest
from unittest.mock import patch, mock_open
from freezegun import freeze_time
import sys
import os

# Add the project root to sys.path to allow module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pipelines.load import save_json_data


class TestSaveJsonData(unittest.TestCase):
    """
    Unit tests for the save_json_data function, which writes JSON files
    to the appropriate Data Lake directory based on layer and timestamp.
    """

    def setUp(self):
        """
        Patch logging methods before each test to suppress output and allow verification.
        """
        self.log_patcher = patch("logging.info")
        self.mock_log_info = self.log_patcher.start()

        self.log_error_patcher = patch("logging.error")
        self.mock_log_error = self.log_error_patcher.start()

    def tearDown(self):
        """
        Stop all patches after each test.
        """
        self.log_patcher.stop()
        self.log_error_patcher.stop()

    @freeze_time("2025-06-22 15:30:00")
    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    def test_save_in_bronze_layer_creates_subdir_and_saves_file(
        self, mock_open_file, mock_makedirs
    ):
        """
        Test that saving to the bronze layer creates a timestamped subdirectory
        and writes the file correctly. Also verifies logging.
        """
        data = {"key": "value"}
        layer = "bronze"
        path = "/datalake/bronze/brewery"
        filename = "file1"
        format = "json"

        save_json_data(data, layer, path, filename, format)

        expected_dir = "/datalake/bronze/brewery\\dt_extraction=2025-06-22T15-30"
        mock_makedirs.assert_called_once_with(expected_dir, exist_ok=True)
        mock_open_file.assert_called_once()

        # Check if file write was triggered
        handle = mock_open_file()
        handle.write.assert_called()

        # Check log message for correct path
        expected_file_path = (
            "C:\\datalake\\bronze\\brewery\\dt_extraction=2025-06-22T15-30\\file1.json"
        )
        self.mock_log_info.assert_called_with(f"File saved at: {expected_file_path}")

    @freeze_time("2025-06-22 15:30:00")
    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    def test_save_in_other_layer_saves_directly(self, mock_open_file, mock_makedirs):
        """
        Test that saving in a non-bronze layer also creates the timestamped
        subdirectory and writes the file.
        """
        data = {"key": "value"}
        layer = "bronze"  # Should still trigger dt_extraction subdir creation
        path = "datalake\\silver\\brewery"
        filename = "file2"
        format = "json"

        save_json_data(data, layer, path, filename, format)

        expected_filepath = f"{path}\\dt_extraction=2025-06-22T15-30"
        mock_makedirs.assert_called_once_with(expected_filepath, exist_ok=True)
        mock_open_file.assert_called_once()

    @patch("os.makedirs", side_effect=OSError("cannot create dir"))
    def test_os_error_during_makedirs_raises_and_logs(self, mock_makedirs):
        """
        Test that an OSError during directory creation is raised and logged.
        """
        data = {"key": "value"}
        layer = "bronze"
        path = "/datalake/silver/brewery"
        filename = "file"
        format = "json"

        with self.assertRaises(OSError):
            save_json_data(data, layer, path, filename, format)

        # Ensure error was logged
        self.mock_log_error.assert_called()
        call_args = self.mock_log_error.call_args[0][0]
        self.assertIn("Error creating directory or writing file", call_args)

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    def test_type_error_during_json_dump_raises_and_logs(
        self, mock_open_file, mock_makedirs
    ):
        """
        Test that a TypeError during JSON serialization is raised and logged.
        """

        class NonSerializable:
            pass

        data = NonSerializable()
        layer = "bronze"
        path = "datalake/silver/brewery"
        filename = "file"
        format = "json"

        with self.assertRaises(TypeError):
            save_json_data(data, layer, path, filename, format)

        # Ensure serialization error was logged
        self.mock_log_error.assert_called()
        call_args = self.mock_log_error.call_args[0][0]
        self.assertIn("Data serialization error", call_args)


if __name__ == "__main__":
    # Run all unit tests
    unittest.main()
