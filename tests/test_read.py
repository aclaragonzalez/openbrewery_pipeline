import unittest
from unittest.mock import patch
import sys
import os

# Add the project root to sys.path to allow module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pipelines.read import (
    get_latest_extraction_path,
)


class TestGetLatestExtractionPath(unittest.TestCase):
    """
    Unit tests for the get_latest_extraction_path function, which identifies
    the most recent dt_extraction directory in a given path and returns its full path.
    """

    def setUp(self):
        """
        Set up test variables and patch logging functions before each test.
        """
        self.path = "/fake/path"
        self.filename = "data.json"

        self.log_info_patcher = patch("logging.info")
        self.mock_log_info = self.log_info_patcher.start()

        self.log_error_patcher = patch("logging.error")
        self.mock_log_error = self.log_error_patcher.start()

        self.log_warning_patcher = patch("logging.warning")
        self.mock_log_warning = self.log_warning_patcher.start()

    def tearDown(self):
        """
        Stop all logging patches after each test.
        """
        self.log_info_patcher.stop()
        self.log_error_patcher.stop()
        self.log_warning_patcher.stop()

    @patch("os.listdir")
    def test_returns_latest_folder_path(self, mock_listdir):
        """
        Test that the latest dt_extraction folder is selected based on datetime.
        """
        mock_listdir.return_value = [
            "dt_extraction=2024-01-01T10-00",
            "dt_extraction=2024-01-01T12-00",
            "dt_extraction=2023-12-31T23-59",
        ]

        expected = "/fake/path\\dt_extraction=2024-01-01T12-00"
        result = get_latest_extraction_path(self.path)

        self.assertEqual(result, expected)
        self.mock_log_info.assert_called_with(
            f"Latest extraction path found: {expected}"
        )

    @patch("os.listdir")
    def test_returns_latest_file_path_when_filename_provided(self, mock_listdir):
        """
        Test that the function returns the full file path if a filename is provided.
        """
        mock_listdir.return_value = [
            "dt_extraction=2024-01-01T08-00",
            "dt_extraction=2024-01-01T09-00",
        ]
        expected = "/fake/path\\dt_extraction=2024-01-01T09-00\\data.json"
        result = get_latest_extraction_path(self.path, filename="data.json")

        self.assertEqual(result, expected)
        self.mock_log_info.assert_called_with(
            f"Latest extraction path found: {expected}"
        )

    @patch("os.listdir")
    def test_ignores_invalid_dt_format_folders(self, mock_listdir):
        """
        Test that folders with invalid dt_extraction formats are ignored.
        """
        mock_listdir.return_value = [
            "not_a_valid_folder",
            "dt_extraction=invalid-date",
            "dt_extraction=2024-01-01T07-00",
        ]

        expected = "/fake/path\\dt_extraction=2024-01-01T07-00"
        result = get_latest_extraction_path(self.path)

        self.assertEqual(result, expected)

    @patch("os.listdir", side_effect=FileNotFoundError("directory missing"))
    def test_raises_file_not_found_and_logs(self, mock_listdir):
        """
        Test that a FileNotFoundError is raised and logged if the path does not exist.
        """
        with self.assertRaises(FileNotFoundError):
            get_latest_extraction_path(self.path)
        self.mock_log_error.assert_called_with(f"Directory not found: {self.path}")

    @patch("os.listdir", side_effect=PermissionError("access denied"))
    def test_raises_permission_error_and_logs(self, mock_listdir):
        """
        Test that a PermissionError is raised and logged when access is denied.
        """
        with self.assertRaises(PermissionError):
            get_latest_extraction_path(self.path)
        self.mock_log_error.assert_called_with(
            f"Permission denied to access directory: {self.path}"
        )

    @patch("os.listdir", side_effect=Exception("unexpected"))
    def test_raises_generic_exception_and_logs(self, mock_listdir):
        """
        Test that unexpected exceptions are raised and properly logged.
        """
        with self.assertRaises(Exception):
            get_latest_extraction_path(self.path)
        self.mock_log_error.assert_called()
        self.assertIn(
            "Unexpected error accessing directory", self.mock_log_error.call_args[0][0]
        )

    @patch("os.listdir")
    def test_raises_when_no_valid_folders_found(self, mock_listdir):
        """
        Test that FileNotFoundError is raised and logged when no valid folders are found.
        """
        mock_listdir.return_value = ["folder1", "other_folder"]

        with self.assertRaises(FileNotFoundError):
            get_latest_extraction_path(self.path)

        self.mock_log_error.assert_called_with(
            "No folder matching the pattern 'dt_extraction=' was found."
        )


if __name__ == "__main__":
    # Run all unit tests
    unittest.main()
