import os
import re
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def get_latest_extraction_path(path: str, filename: str = None) -> str:
    """
    Returns the path of the folder (or file if `filename` is provided) with the most recent dt_extraction timestamp.

    Parameters
    ----------
    path : str
        Directory where folders with the prefix dt_extraction= are located.
    filename : str, optional
        Name of the file to be returned inside the most recent folder.

    Returns
    -------
    str
        Full path of the folder or file with the most recent dt_extraction timestamp.

    Raises
    ------
    FileNotFoundError
        If no folder matching the dt_extraction= pattern is found in the given directory.
    """
    pattern = r"dt_extraction=(\d{4}-\d{2}-\d{2}T\d{2}-\d{2})"
    latest_time = None
    latest_path = None

    try:
        entries = os.listdir(path)
    except FileNotFoundError as e:
        logging.error(f"Directory not found: {path}")
        raise e
    except PermissionError as e:
        logging.error(f"Permission denied to access directory: {path}")
        raise e
    except Exception as e:
        logging.error(
            f"Unexpected error accessing directory {path}: {e}", exc_info=True
        )
        raise e

    for entry in entries:
        match = re.match(pattern, entry)
        if match:
            try:
                extraction_time = datetime.strptime(match.group(1), "%Y-%m-%dT%H-%M")
            except ValueError:
                logging.warning(
                    f"Skipping folder with invalid datetime format: {entry}"
                )
                continue

            if latest_time is None or extraction_time > latest_time:
                latest_time = extraction_time
                latest_path = os.path.join(path, entry)

    if latest_path is None:
        error_msg = "No folder matching the pattern 'dt_extraction=' was found."
        logging.error(error_msg)
        raise FileNotFoundError(error_msg)

    final_path = os.path.join(latest_path, filename) if filename else latest_path
    logging.info(f"Latest extraction path found: {final_path}")
    return final_path
