import os
import json
import logging
from datetime import datetime
from typing import Any

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def save_json_data(
    data: Any, layer: str, path: str, filename: str, format: str
) -> None:
    """
    Saves data to a specified path and filename with the given format.

    If the layer is 'bronze', it creates a subdirectory with the current UTC
    extraction datetime in the format 'dt_extraction=YYYY-MM-DDTHH-MM'.

    Parameters
    ----------
    data : Any
        The data to save. Should be JSON serializable.
    path : str
        Base directory path where data will be saved.
    filename : str
        Name of the file (without extension).
    format : str
        File format/extension (e.g., 'json').

    Raises
    ------
    OSError
        If there is an error creating directories or writing the file.
    TypeError
        If data is not serializable to JSON.
    """
    if layer == "bronze":
        dt_extraction = datetime.utcnow().strftime("%Y-%m-%dT%H-%M")
        dir_path = os.path.join(path, f"dt_extraction={dt_extraction}")
        filename = os.path.join(dir_path, f"{filename}.{format}")
    else:
        dir_path = path
        filename = os.path.join(path, f"{filename}.{format}")

    try:
        os.makedirs(dir_path, exist_ok=True)

        with open(filename, "w") as f:
            json.dump(data, f, indent=4)
        logging.info(f"File saved at: {os.path.abspath(filename)}")

    except OSError as e:
        logging.error(f"Error creating directory or writing file: {e}", exc_info=True)
        raise
    except TypeError as e:
        logging.error(f"Data serialization error: {e}", exc_info=True)
        raise
