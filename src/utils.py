from loguru import logger
from pathlib import Path
from functools import wraps
import time
import csv
from loguru import logger


def to_csv(values: list[dict], csv_path, encoding="utf-8", header=True):
    """
    Creates a CSV file from a list of dictionaries.

    This function takes a list of dictionaries and writes its content to a CSV file.
    Each dictionary in the list represents a row, and the keys of the dictionaries
    represent the column names in the CSV file. The CSV file is created at the specified
    file path. If the file path doesn't exist, it will be created.

    Parameters:
    ----------
    values : list[dict]
        A list of dictionaries, where each dictionary represents a row in the CSV file.
        The keys of the first dictionary are used as the fieldnames (column headers).

    csv_path : str or Path
        The file path where the CSV file will be saved. If the parent directory does
        not exist, it will be created automatically.

    encoding : str, optional
        The encoding used for writing the CSV file. Defaults to "utf-8".

    header : bool, optional
        If True (default), writes a header row using the dictionary keys as column names.
        If False, only the data rows are written.

    Returns:
    --------
    None
        This function does not return any value. It writes data to the specified CSV file.

    Behavior:
    ---------
    - If the `values` list is empty, a warning will be logged, and the function will exit
      without creating a file.
    - The function creates any necessary parent directories for the CSV file if they don't exist.
    - The first dictionary's keys will be used as the column headers.
    - The CSV is written with the specified encoding and supports appending or overwriting
      the file depending on the mode.

    Example:
    --------
    values = [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Bob", "age": 25, "city": "San Francisco"}
    ]

    to_csv(values, "output/data.csv")

    This will create a `data.csv` file with the following content:

    name,age,city
    Alice,30,New York
    Bob,25,San Francisco
    """

    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    if not values:
        logger.warning("Value list is empty. No data to write")
        return

    fieldnames = values[0].keys()

    # Write to CSV
    with csv_path.open(mode="w+", encoding=encoding) as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if header:
            writer.writeheader()  # write the header row
        writer.writerows(values)  # write the values
