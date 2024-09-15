from pathlib import Path
import csv
from faker import Faker
from loguru import logger
from models.file import File


def to_csv(values: list[dict], csv_path, header=True, **kwargs):
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
    with csv_path.open(**kwargs) as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if header:
            writer.writeheader()  # write the header row
        writer.writerows(values)  # write the values


fake = Faker("en_AU")  # set localization to Australia


def generate_csv_file(n_rows: int, path: str, header=True, seed=0, mode="w"):
    """
    Generates a CSV file with fake data for testing purposes.

    This function generates `n_rows` rows of fake data, including first name, last name, address,
    and date of birth, using the `Faker` library localized to Australian data formats. The generated
    data is written to a CSV file at the specified `path`.

    Parameters:
    -----------
    n_rows : int
        The number of rows of fake data to generate.

    path : str
        The file path where the generated CSV file will be saved. The function will create
        any necessary parent directories.

    header : bool, optional
        If True (default), writes a header row with column names ("first_name", "last_name", "address",
        and "date_of_birth") to the CSV file. If False, only the data rows are written.

    Behavior:
    ---------
    - Logs the number of rows to be generated and the file path where the CSV will be saved.
    - For each row, generates a `File` object containing:
        - `first_name`: A random first name.
        - `last_name`: A random last name.
        - `address`: A randomly generated address with newlines replaced by commas.
        - `date_of_birth`: A randomly generated date of birth.
    - Converts each `File` object into a dictionary of values and logs the generated data for debugging.
    - Writes the generated data to a CSV file using the `to_csv` utility function.

    Returns:
    --------
    None
        This function does not return any value. It generates the specified number of rows and writes
        them to a CSV file.

    Example:
    --------
    generate_csv_file(n_rows=100, path="output/fake_data.csv")

    This will create a `fake_data.csv` file with 100 rows of randomly generated fake data, including
    headers for each column if `header=True`.
    """

    Faker.seed(seed)  # add a seed to get consistency in random generation
    logger.info(f"Rows to generate: {n_rows}")
    logger.debug(f"Faker seed: {seed}")
    logger.debug(f"File path: {path}")

    # Generate data
    data = []
    for _ in range(n_rows):
        file = File(
            first_name=fake.first_name(),
            last_name=fake.last_name(),
            address=fake.address().replace("\n", ", "),  # replace new lines with commas
            date_of_birth=fake.date_of_birth(),
        )
        values = file.values()
        logger.debug(f"Generated data: {values}")
        data.append(values)

    # Write to CSV
    to_csv(values=data, csv_path=path, header=header, mode=mode, encoding="utf-8")
