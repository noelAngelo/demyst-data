from faker import Faker
from loguru import logger
from models.file import File
from src.utils import to_csv


fake = Faker("en_AU")  # set localization to Australia


def generate_csv_file(n_rows: int, path: str, header=True, seed=0):
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
    to_csv(values=data, csv_path=path, header=header)


class MockRow:
    """
    Mock class for simulating a pandas row-like object that has a `to_dict()` method.
    """

    def __init__(self, data):
        self.data = data

    def to_dict(self):
        return self.data
