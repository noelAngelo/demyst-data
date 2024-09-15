from src.utils import generate_csv_file
from pathlib import Path
from loguru import logger
import csv


def test_generate_file_with_header_success():
    """
    Test utility function to generate a CSV file that has headers
    """

    # Configurations
    tox_dir = Path.cwd()
    output_dir = f"{tox_dir}/tests/output"
    filename = "test_generate_file_success.csv"
    n_rows = 50
    path = Path(f"{output_dir}/{filename}")

    # Show logs
    logger.debug(f"Current working directory: {tox_dir}")

    try:
        # Run generator
        generate_csv_file(n_rows=n_rows, path=path)

        # Assert the file was created
        assert path.is_file(), f"File {path} does not exist"

        # Assert 50 rows were generated
        rows = []
        with path.open(mode="r") as file:
            reader = csv.reader(file)
            for row in reader:
                rows.append(row)
        assert len(rows) == n_rows + 1  # includes header

    finally:
        # remove the file
        # path.unlink(missing_ok=True)
        pass


def test_generate_file_without_header_success():
    """
    Test utility function to generate a CSV file without any headers
    """

    # Configurations
    tox_dir = Path.cwd()
    output_dir = f"{tox_dir}/tests/output"
    filename = "test_generate_file_without_header_success.csv"
    n_rows = 50
    path = Path(f"{output_dir}/{filename}")

    # Show logs
    logger.debug(f"Current working directory: {tox_dir}")

    try:
        # Run generator
        generate_csv_file(n_rows=n_rows, path=path, header=False)

        # Assert the file was created
        assert path.is_file(), f"File {path} does not exist"

        # Assert 50 rows were generated
        rows = []
        with path.open(mode="r") as file:
            reader = csv.reader(file)
            for row in reader:
                rows.append(row)
        assert len(rows) == n_rows  # excluding header

    finally:
        # remove the file
        path.unlink(missing_ok=True)


def test_generate_file_without_data():
    """
    Test utility function to not generate a CSV file without any data
    """

    # Configurations
    tox_dir = Path.cwd()
    output_dir = f"{tox_dir}/tests/output"
    filename = "test_generate_file_without_data.csv"
    n_rows = 0
    path = Path(f"{output_dir}/{filename}")

    # Show logs
    logger.debug(f"Current working directory: {tox_dir}")

    # Run generator
    generate_csv_file(n_rows=n_rows, path=path, header=False)

    # Assert the file was created
    assert not path.is_file(), f"File {path} was created"
