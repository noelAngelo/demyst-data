import unittest
from src.processor.file import anonymise_dataframe
from unittest.mock import patch
from models.file import File
from loguru import logger
from pathlib import Path
from src.utils import generate_csv_file
import pandas as pd
import time
import ray
import sys


def create_chunk_sizes(num_rows, chunk_size):
    """
    Creates a list of chunk sizes where each element represents the number of rows for a chunk.
    The last chunk may have fewer rows if num_rows is not divisible by chunk_size.
    """
    chunks = [chunk_size] * (num_rows // chunk_size)  # List of evenly sized chunks
    remainder = num_rows % chunk_size

    if remainder > 0:
        chunks.append(remainder)  # Add the remainder as the last chunk

    return chunks


class TestStressLoad(unittest.TestCase):

    @patch("src.processor.file.anonymise")
    def test_anonymise_columns_on_50_rows(self, mock_anonymise):

        # Arrange
        mock_anonymise.side_effect = lambda row: {
            "anonymised_data": f"hashed_{row['first_name']}"
        }

        # Create a DataFrame with 50 rows
        df = pd.DataFrame(
            {
                "first_name": [f"Person_{i}" for i in range(50)],
                "last_name": [f"Last_{i}" for i in range(50)],
                "date_of_birth": [f"1990-01-{i + 1:02d}" for i in range(50)],
                "address": [f"{i} Main St" for i in range(50)],
            }
        )

        # Act
        start_time = time.time()
        result = anonymise_dataframe(df, parallel=False)
        end_time = time.time()

        # Assert
        expected = pd.DataFrame(
            [{"anonymised_data": f"hashed_Person_{i}"} for i in range(50)]
        )
        pd.testing.assert_frame_equal(result, expected)
        logger.info(f"Non-parallel execution time: {end_time - start_time} seconds")

    def test_anonymise_dataframe_stress_test_2gb_parallel_chunking(self):
        # Initialize Ray
        ray.init(ignore_reinit_error=True)  # Ensure Ray is initialized

        # Mocking a file object with personal information
        file = File(
            first_name="John",
            last_name="Doe",
            date_of_birth="1990-01-01",
            address="123 Main St",
        )

        # Define the chunk size (number of rows per chunk)
        chunk_size = 10 ** 6  # 1 million rows per chunk

        try:
            # Arrange
            tox_dir = Path.cwd()
            output_dir = f"{tox_dir}/tests/output"
            filename = "large_file.csv"  # Path to a temporary CSV file
            csv_file = Path(f"{output_dir}/{filename}")
            average_row_size_bytes = sys.getsizeof(
                file
            )  # Approximate size per row (bytes)
            target_size_gb = 2  # We want to generate 2 GB of data
            target_size_bytes = target_size_gb * (1024 ** 3)
            num_rows = target_size_bytes // average_row_size_bytes
            logger.info(f"Simulating a DataFrame with rows: {num_rows:,.2f}")
            chunk_sizes = create_chunk_sizes(num_rows, chunk_size)
            for chunk in chunk_sizes:
                generate_csv_file(n_rows=chunk, path=csv_file, header=False, mode="a+")

            # Act
            start_time = time.time()  # Track performance

            # Reading and anonymizing the DataFrame in chunks
            for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
                result = anonymise_dataframe(chunk, num_splits=4, parallel=True, ray=ray)
                # Optionally write anonymized chunk to a new CSV (or process further)
                result.to_csv("anonymized_file.csv", mode="a", index=False, header=False)

            end_time = time.time()

            # Assert
            logger.info(
                f"Parallel execution time for 2GB DataFrame with chunking: {end_time - start_time:.2f} seconds"
            )

        finally:
            # Shutdown Ray after the test
            ray.shutdown()
