import unittest
from src.processor.file import anonymise_dataframe
from unittest.mock import patch
from models.file import File
from loguru import logger
import pandas as pd
import time
import ray
import sys

class TestStressLoad(unittest.TestCase):

    @patch('src.processor.file.anonymise')
    def test_anonymise_columns_on_50_rows(self, mock_anonymise):
        
         # Arrange
        mock_anonymise.side_effect = lambda row: {"anonymised_data": f"hashed_{row['first_name']}"}


        # Create a DataFrame with 50 rows
        df = pd.DataFrame({
            "first_name": [f"Person_{i}" for i in range(50)],
            "last_name": [f"Last_{i}" for i in range(50)],
            "date_of_birth": [f"1990-01-{i+1:02d}" for i in range(50)],
            "address": [f"{i} Main St" for i in range(50)]
        })

        # Act
        start_time = time.time()
        result = anonymise_dataframe(df, parallel=False)
        end_time = time.time()

        # Assert
        expected = pd.DataFrame([{"anonymised_data": f"hashed_Person_{i}"} for i in range(50)])
        pd.testing.assert_frame_equal(result, expected)
        logger.info(f"Non-parallel execution time: {end_time - start_time} seconds")

    def test_anonymise_dataframe_stress_test_2gb_parallel(self):
        # Initialize Ray
        ray.init(ignore_reinit_error=True)  # Ensure Ray is initialized

        # Mocking a file object with personal information
        file = File(
            first_name="John",
            last_name="Doe",
            date_of_birth="1990-01-01",
            address="123 Main St",
        )

        try:
            # Arrange
            average_row_size_bytes = sys.getsizeof(file)  # Approximate size per row (bytes)
            target_size_gb = 2  # We want to generate 2 GB of data
            target_size_bytes = target_size_gb * (1024 ** 3)
            num_rows = target_size_bytes // average_row_size_bytes
            logger.info(f"Simulating a DataFrame with rows: {num_rows:,.2f}")

            # Creating a DataFrame of this size
            df = pd.DataFrame({
                "first_name": [f"Person_{i}" for i in range(num_rows)],
                "last_name": [f"Last_{i}" for i in range(num_rows)],
                "date_of_birth": [f"1990-01-{(i % 30) + 1:02d}" for i in range(num_rows)],
                "address": [f"{i} Main St" for i in range(num_rows)]
            })

            # Act
            start_time = time.time()  # Track performance
            result = anonymise_dataframe(df, num_splits=4, parallel=True, ray=ray)
            end_time = time.time()

            # Assert
            # We're not asserting on content here since the focus is on stress testing and performance
            self.assertEqual(len(result), len(df))  # Ensure all rows are processed
            logger.info(f"Parallel execution time for 2GB DataFrame: {end_time - start_time:.2f} seconds")

        finally:
            # Shutdown Ray after the test
            ray.shutdown()

    def test_anonymise_columns_on_5gb_rows(self):
        # Initialize Ray
        ray.init(ignore_reinit_error=True)  # Ensure Ray is initialized

        # Mocking a file object with personal information
        file = File(
            first_name="John",
            last_name="Doe",
            date_of_birth="1990-01-01",
            address="123 Main St",
        )

        try:
            # Arrange
            average_row_size_bytes = sys.getsizeof(file)  # Approximate size per row (bytes)
            target_size_gb = 3  # We want to generate 3 GB of data
            target_size_bytes = target_size_gb * (1024 ** 3)
            num_rows = target_size_bytes // average_row_size_bytes
            logger.info(f"Simulating a DataFrame with rows: {num_rows:,.2f}")

            # Creating a DataFrame of this size
            df = pd.DataFrame({
                "first_name": [f"Person_{i}" for i in range(num_rows)],
                "last_name": [f"Last_{i}" for i in range(num_rows)],
                "date_of_birth": [f"1990-01-{(i % 30) + 1:02d}" for i in range(num_rows)],
                "address": [f"{i} Main St" for i in range(num_rows)]
            })

            # Act
            start_time = time.time()  # Track performance
            result = anonymise_dataframe(df, num_splits=4, parallel=True, ray=ray)
            end_time = time.time()

            # Assert
            # We're not asserting on content here since the focus is on stress testing and performance
            self.assertEqual(len(result), len(df))  # Ensure all rows are processed
            logger.info(f"Parallel execution time for 2GB DataFrame: {end_time - start_time:.2f} seconds")

        finally:
            # Shutdown Ray after the test
            ray.shutdown()

