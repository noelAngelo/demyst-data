import unittest
import hashlib
from src.processor.file import (
    hash_data,
    anonymise_data,
    anonymise,
    anonymise_chunk,
    anonymise_dataframe,
)
from unittest.mock import patch
from pydantic_core import ValidationError
from models.file import File
from loguru import logger
import pandas as pd
import time
import ray
import sys


class TestHashData(unittest.TestCase):
    def test_hash_data(self):
        # Test case 1: Check hash of a simple string
        input_value = "example_string"
        expected_hash = hashlib.sha256(input_value.encode()).hexdigest()
        self.assertEqual(hash_data(input_value), expected_hash)

        # Test case 2: Check hash of an empty string
        input_value = ""
        expected_hash = hashlib.sha256(input_value.encode()).hexdigest()
        self.assertEqual(hash_data(input_value), expected_hash)

        # Test case 3: Check hash of a long string
        input_value = "a" * 1000
        expected_hash = hashlib.sha256(input_value.encode()).hexdigest()
        self.assertEqual(hash_data(input_value), expected_hash)


class TestAnonymiseData(unittest.TestCase):
    def test_anonymise_data(self):
        # Mocking a file object with personal information
        file = File(
            first_name="John",
            last_name="Doe",
            date_of_birth="1990-01-01",
            address="123 Main St",
        )

        # Expected hash values
        expected_data = {
            "first_name": hashlib.sha256("John".encode()).hexdigest(),
            "last_name": hashlib.sha256("Doe".encode()).hexdigest(),
            "date_of_birth": hashlib.sha256("1990-01-01".encode()).hexdigest(),
            "address": hashlib.sha256("123 Main St".encode()).hexdigest(),
        }

        # Call anonymise_data and compare with expected results
        self.assertEqual(anonymise_data(file), expected_data)

    def test_anonymise_data_empty_fields(self):

        with self.assertRaises(ValidationError) as context:
            # Test with empty fields to ensure it raises a validation error for missing data
            File(first_name="", last_name="", date_of_birth="", address=None)


class TestAnonymise(unittest.TestCase):

    def test_anonymise(self):
        file = File(
            first_name="John",
            last_name="Doe",
            date_of_birth="1990-01-01",
            address="123 Main St",
        )
        file_dict = file.values()
        df = pd.DataFrame([file_dict])
        row = df.iloc[0]
        result = anonymise(row)
        expected = anonymise_data(file)
        self.assertEqual(result, expected)


class TestAnonymiseFunctions(unittest.TestCase):

    @patch("src.processor.file.anonymise")  # Mock anonymise
    def test_anonymise_chunk(self, mock_anonymise):
        # Arrange
        mock_anonymise.side_effect = lambda row: {
            "anonymised_data": f"hashed_{row['first_name']}"
        }

        # Create a DataFrame chunk
        df_chunk = pd.DataFrame(
            {
                "first_name": ["John", "Jane"],
                "last_name": ["Doe", "Smith"],
                "date_of_birth": ["1990-01-01", "1985-05-20"],
                "address": ["123 Main St", "456 Oak St"],
            }
        )

        # Act
        result = anonymise_chunk(df_chunk)

        # Assert
        expected = pd.DataFrame(
            [{"anonymised_data": "hashed_John"}, {"anonymised_data": "hashed_Jane"}]
        )
        pd.testing.assert_frame_equal(result, expected)
        self.assertEqual(
            mock_anonymise.call_count, 2
        )  # Ensure anonymise was called twice (for each row)

    @patch("src.processor.file.anonymise")  # Mock anonymise
    def test_anonymise_dataframe_non_parallel(self, mock_anonymise):
        # Arrange
        mock_anonymise.side_effect = lambda row: {
            "anonymised_data": f"hashed_{row['first_name']}"
        }

        # Create a DataFrame
        df = pd.DataFrame(
            {
                "first_name": ["John", "Jane"],
                "last_name": ["Doe", "Smith"],
                "date_of_birth": ["1990-01-01", "1985-05-20"],
                "address": ["123 Main St", "456 Oak St"],
            }
        )

        # Act
        result = anonymise_dataframe(df, parallel=False)

        # Assert
        expected = pd.DataFrame(
            [{"anonymised_data": "hashed_John"}, {"anonymised_data": "hashed_Jane"}]
        )
        pd.testing.assert_frame_equal(result, expected)
        self.assertEqual(mock_anonymise.call_count, 2)


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

