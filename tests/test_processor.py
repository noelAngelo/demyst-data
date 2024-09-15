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
import pandas as pd


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
