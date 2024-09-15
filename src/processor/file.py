from models.file import File
import hashlib
import numpy as np
import pandas as pd


def hash_data(value: str) -> str:
    """
    Generates a SHA-256 hash for a given string.

    This function takes a string input and returns its SHA-256 hash, which can be used
    to anonymize or secure sensitive data.

    Parameters:
    -----------
    value : str
        The string to be hashed.

    Returns:
    --------
    str
        The SHA-256 hash of the input string.

    Example:
    --------
    hashed_value = hash_data("example_string")
    print(hashed_value)  # Outputs the SHA-256 hash of "example_string"
    """
    return hashlib.sha256(value.encode()).hexdigest()


def anonymise_data(value: File) -> dict:
    """
    Anonymizes sensitive data in a File object by hashing its attributes.

    This function takes a `File` object and returns a dictionary where the sensitive
    fields (`first_name`, `last_name`, `date_of_birth`, and `address`) are hashed using
    the SHA-256 algorithm. This is useful for anonymizing personal information for privacy
    or data security purposes.

    Parameters:
    -----------
    value : File
        The `File` object containing personal information to be anonymized.

    Returns:
    --------
    dict
        A dictionary with the following fields:
        - `first_name`: The hashed first name.
        - `last_name`: The hashed last name.
        - `date_of_birth`: The hashed date of birth.
        - `address`: The hashed address.

    Example:
    --------
    file = File(
        first_name="John",
        last_name="Doe",
        date_of_birth="1990-01-01",
        address="123 Main St"
    )
    anonymised_data = anonymise_data(file)
    print(anonymised_data)
    # Output:
    # {
    #   "first_name": "<hashed_value>",
    #   "last_name": "<hashed_value>",
    #   "date_of_birth": "<hashed_value>",
    #   "address": "<hashed_value>"
    # }
    """
    return {
        "first_name": hash_data(value.first_name),
        "last_name": hash_data(value.last_name),
        "date_of_birth": hash_data(str(value.date_of_birth)),
        "address": hash_data(value.address),
    }


def anonymise(row):
    file = File(**row.to_dict())
    return anonymise_data(file)


def anonymise_chunk(df_chunk):
    anonymised_data = list(df_chunk.apply(lambda row: anonymise(row), axis=1))
    return pd.DataFrame(anonymised_data)


def anonymise_dataframe(df, num_splits=4, parallel=False, ray=None):

    if parallel:

        # Split the DataFrame into chunks
        df_split = np.array_split(df, num_splits)

        @ray.remote
        def process_remote(df_chunk):
            return anonymise_chunk(df_chunk)

        # Execute the function on each chunk in parallel
        futures = [process_remote.remote(chunk) for chunk in df_split]
        results = ray.get(futures)

        # Concatenate the results back into a single DataFrame
        df_processed = pd.concat(results)

        # Return results
        return df_processed

    else:
        anonymised_data = list(df.apply(lambda row: anonymise(row), axis=1))
        return pd.DataFrame(anonymised_data)
