import streamlit as st
from streamlit.logger import get_logger
from pathlib import Path
from src.processor.file import anonymise_dataframe
import pandas as pd
import ray

LOGGER = get_logger(__name__)
INPUT_DIR = f"{Path.cwd()}/data/input"


def list_files_in_directory(directory):
    try:
        # Convert the directory string to a Path object
        dir_path = Path(directory)
        # List all files in the directory
        files = [f.name for f in dir_path.iterdir() if f.is_file()]
        return files
    except FileNotFoundError:
        return ["Directory not found"]


def run():
    st.set_page_config(
        page_title="Anonymise Columns Demo",
        page_icon="👋",
    )

    st.write("# Welcome to Noel's App! 👋")
    st.write(
        """This demo shows how to use the function
    [`anonymise_data`](https://docs.streamlit.io/library/api-reference/charts/st.pydeck_chart)
    to anonymise the columns `first_name`, `last_name`, `address` and `date_of_birth` from a file."""
    )

    st.sidebar.header("Anonymise Columns Demo Config")
    st.sidebar.markdown(f"- Current Working Directory: `{Path.cwd()}`")

    # Directory to list files from
    directory = st.text_input("Enter the directory path:", value=INPUT_DIR)

    # Get list of files in the given directory
    files = list_files_in_directory(directory)

    # Create a dropdown (selectbox) to display files
    selected_file = st.selectbox("Select a file", files)
    parallel = st.checkbox("Run in parallel")
    num_splits = None
    if parallel:
        num_splits = st.slider("Number of splits", min_value=2, max_value=10)

    if st.button("Render"):

        # Show the selected file
        df = pd.read_csv(f"{INPUT_DIR}/{selected_file}")
        st.write(f"Selected file: {selected_file}")
        st.write(f"Original data [shape]: {df.shape}")
        st.dataframe(df)

        # Anonymise data
        df_anonymised = anonymise_dataframe(
            df, num_splits=num_splits, parallel=parallel
        )
        st.write(f"Anonymised dataframe [shape]: {df_anonymised.shape}")
        st.dataframe(df_anonymised)


if __name__ == "__main__":
    run()
    if not ray.is_initialized():
        ray.init()

        # Shutdown Ray
        ray.shutdown()
