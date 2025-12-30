from pathlib import Path
import pandas as pd

RAW_DATA_DIR = Path("data/raw")

def load_raw_data(file_name: str) -> pd.DataFrame:
    """
    Load raw data from a CSV file into a pandas DataFrame.

    Args:
        file_name (str): The name of the CSV file to load.
    """
    file_path = RAW_DATA_DIR / file_name
    if not file_path.exists():
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    return pd.read_csv(file_path)

def load_all_raw_data() -> dict[str, pd.DataFrame]:
    """
    Load all raw data files from the RAW_DATA_DIR into a dictionary of DataFrames.

    Returns:
        dict: A dictionary where keys are file names and values are DataFrames.
    """
    data_frames = {}
    for file_path in RAW_DATA_DIR.glob("*.csv"):
        data_frames[file_path.name] = pd.read_csv(file_path)
    return data_frames

if __name__ == "__main__":
   
    all_data = load_all_raw_data()
    for file_name, df in all_data.items():
        print(f"Data from {file_name}:")
        print(df.head())