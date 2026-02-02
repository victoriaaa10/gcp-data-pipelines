import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

def get_parquet_schema(file_path: str) -> pd.DataFrame:
    """
    reads only the metadata/header of a parquet file
    """
    return pd.read_parquet(file_path).head(0)

def get_parquet_batches(file_path: str, chunk_size: int = 100000):
    """
    yields DataFrames in chunks to prevent 
    Out-Of-Memory (OOM) errors for large datasets.
    """
    parquet_file = pq.ParquetFile(file_path)
    
    for batch in parquet_file.iter_batches(batch_size=chunk_size):
        yield batch.to_pandas()

def load_csv(file_path: str) -> pd.DataFrame:
    """
    load CSV files into pandas DataFrame
    """
    if not Path(file_path).exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")
        
    return pd.read_csv(file_path)

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    basic data cleaning 
    removes records with invalid trip distances or negative amounts
    """
    # filtering out data noise often found in the NYC Taxi dataset
    initial_count = len(df)
    df = df[(df['trip_distance'] > 0) & (df['total_amount'] > 0)]
    
    # logging if rows were dropped
    if len(df) < initial_count:
        dropped = initial_count - len(df)
        print(f"Cleaned {dropped} invalid rows from batch.")
        
    return df