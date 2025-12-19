import pandas as pd
import pyarrow.parquet as pq 


df = pd.read_parquet("processed_2025-12-19 10_03_56.688588.parquet")

print(df.head())
