import pandas as pd

# Read parquet file
df_parquet = pd.read_parquet("yellow_tripdata_2023-01.parquet")

# Save as CSV
df_parquet.to_csv("yellow_tripdata_2023-01.csv", index=False)

print("Conversion completed! CSV file created.")