import os
import requests
import gzip
import shutil

# Create data folder if it doesn't exist
os.makedirs("data", exist_ok=True)

# File URL and paths
url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2023-01.csv.gz"
csv_gz_path = "data/yellow_tripdata_2023-01.csv.gz"
csv_path = "data/yellow_tripdata_2023-01.csv"

# Download the file
print("Downloading data...")
response = requests.get(url, stream=True)
with open(csv_gz_path, "wb") as f:
    shutil.copyfileobj(response.raw, f)
print(f"Downloaded to {csv_gz_path}")

# Extract the file
print("Extracting...")
with gzip.open(csv_gz_path, "rb") as f_in:
    with open(csv_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
print(f"Extracted to {csv_path}")
