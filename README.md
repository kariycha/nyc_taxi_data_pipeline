# NYC Taxi Data Pipeline

This project is a modular, production-ready data engineering pipeline that downloads, transforms, and uploads NYC Taxi data. It is designed to mimic a real-world data pipeline suitable for AWS S3 + Athena querying.

----------
## 📥 Data Source

The NYC Taxi trip data was sourced from the official New York City Taxi & Limousine Commission (TLC).

These files were converted to Parquet format and hosted in a public GitHub repository as part of this data engineering project.

----------

## 🧩 Components

Script

Description

`download_data.py`

Downloads Parquet files from a GitHub folder using the GitHub API and combines them locally.

`transform_data.py`

Cleans and transforms the Parquet files (e.g., datetime handling, nulls).

`upload_to_s3.py`

Uploads transformed files to a specified S3 bucket.

`main_pipeline.py`

Orchestrates the entire workflow (download → transform → upload).

`logger_setup.py`

Configures rotating file-based logging shared by all modules.

----------

## 🛠️ Requirements

-   Python 3.11+
    
-   AWS credentials set up locally (via `~/.aws/credentials`, environment variables, or IAM Role)
    
-   Python packages:
    
    ```
    pip install pandas pyarrow boto3 requests
    ```
    

----------

## 🗂️ Directory Structure

```
nyc_taxi_data_pipeline/
├── data/
│   ├── source/           # Original Parquet files (from GitHub)
│   ├── processed/        # Transformed files ready for S3
│   └── error/            # Files that failed during transformation
├── logs/                 # Rotating log files
├── download_data.py
├── transform_data.py
├── upload_to_s3.py
├── main_pipeline.py
├── logger_setup.py
└── README.md
```

----------

## 🚀 How to Run the Pipeline

1.  **Ensure your AWS credentials are configured** (see below).
    
2.  **Run the entire pipeline**:
    

```
python main_pipeline.py
```

This will:

-   Download Parquet files from GitHub (or any configured source)
    
-   Clean and transform them
    
-   Upload them to the configured S3 bucket
    

----------

## 🔐 AWS Credentials Setup

### Option 1: Environment Variables

```
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=ap-southeast-2
```

### Option 2: AWS CLI Profile (~/.aws/credentials)

```
[default]
aws_access_key_id = YOUR_KEY
aws_secret_access_key = YOUR_SECRET
```

----------

## 🧪 Testing Individual Steps

### Run Download:

```
python download_data.py
```

### Run Transform:

```
python transform_data.py --input-dir data
```

### Run Upload:

```
python upload_to_s3.py --bucket your-bucket-name --input-dir data/processed
```

----------

## 📋 Logging

All logs are stored in the `logs/` folder with log rotation (2 backups, 1MB each).

----------

## 📌 Notes

-   GitHub API is used to fetch a dynamic list of `.parquet` files.
    
-   PyArrow and pandas are used for efficient Parquet processing.
    
-   AWS S3 key prefix is customizable.
    

----------

## 🧑 Author

**CK**, 2025-Jul

MIT License
