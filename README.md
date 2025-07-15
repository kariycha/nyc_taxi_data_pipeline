# NYC Taxi Data Pipeline on AWS

This project demonstrates a simple yet production-style data engineering pipeline using **AWS services**, **Python**, and **SQL**. The pipeline ingests raw NYC taxi trip data, transforms it using `pandas`, stores it in Amazon S3, and queries it with Amazon Athena.

---

## 🚀 Features
- Ingests open-source NYC taxi data
- Cleans and transforms CSV using Python + Pandas
- Uploads data to Amazon S3
- Queries data using Amazon Athena
- Easily extendable to use Glue, Lambda, or CI/CD workflows

---

## 📁 Project Structure
```
aws_data_pipeline/
├── data/                        # Optional: Local raw/cleaned files
├── scripts/
│   ├── download_data.py        # Downloads NYC taxi CSV data
│   ├── transform_data.py       # Cleans & transforms raw data
│   └── upload_to_s3.py         # Uploads cleaned data to S3
├── sql/
│   └── create_athena_table.sql # DDL for creating Athena external table
├── .gitignore
├── README.md
└── requirements.txt
```

---

## 🧰 Tools & Services
- Python 3.7
- Pandas
- Boto3 (AWS SDK for Python)
- Amazon S3
- Amazon Athena
- AWS CLI

---

## ⚙️ Setup Instructions

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure AWS Credentials
```bash
aws configure
# Enter AWS Access Key, Secret, and region (e.g. ap-southeast-2)
```

### 3. Run Scripts
```bash
# Step 1: Download raw data\python scripts/download_data.py

# Step 2: Transform data
python scripts/transform_data.py

# Step 3: Upload to S3
python scripts/upload_to_s3.py
```

### 4. Create Athena Table
Run the SQL in `sql/create_athena_table.sql` using the AWS Athena console or `boto3`.

---

## 📊 Example Queries
```sql
SELECT VendorID, COUNT(*) as trip_count
FROM nyc_taxi_data
GROUP BY VendorID;
```

---

## 📌 Resume Bullet Point Example
> Built an AWS-based ETL pipeline to ingest NYC taxi data, transform with Python, and load to S3 for querying via Athena. Developed modular scripts for ingestion, transformation, and storage. Documented pipeline and followed CI/CD principles for automation-readiness.

---

## 📈 Future Enhancements
- Add GitHub Actions CI pipeline
- Convert CSV to Parquet for performance
- Automate with AWS Glue or Apache Airflow
- Integrate monitoring and alerting

---

## 👤 Author
CK

---

## 📝 License
MIT
