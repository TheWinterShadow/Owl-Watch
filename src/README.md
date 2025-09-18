# Owl-Watch Source Code

PySpark ETL jobs and ML processing code for the data engineering pipeline.

## Structure

- `etl/` - Glue ETL jobs
  - `data_cleaning.py` - Data cleaning and transformation
  - `sentiment_analysis.py` - ML-based sentiment analysis
- `ml/` - Machine learning processors
  - `bedrock_processor.py` - AWS Bedrock integration
- `scripts/` - Utility scripts
  - `upload_glue_scripts.py` - Deploy scripts to S3

## Pipeline Flow

1. **Raw Data** → S3 Raw Bucket
2. **Data Cleaning** → Glue ETL Job → S3 Cleaned Bucket  
3. **ML Processing** → Sentiment Analysis + Bedrock → S3 Curated Bucket

## Usage

### Deploy Glue Scripts
```bash
bazel run //src:upload_scripts
```

### Run Integration Tests
```bash
bazel test //tests:integration_tests
```