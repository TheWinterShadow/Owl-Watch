
# Owl-Watch Execution


# Owl-Watch Execution

This package contains all ETL, ML, and utility code for the Owl-Watch data pipeline.

- `etl/`: ETL orchestration and job classes
  - `jobs/`: All ETL job classes (data cleaning, email, slack, sentiment analysis, etc.)
  - `main_etl_running.py`: Main runner for ETL jobs
  - `etl_class_factory.py`: Factory for selecting ETL job classes
- `ml/`: Machine learning and AI processing
- `utils/`: Shared utilities and base classes
- `docs/`: Documentation

## Pipeline Flow

1. **Raw Data** uploaded to S3 Raw Bucket
2. **Data Cleaning** via Glue ETL job (etl/jobs/) → S3 Cleaned Bucket
3. **ML Processing** (Sentiment Analysis, Bedrock Lambda in ml/) → S3 Curated Bucket

## Usage

### Deploy Glue Scripts
Scripts in `etl/jobs/` are deployed to S3 by the CDK stack. You can also upload manually if needed.

### Run ETL Jobs
Glue jobs in `etl/jobs/` are triggered by pipeline events or can be run manually in AWS Glue.

### Run Unit Tests
```bash
hatch test
```