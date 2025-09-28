
# Owl-Watch Execution

[![Execution Build](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/execution-build.yml/badge.svg)](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/execution-build.yml)

PySpark ETL jobs and ML processing code for the Owl-Watch data pipeline. Includes Glue jobs for data cleaning and sentiment analysis, and a Lambda processor for Bedrock ML integration.

## Structure

- `src/etl/`
  - `data_cleaning.py` - Glue ETL job for data cleaning and transformation
  - `sentiment_analysis.py` - Glue ETL job for sentiment analysis using ML
- `src/ml/`
  - `bedrock_processor.py` - Lambda function for Bedrock AI processing

## Pipeline Flow

1. **Raw Data** uploaded to S3 Raw Bucket
2. **Data Cleaning** via Glue ETL job → S3 Cleaned Bucket
3. **ML Processing** (Sentiment Analysis, Bedrock Lambda) → S3 Curated Bucket

## Usage

### Deploy Glue Scripts
Scripts in `src/etl/` are deployed to S3 by the CDK stack. You can also upload manually if needed.

### Run ETL Jobs
Glue jobs are triggered by pipeline events or can be run manually in AWS Glue.

### Run Unit Tests
```bash
hatch test
```