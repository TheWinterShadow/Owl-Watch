# Owl-Watch

A data engineering pipeline for ingesting, processing, and curating data using AWS Glue, Bedrock, and ML techniques.

## Project Structure

- **`cdk/`** - AWS CDK infrastructure stacks (Data, Monitoring, ML)
- **`src/`** - PySpark ETL jobs and data processing code
- **`tests/`** - Integration tests for Glue jobs and data pipeline

## Prerequisites

- Python 3.9+
- Node.js 18+
- AWS CLI configured
- Bazel 6.0+

## Quick Start

1. Deploy infrastructure:
   ```bash
   cd cdk && npm install && cdk deploy --all
   ```

2. Run integration tests:
   ```bash
   bazel test //tests/integration/...
   ```

## Pipeline Flow

1. Raw data → S3 Raw Bucket
2. Glue ETL Job → Data cleaning and transformation
3. Cleaned data → S3 Cleaned Bucket
4. ML Processing (Bedrock, Sentiment Analysis) → S3 Curated Bucket

## License

MIT License - see [LICENSE](LICENSE) file for details.