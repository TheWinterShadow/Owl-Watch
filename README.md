# Owl-Watch

A data engineering pipeline for ingesting, processing, and curating data using AWS Glue, Bedrock, and ML techniques.

## Project Structure

- **`cdk/`** - AWS CDK infrastructure stacks (Data, Monitoring, ML)
- **`execution/`** - PySpark ETL jobs and data processing code
- **`integration_tests/`** - Integration tests for Glue jobs and data pipeline

## Prerequisites

- Python 3.9+
- Node.js 18+
- AWS CLI configured
 - Hatch (Python build tool)

## Quick Start

1. Deploy infrastructure:
   ```bash
   cd cdk && npm install && npm run build && cdk deploy --all
   ```

2. Run integration tests:
   ```bash
   hatch run test
   ```

## Pipeline Flow

1. Raw data → S3 Raw Bucket
2. Glue ETL Job → Data cleaning and transformation
3. Cleaned data → S3 Cleaned Bucket
4. ML Processing (Bedrock, Sentiment Analysis) → S3 Curated Bucket

## License

MIT License - see [LICENSE](LICENSE) file for details.