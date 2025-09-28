# Owl-Watch

[![Execution Build](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/execution-build.yml/badge.svg)](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/execution-build.yml) [![CDK Build](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/cdk-build.yml/badge.svg)](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/cdk-build.yml) [![Integration Build](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/integration-build.yml/badge.svg)](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/integration-build.yml) [![Security Scanning](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/security-scanning.yml/badge.svg)](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/security-scanning.yml) [![CICD Pipeline](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/cicd-pipeline.yml/badge.svg)](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/cicd-pipeline.yml)


Owl-Watch is a data engineering pipeline for ingesting, processing, and curating data using AWS Glue, Bedrock, and ML techniques. It features:
- Automated infrastructure deployment with AWS CDK (TypeScript)
- PySpark ETL jobs for data cleaning and sentiment analysis
- ML processing with AWS Bedrock and Lambda
- End-to-end integration tests with mocked AWS services

## Project Structure

- **`cdk/`** - AWS CDK infrastructure (TypeScript)
   - `lib/stacks/` - Data, Glue, and Monitoring stacks
   - `lib/utils/` - Asset and resource creation utilities
- **`execution/`** - PySpark ETL and ML code (Python)
   - `etl/` - Glue ETL jobs: `data_cleaning.py`, `sentiment_analysis.py`
   - `ml/` - Bedrock Lambda processor
- **`integration_tests/`** - Integration tests (Python, pytest)
   - `integration/` - End-to-end and component tests
   - `fixtures/sample_data/` - Example test data

## Prerequisites

- Python 3.11+
- Node.js 20+
- AWS CLI configured
- Hatch (Python build tool)

## Quick Start

1. **Deploy infrastructure:**
   ```bash
   cd cdk
   npm install
   npm run build
   cdk deploy --all

   **`cdk/`** - AWS CDK infrastructure (TypeScript)
      - `lib/stacks/` - Data, Glue, and Monitoring stacks
      - `lib/utils/` - Asset and resource creation utilities
   **`execution/`** - PySpark ETL and ML code (Python)
      - `etl/` - Glue ETL jobs: `data_cleaning.py`, `sentiment_analysis.py`
      - `ml/` - Bedrock Lambda processor
   **`integration_tests/`** - Integration tests (Python, pytest)
      - `integration/` - End-to-end and component tests
      - `fixtures/sample_data/` - Example test data
2. **Glue ETL Job** performs data cleaning and transformation
3. **Cleaned data** stored in S3 Cleaned Bucket
4. **ML Processing** (Bedrock, Sentiment Analysis) produces curated data in S3 Curated Bucket

## Testing

Integration tests use `pytest` and `moto` to mock AWS services. Test scenarios include:
- S3 upload triggers
- Glue job creation and execution
- Data quality validation
- End-to-end pipeline flow
- ML processing with Lambda and Bedrock


## License

MIT License - see [LICENSE](LICENSE) file for details.
