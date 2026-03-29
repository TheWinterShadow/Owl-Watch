# 🦉 Owl-Watch

[![Execution Build](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/execution-build.yml/badge.svg)](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/execution-build.yml) [![CDK Build](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/cdk-build.yml/badge.svg)](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/cdk-build.yml) [![Integration Build](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/integration-build.yml/badge.svg)](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/integration-build.yml) [![Security Scanning](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/security-scanning.yml/badge.svg)](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/security-scanning.yml) [![CICD Pipeline](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/cicd-pipeline.yml/badge.svg)](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/cicd-pipeline.yml)

**Owl-Watch** is an AWS-native data engineering pipeline for ingesting, processing, and curating data using AWS Glue, Bedrock, and ML techniques. 

It handles data extraction, scalable PySpark ETL jobs, machine learning curation, and automated infrastructure deployment through AWS CDK.

## Key Features

- 🏗️ **Infrastructure as Code** - Automated infrastructure deployment with AWS CDK (TypeScript)
- 📊 **Scalable ETL** - PySpark ETL jobs for data cleaning and transformation
- 🧠 **ML Integration** - Machine Learning processing with AWS Bedrock and Lambda for sentiment analysis and data curation
- 🧪 **Testing & Validation** - End-to-end integration tests with mocked AWS services
- 💻 **Local Development** - Built-in local Spark runner for fast job iteration without cloud overhead

## Quick Start

### Prerequisites

- Python 3.11+
- Node.js 20+
- AWS CLI configured
- [Hatch](https://hatch.pypa.io/) (Python build tool)

### Deploy Infrastructure

```bash
cd cdk
npm install
npm run build
cdk deploy --all
```

## Running ETL Jobs Locally

You can run ETL jobs locally for development and testing using the built-in Hatch scripts. The local ETL runner uses local Spark instead of AWS Glue, creates sample data, outputs results to your local filesystem, and shows a transformed data preview.

```bash
# Run any ETL job with custom arguments
hatch run run-etl --job-type email_communication --output-path ./my-output

# Run specific ETL jobs with defaults
hatch run run-email-etl
hatch run run-slack-etl

# Run with custom output path
hatch run run-email-etl --output-path ./email-results
```

### Testing

Integration tests use `pytest` and `moto` to mock AWS services. 

```bash
# Run all tests
hatch run pytest

# Run specific test suite
hatch run pytest tests/execution/
hatch run pytest integration_tests/
```
