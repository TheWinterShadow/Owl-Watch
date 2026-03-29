# Architecture

Owl-Watch utilizes a fully AWS-native serverless architecture to orchestrate large-scale data engineering workloads. It separates data into multiple zones and uses Glue for ETL processing and Lambda/Bedrock for advanced machine learning curation.

## High-Level Flow

1. **Ingestion**: Raw data is uploaded or streamed into the Raw S3 bucket.
2. **ETL Processing**: AWS Glue jobs perform distributed data cleaning and transformation using PySpark.
3. **Storage**: Cleaned data is stored in the S3 Cleaned Bucket.
4. **ML Curation**: Machine learning processing (using AWS Bedrock via Lambda functions) further refines and analyzes the data, producing sentiment analysis and insights.
5. **Output**: The finalized curated data is stored in the S3 Curated Bucket for downstream consumption.

## Project Structure

- **`cdk/`** - AWS CDK infrastructure (TypeScript)
   - `lib/stacks/` - Data, Glue, and Monitoring stacks
   - `lib/utils/` - Asset and resource creation utilities
- **`execution/`** - PySpark ETL and ML code (Python)
   - `core/` - Job runners, config managers, and factory patterns
   - `jobs/` - Specialized ETL and ML jobs
   - `models/` - Data models and quality constraints
   - `schemas/` - Standardized schemas for communication data
- **`integration_tests/`** - Integration tests (Python, pytest)
