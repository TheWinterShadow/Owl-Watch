# Owl-Watch Tests

Integration tests for the data engineering pipeline.

## Structure

- `integration/` - End-to-end pipeline tests
  - `test_data_pipeline.py` - Complete pipeline flow testing
  - `test_glue_jobs.py` - Glue ETL job testing
- `fixtures/` - Test data and sample files
- `conftest.py` - Pytest configuration with AWS mocking

## Running Tests

### All Integration Tests
```bash
bazel test //tests:integration_tests
```

### Specific Test File
```bash
bazel test //tests:integration_tests --test_filter=test_data_pipeline
```

## Test Scenarios

- **S3 Upload Trigger**: Tests file upload triggering pipeline
- **Glue Job Processing**: Tests ETL job creation and execution
- **Data Quality Validation**: Tests data validation rules
- **End-to-End Flow**: Tests complete raw → cleaned → curated flow
- **ML Processing**: Tests sentiment analysis and Bedrock integration

## Test Data

Sample data includes:
- Customer feedback with various sentiment levels
- Product reviews and ratings
- Support ticket data

## Mocking

Tests use `moto` library to mock:
- S3 buckets and operations
- Glue jobs and database
- Lambda functions