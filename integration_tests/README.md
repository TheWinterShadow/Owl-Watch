
# Owl-Watch Integration Tests

Integration and end-to-end tests for the Owl-Watch data pipeline. Uses `pytest` and `moto` to mock AWS services.

## Structure

- `integration/` - End-to-end and component tests
  - `test_data_pipeline.py` - Full pipeline flow
  - `test_glue_jobs.py` - Glue ETL job creation and validation
  - `test_lambda_handler.py` - Lambda/Bedrock integration
- `fixtures/sample_data/` - Example test data (e.g., `customer_feedback.json`)
- `conftest.py` - Pytest config and AWS mocks

## Running Tests

From the project root:
```bash
hatch run test
```

## Test Scenarios

- **S3 Upload Trigger**: Uploading to S3 raw bucket triggers pipeline
- **Glue Job Processing**: ETL job creation and execution
- **Data Quality Validation**: Data validation rules
- **End-to-End Flow**: Complete raw → cleaned → curated flow
- **ML Processing**: Sentiment analysis and Bedrock Lambda integration

## Test Data

Sample data includes:
- Customer feedback (various sentiment levels)
- Product reviews and ratings
- Support ticket data

## Mocking

Tests use the `moto` library to mock:
- S3 buckets and operations
- Glue jobs and database
- Lambda functions