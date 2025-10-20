# Integration Tests for Owl-Watch ETL Pipeline

This directory contains infrastructure-based integration tests for the Owl-Watch ETL pipeline. The tests are deployed as AWS Lambda functions and can be run locally via pytest or in CI/CD via GitHub Actions.

## 🏗️ Architecture

The integration testing system uses a **Lambda-based architecture** for scalable, parallel test execution:

- **CDK Infrastructure**: Deploys S3 buckets, Lambda functions, and IAM roles
- **Lambda Test Runner**: Orchestrates multiple tests in parallel
- **Test Modules**: Individual test implementations (CSV transform, data quality, etc.)
- **GitHub Actions**: Automated testing in CI/CD pipelines

## 📁 Directory Structure

```
integration_tests/
├── cdk/                           # CDK Infrastructure
│   ├── lib/integration-test-stack.ts
│   ├── lambda/test-runner/        # Lambda function code
│   │   ├── index.py              # Main test orchestrator
│   │   ├── tests/                # Test modules
│   │   │   └── csv_transform_test.py
│   │   └── requirements.txt
│   ├── app.ts
│   ├── package.json
│   └── cdk.json
├── fixtures/                     # Test data and expected results
│   ├── data/sample_input.csv
│   └── expected/expected_output.json
├── scripts/                      # Deployment and execution scripts
│   ├── deploy-infrastructure.sh
│   ├── run-integration-tests.sh
│   └── destroy-infrastructure.sh
├── .github/workflows/            # GitHub Actions workflow
│   └── integration-tests.yml
├── test_integration_runner.py    # Pytest wrapper for local testing
├── pytest.ini
└── README.md
```

## 🚀 Quick Start

### 1. Deploy Infrastructure

```bash
# Deploy to dev environment
./scripts/deploy-infrastructure.sh dev

# Deploy to staging
./scripts/deploy-infrastructure.sh staging us-east-1
```

### 2. Run Tests

```bash
# Run all tests
./scripts/run-integration-tests.sh dev

# Run specific test
./scripts/run-integration-tests.sh dev csv-etl-transform-test
```

### 3. Local Testing (Optional)

```bash
# Run via pytest (requires deployed infrastructure)
hatch run pytest test_integration_runner.py -v

# Run specific test markers
hatch run pytest -m integration -v
```

## 🧪 Available Tests

### CSV ETL Transform Test (`csv-etl-transform-test`)
- **Purpose**: Validates CSV to JSON/Parquet transformation pipeline
- **Process**: 
  1. Uploads test CSV to S3 input bucket
  2. Waits for Glue job to process the data (via S3 trigger)
  3. Validates JSON and Parquet outputs
  4. Performs data quality checks
  5. Cleans up test resources
- **Duration**: ~15 minutes (includes Glue job execution)

## ⚙️ Configuration

### Environment Variables (Lambda)
The following environment variables are configured in the CDK stack:

```bash
ENVIRONMENT=dev                                    # Deployment environment
INPUT_BUCKET=owl-watch-integration-tests-input-*   # S3 input bucket
OUTPUT_BUCKET=owl-watch-integration-tests-output-* # S3 output bucket
RESULTS_BUCKET=owl-watch-integration-tests-results-* # Test results storage
GLUE_JOB_NAME_CSV_TRANSFORM=placeholder-job       # TODO: Update with actual Glue job name
```

### Test Configuration
Tests are configured via JSON payload when invoking the Lambda:

```json
{
  "test_config": {
    "tests": ["csv-etl-transform-test"],
    "timeout_seconds": 900
  }
}
```

## 🔧 Customization Guide

### Adding New Tests

1. **Create Test Module**: Add new test class in `cdk/lambda/test-runner/tests/`
2. **Register Test**: Add to `available_tests` dict in `index.py`
3. **Update Documentation**: Document the new test in this README

### Customizing CSV Test Data

1. **Input Data**: Edit `fixtures/data/sample_input.csv`
2. **Expected Output**: Update `fixtures/expected/expected_output.json`
3. **Test Logic**: Modify `csv_transform_test.py` validation methods

### Updating Glue Job Names

Update the environment variables in `integration-test-stack.ts`:

```typescript
GLUE_JOB_NAME_CSV_TRANSFORM: 'your-actual-glue-job-name'
```

## 🔄 GitHub Actions Integration

### Workflow Triggers
- **Push to main/master/develop**: Runs integration tests
- **Pull Request**: Runs tests and comments results
- **Manual Dispatch**: Allows custom environment/test selection

### Required Secrets
Add these secrets to your GitHub repository:

```bash
AWS_ROLE_TO_ASSUME  # IAM role ARN for GitHub OIDC authentication
```

### Workflow Outputs
The workflow provides structured outputs for downstream jobs:

```yaml
test_results_passed: "3"
test_results_total: "3" 
test_results_duration: "125.67"
test_results_success: "true"
```

## 📊 Results and Monitoring

### Test Results Format
Tests return GitHub Actions compatible JSON:

```json
{
  "success": true,
  "total_tests": 1,
  "passed": 1,
  "failed": 0,
  "errors": 0,
  "duration_seconds": 125.67,
  "tests": [
    {
      "name": "csv-etl-transform-test",
      "status": "PASSED",
      "duration": 125.67,
      "message": "Test completed successfully",
      "details": {
        "test_id": "csv-transform-abc123",
        "glue_job_run_id": "jr_12345",
        "json_record_count": 10,
        "parquet_file_size": 2048
      }
    }
  ]
}
```

### CloudWatch Logs
All test execution is logged to CloudWatch:
- **Log Group**: `/aws/lambda/integration-tests-{environment}`
- **Retention**: 30 days
- **Searchable**: Filter by test name, status, or error messages

### S3 Storage
Test results are archived in S3:
- **Bucket**: `owl-watch-integration-tests-results-{environment}-{region}`
- **Path**: `test-results/{environment}/{timestamp}/results-{test_id}.json`
- **Lifecycle**: 30 days retention

## 🛠️ Development Workflow

### Local Development
1. **Deploy Infrastructure**: `./scripts/deploy-infrastructure.sh dev`
2. **Test Changes**: `./scripts/run-integration-tests.sh dev`
3. **Debug**: Check CloudWatch logs and S3 results
4. **Iterate**: Update Lambda code and redeploy

### Production Deployment
1. **Staging Tests**: Deploy and test in staging environment
2. **Production Deploy**: `./scripts/deploy-infrastructure.sh prod`
3. **Smoke Test**: Run basic tests to verify deployment
4. **Monitor**: Set up CloudWatch alarms for test failures

## 🗑️ Cleanup

### Temporary Cleanup
```bash
# Destroy dev environment
./scripts/destroy-infrastructure.sh dev
```

### Automatic Cleanup
- **Test Data**: S3 lifecycle rules delete after 7 days
- **Test Results**: Archived for 30 days in results bucket
- **CloudWatch Logs**: 30 day retention

## 🔍 Troubleshooting

### Common Issues

1. **"Lambda function not found"**
   - Deploy infrastructure: `./scripts/deploy-infrastructure.sh {env}`

2. **"S3 bucket not found"**
   - Check CDK deployment completed successfully
   - Verify bucket naming matches environment/region

3. **"Glue job timeout"**
   - Check Glue job is configured with S3 triggers
   - Verify IAM permissions for Glue service
   - Update `GLUE_JOB_NAME_CSV_TRANSFORM` environment variable

4. **"Access denied"**
   - Verify AWS credentials have sufficient permissions
   - Check IAM roles in CDK stack have required policies

### Debug Commands

```bash
# Check Lambda function
aws lambda get-function --function-name owl-watch-integration-test-runner-dev

# List S3 buckets
aws s3 ls | grep owl-watch-integration

# View recent logs
aws logs tail /aws/lambda/integration-tests-dev --follow

# Check Glue job status
aws glue get-job-runs --job-name your-glue-job-name --max-results 5
```

## 📈 Performance Benchmarks

### Expected Performance
- **Test Execution**: < 15 minutes per test
- **Lambda Cold Start**: < 5 seconds
- **S3 Operations**: < 10 seconds
- **Glue Job Wait**: 5-12 minutes (depending on job complexity)

### Scaling Considerations
- **Parallel Tests**: Up to 3 concurrent tests supported
- **Lambda Timeout**: 15 minutes maximum
- **S3 Throughput**: No limits for test data sizes
- **Cost**: ~$0.10 per test run (estimated)

## 🆘 Support

For issues with integration tests:

1. **Check Logs**: CloudWatch logs contain detailed execution information
2. **Review Results**: S3 results bucket has complete test output
3. **GitHub Issues**: Create issues for bugs or feature requests
4. **Documentation**: Update this README for new procedures