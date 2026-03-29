# Integration Test Infrastructure

This directory contains AWS CDK infrastructure for integration testing.

## Components

- `integration_test_stack.ts` - Main CDK stack for test infrastructure
- `lambda/` - Lambda function code for test orchestration
- `cdk.json` - CDK configuration for test stack

## Resources Created

1. **S3 Buckets**
   - Input bucket for test data
   - Output bucket for processed results
   - Archive bucket for test history

2. **Lambda Functions**
   - Test orchestrator function
   - Data validation function
   - Cleanup function

3. **IAM Roles and Policies**
   - Lambda execution roles
   - S3 access policies
   - Glue service permissions

4. **CloudWatch Resources**
   - Log groups for test execution
   - Alarms for test failures
   - Metrics for test performance

## Deployment

```bash
# Deploy integration test infrastructure
cd integration_tests/infrastructure
cdk deploy IntegrationTestStack

# Destroy test infrastructure
cdk destroy IntegrationTestStack
```