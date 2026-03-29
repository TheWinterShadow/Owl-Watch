#!/bin/bash
set -e

# Run Integration Tests via Lambda
# This script invokes the Lambda function to run integration tests

ENVIRONMENT=${1:-dev}
LAMBDA_FUNCTION_NAME="owl-watch-integration-test-runner-$ENVIRONMENT"
TESTS_TO_RUN=${2:-"csv-etl-transform-test"}

echo "🧪 Running integration tests..."
echo "Environment: $ENVIRONMENT"
echo "Lambda Function: $LAMBDA_FUNCTION_NAME" 
echo "Tests: $TESTS_TO_RUN"

# Create test configuration
TEST_CONFIG=$(cat <<EOF
{
    "test_config": {
        "tests": ["$TESTS_TO_RUN"],
        "timeout_seconds": 900
    }
}
EOF
)

echo "📋 Test Configuration:"
echo "$TEST_CONFIG" | jq '.'

# Invoke Lambda function
echo ""
echo "🚀 Invoking Lambda function..."
RESULT=$(aws lambda invoke \
    --function-name "$LAMBDA_FUNCTION_NAME" \
    --payload "$TEST_CONFIG" \
    --cli-binary-format raw-in-base64-out \
    response.json)

# Check if invocation was successful
if [ $? -ne 0 ]; then
    echo "❌ Lambda invocation failed"
    exit 1
fi

echo "📊 Lambda Invocation Response:"
echo "$RESULT" | jq '.'

# Parse the response
echo ""
echo "📈 Test Results:"
cat response.json | jq '.'

# Extract success status
SUCCESS=$(cat response.json | jq -r '.success // false')
STATUS_CODE=$(cat response.json | jq -r '.statusCode // 500')

# Parse test results from body
BODY=$(cat response.json | jq -r '.body')
if [ "$BODY" != "null" ] && [ "$BODY" != "" ]; then
    echo ""
    echo "🔍 Detailed Results:"
    echo "$BODY" | jq '.'
    
    # Extract summary for GitHub Actions
    PASSED=$(echo "$BODY" | jq -r '.passed // 0')
    TOTAL=$(echo "$BODY" | jq -r '.total_tests // 0')
    DURATION=$(echo "$BODY" | jq -r '.duration_seconds // 0')
    
    echo ""
    echo "📊 Summary:"
    echo "Tests Passed: $PASSED/$TOTAL"
    echo "Duration: ${DURATION}s"
    
    # Set GitHub Actions outputs if running in CI
    if [ "$GITHUB_ACTIONS" = "true" ]; then
        echo "test_results_passed=$PASSED" >> $GITHUB_OUTPUT
        echo "test_results_total=$TOTAL" >> $GITHUB_OUTPUT
        echo "test_results_duration=$DURATION" >> $GITHUB_OUTPUT
        echo "test_results_success=$SUCCESS" >> $GITHUB_OUTPUT
    fi
fi

# Clean up response file
rm -f response.json

# Exit with appropriate code
if [ "$SUCCESS" = "true" ]; then
    echo ""
    echo "✅ All integration tests passed!"
    exit 0
else
    echo ""
    echo "❌ Integration tests failed!"
    exit 1
fi