#!/bin/bash
set -e

# Destroy Integration Test Infrastructure
# This script destroys the CDK stack for integration testing

ENVIRONMENT=${1:-dev}
REGION=${2:-us-east-1}

echo "🗑️  Destroying integration test infrastructure..."
echo "Environment: $ENVIRONMENT"
echo "Region: $REGION"

read -p "Are you sure you want to destroy the $ENVIRONMENT integration test infrastructure? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Cancelled"
    exit 1
fi

# Change to CDK directory
cd "$(dirname "$0")/../cdk"

# Destroy CDK stack
echo "☁️  Destroying AWS resources..."
CDK_DEFAULT_REGION=$REGION npm run cdk destroy -- \
    --context environment=$ENVIRONMENT \
    --force

# Clean up output files
rm -f "../outputs-$ENVIRONMENT.json"

echo "✅ Infrastructure destroyed!"