#!/bin/bash
set -e

# Deploy Integration Test Infrastructure
# This script deploys the CDK stack for integration testing

ENVIRONMENT=${1:-dev}
REGION=${2:-us-east-1}

echo "🚀 Deploying integration test infrastructure..."
echo "Environment: $ENVIRONMENT"
echo "Region: $REGION"

# Change to CDK directory
cd "$(dirname "$0")/../cdk"

# Install dependencies if node_modules doesn't exist
if [ ! -d "node_modules" ]; then
    echo "📦 Installing CDK dependencies..."
    npm install
fi

# Build TypeScript
echo "🔨 Building CDK stack..."
npm run build

# Deploy CDK stack
echo "☁️  Deploying to AWS..."
CDK_DEFAULT_REGION=$REGION npm run cdk deploy -- \
    --context environment=$ENVIRONMENT \
    --require-approval never \
    --outputs-file ../outputs-$ENVIRONMENT.json

echo "✅ Deployment completed!"

# Display outputs
if [ -f "../outputs-$ENVIRONMENT.json" ]; then
    echo ""
    echo "📋 Stack Outputs:"
    cat "../outputs-$ENVIRONMENT.json" | jq '.'
fi

echo ""
echo "🎯 Next steps:"
echo "1. Update the Glue job names in the Lambda environment variables"
echo "2. Test the deployment with: ./scripts/run-integration-tests.sh $ENVIRONMENT"
echo "3. Add the Lambda function name to your GitHub Actions workflow"