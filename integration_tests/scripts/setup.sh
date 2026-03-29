#!/bin/bash
set -e

# Integration Tests Setup Script
# This script sets up the integration testing environment from scratch

echo "🏗️  Owl-Watch Integration Tests Setup"
echo "===================================="

# Check prerequisites
echo "🔍 Checking prerequisites..."

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "❌ Node.js is not installed. Please install it first."
    exit 1
fi

# Check if CDK is installed
if ! command -v cdk &> /dev/null; then
    echo "📦 Installing AWS CDK CLI..."
    npm install -g aws-cdk
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo "❌ AWS credentials not configured. Please run 'aws configure' first."
    exit 1
fi

echo "✅ Prerequisites check passed"

# Get deployment parameters
ENVIRONMENT=${1:-dev}
REGION=${2:-us-east-1}

echo ""
echo "📋 Configuration:"
echo "Environment: $ENVIRONMENT"
echo "Region: $REGION"

# Install CDK dependencies
echo ""
echo "📦 Installing CDK dependencies..."
cd "$(dirname "$0")/../cdk"
npm install

# Bootstrap CDK (if needed)
echo ""
echo "🚀 Bootstrapping CDK (if needed)..."
CDK_DEFAULT_REGION=$REGION cdk bootstrap --context environment=$ENVIRONMENT

# Deploy infrastructure
echo ""
echo "☁️  Deploying integration test infrastructure..."
./scripts/deploy-infrastructure.sh $ENVIRONMENT $REGION

echo ""
echo "✅ Setup completed successfully!"
echo ""
echo "🎯 Next steps:"
echo "1. Update Glue job names in the Lambda environment variables:"
echo "   - Edit cdk/lib/integration-test-stack.ts"
echo "   - Update GLUE_JOB_NAME_CSV_TRANSFORM with your actual job name"
echo ""
echo "2. Customize test data (optional):"
echo "   - Edit fixtures/data/sample_input.csv"
echo "   - Update fixtures/expected/expected_output.json"
echo ""
echo "3. Test the setup:"
echo "   ./scripts/run-integration-tests.sh $ENVIRONMENT"
echo ""
echo "4. For GitHub Actions, add this secret to your repo:"
echo "   AWS_ROLE_TO_ASSUME: <your-github-oidc-role-arn>"
echo ""
echo "📚 See README.md for detailed documentation"