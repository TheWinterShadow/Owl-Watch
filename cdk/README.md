# Owl-Watch CDK

AWS CDK infrastructure for the Owl-Watch data engineering pipeline.

## Structure

- `lib/stacks/` - Infrastructure stacks (Data, ML, Monitoring)
- `lib/constructs/` - Reusable CDK constructs

## Stacks

### DataStack

- S3 buckets (raw, cleaned, curated)
- Glue ETL jobs and database
- EventBridge rules for automation

### MLStack

- Bedrock integration for AI processing
- Sentiment analysis Glue jobs
- Lambda functions for ML workflows

### MonitoringStack

- CloudWatch dashboards and alarms
- SNS notifications
- Pipeline monitoring metrics

## Commands

```bash
# Install dependencies
npm install

# Build TypeScript
npm run build

# Deploy all stacks
npm run deploy -- --all

# Deploy specific stack
npm run deploy -- OwlWatchDataStack

# Destroy all stacks
npm run destroy -- --all
```
