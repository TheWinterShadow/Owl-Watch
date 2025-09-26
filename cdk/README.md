# Owl-Watch CDK

[![CDK Build](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/cdk-build.yml/badge.svg)](https://github.com/TheWinterShadow/Owl-Watch/actions/workflows/cdk-build.yml)

AWS CDK (TypeScript) infrastructure for the Owl-Watch data engineering pipeline. Defines S3 buckets, Glue jobs, Bedrock integration, and monitoring resources.

## Structure

- `lib/stacks/` - Infrastructure stacks:
	- `dataStack.ts` - S3 buckets (raw, cleaned, curated), initial asset deployment
	- `glueStack.ts` - Glue jobs, crawlers, IAM roles
	- `monitoringStack.ts` - CloudWatch dashboards, alarms, metrics
- `lib/utils/` - Asset creation utilities (`createAsset.ts`, `types.ts`)

## Stacks

### DataStack
- S3 buckets for raw, cleaned, and curated data
- Lifecycle rules for data retention
- Deploys ETL scripts to S3 for Glue jobs

### GlueStack
- Glue jobs for ETL and ML processing
- Glue crawlers for schema discovery
- IAM roles for Glue

### MonitoringStack
- CloudWatch dashboards and alarms
- S3, Lambda, and Glue job monitoring

## Deployment Commands

```bash
# Install dependencies
npm install

# Build TypeScript
npm run build

# Deploy all stacks
cdk deploy --all
```
# Deploy specific stack
npm run deploy -- OwlWatchDataStack

# Destroy all stacks
npm run destroy -- --all
```
