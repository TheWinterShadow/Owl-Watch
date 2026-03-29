#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { IntegrationTestStack } from './lib/integration-test-stack';

const app = new cdk.App();

// Get environment from context
const environment = app.node.tryGetContext('environment') || 'dev';
const account = process.env.CDK_DEFAULT_ACCOUNT;
const region = process.env.CDK_DEFAULT_REGION || 'us-east-1';

new IntegrationTestStack(app, `OwlWatchIntegrationTestStack-${environment}`, {
  env: {
    account: account,
    region: region,
  },
  environment: environment,
  stackName: `owl-watch-integration-tests-${environment}`,
  description: `Integration test infrastructure for Owl-Watch ETL pipeline (${environment})`,
});