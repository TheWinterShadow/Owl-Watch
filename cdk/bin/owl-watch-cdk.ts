#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { DataStack } from '../lib/stacks/data-stack';
import { MonitoringStack } from '../lib/stacks/monitoring-stack';
import { MLStack } from '../lib/stacks/ml-stack';

const app = new cdk.App();

const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

const dataStack = new DataStack(app, 'OwlWatchDataStack', { env });
const mlStack = new MLStack(app, 'OwlWatchMLStack', { env, dataStack });
new MonitoringStack(app, 'OwlWatchMonitoringStack', { env, dataStack, mlStack });