import { App } from 'aws-cdk-lib';
import { DataStack } from './stacks/dataStack';
import { GlueStack } from './stacks/glueStack';
import { MonitoringStack } from './stacks/monitoringStack';

const app = new App();

const region = process.env.CDK_DEFAULT_REGION;
const account = process.env.CDK_DEFAULT_ACCOUNT;

// Instantiate DataStack first to get the data bucket
const dataStack = new DataStack(app, 'DataStack', {
  env: { account, region },
});

// Pass the data bucket to GlueStack
const glueStack = new GlueStack(app, 'GlueStack', {
  assetBucket: dataStack.dataBucket,
  env: { account, region },
});

// Pass required resources to MonitoringStack (empty arrays for now)
new MonitoringStack(app, 'MonitoringStack', {
  s3Buckets: [dataStack.dataBucket],
  lambdaFunctions: [],
  glueJobs: glueStack.glueJobs,
  glueCrawlers: glueStack.glueCrawlers,
  env: { account, region },
});
