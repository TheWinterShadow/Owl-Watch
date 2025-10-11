import { App } from 'aws-cdk-lib';
import { DataStack } from './stacks/dataStack';
import { GlueStack } from './stacks/glueStack';
import { MonitoringStack } from './stacks/monitoringStack';
import { StageConfig } from './utils/types';

const app = new App();

// Get AWS Account ID from environment variable (set from GitHub secrets)
const AWS_ACCOUNT_ID = process.env.AWS_ACCOUNT_ID;

if (!AWS_ACCOUNT_ID) {
  throw new Error('AWS_ACCOUNT_ID environment variable is required. Please set it from GitHub repository secrets.');
}

const STAGES: StageConfig[] = [
  { name: 'Beta', accountId: AWS_ACCOUNT_ID, region: 'us-east-1' },
  { name: 'Prod', accountId: AWS_ACCOUNT_ID, region: 'us-east-2' },
  // Add more stages as needed
];

for (const stage of STAGES) {
  createStacksForStage(stage);
}

function createStacksForStage(stage: StageConfig) {
  const { name, accountId, region } = stage;

  // Set environment for the stacks
  const env = { account: accountId, region };

  // Instantiate DataStack first to get the data bucket
  const dataStack = new DataStack(app, `DataStack-${name}`, {
    stageName: name,
    accountId: accountId,
    region: region,
    env: env,
  });

  // Pass the data bucket to GlueStack
  const glueStack = new GlueStack(app, `GlueStack-${name}`, {
    assetBucket: dataStack.dataBucket,
    stageName: name,
    accountId: accountId,
    region: region,
    env: env,
  });

  // Pass required resources to MonitoringStack (empty arrays for now)
  new MonitoringStack(app, `MonitoringStack-${name}`, {
    stageName: name,
    accountId: accountId,
    region: region,
    s3Buckets: [dataStack.dataBucket],
    lambdaFunctions: [],
    glueJobs: glueStack.glueJobs,
    glueCrawlers: glueStack.glueCrawlers,
    env: env,
  });
}
