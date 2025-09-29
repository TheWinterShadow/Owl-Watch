import { App } from 'aws-cdk-lib';
import { DataStack } from './stacks/dataStack';
import { GlueStack } from './stacks/glueStack';
import { MonitoringStack } from './stacks/monitoringStack';
import { StageConfig } from './utils/types';

const app = new App();

const STAGES: StageConfig[] = [
  { name: 'Beta', accountId: '759314175046', region: 'us-east-1' },
  { name: 'Prod', accountId: '759314175046', region: 'us-east-2' },
  // Add more stages as needed
];

for (const stage of STAGES) {
  createStacksForStage(stage);
}

function createStacksForStage(stage: StageConfig) {
  const { name, accountId, region } = stage;
  console.log(`Creating stacks for stage: ${name} in account: ${accountId}, region: ${region}`);

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
