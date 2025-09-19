import { App } from 'aws-cdk-lib';
import { DataStack } from './stacks/dataStack';

const app = new App();
new DataStack(app, 'DataStack');
