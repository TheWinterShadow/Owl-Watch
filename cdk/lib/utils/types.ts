import { StackProps } from 'aws-cdk-lib';
import { CfnCrawler } from 'aws-cdk-lib/aws-glue';
import { Role } from 'aws-cdk-lib/aws-iam';

export interface StageConfig {
  name: string;
  accountId: string;
  region: string;
}

export interface OwlWatchStackProps extends StackProps {
  stageName: string;
  accountId: string;
  region: string;
}

export interface GlueJobConfig {
  name: string;
  description: string;
  scriptPath: string;
  role: Role;
  tempDir: string;
  jobType: string; // e.g., 'glueetl'
  concurrentRuns?: number;
  maxRetries?: number;
  timeout?: number; // in minutes
  workerType?: string; // e.g., 'G.1X', 'G.2X'
  numberOfWorkers?: number;
  defaultArguments?: { [key: string]: string };
  extraArgs?: { [key: string]: string };
}

export interface GlueCrawlerConfig {
  name: string;
  role: Role;
  databaseName: string;
  targets: CfnCrawler.TargetsProperty;
  schedule: CfnCrawler.ScheduleProperty;
}
