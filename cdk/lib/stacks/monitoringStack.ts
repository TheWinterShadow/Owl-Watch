import { Stack, StackProps } from 'aws-cdk-lib';
import { CfnCrawler, CfnJob } from 'aws-cdk-lib/aws-glue';
import { Function as LFunction } from 'aws-cdk-lib/aws-lambda';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import { MonitoringFacade } from 'cdk-monitoring-constructs';

interface MonitoringStackProps extends StackProps {
  s3Buckets: Bucket[];
  lambdaFunctions: LFunction[];
  glueJobs: CfnJob[];
  glueCrawlers: CfnCrawler[];
}

export class MonitoringStack extends Stack {
  constructor(scope: Construct, id: string, props: MonitoringStackProps) {
    super(scope, id, props);

    // Monitoring Dashboard
    const dashboard = new MonitoringFacade(this, 'MonitoringDashboard');

    // S3 Bucket Monitoring
    for (const bucket of props.s3Buckets) {
      dashboard.monitorS3Bucket({ bucket: bucket });
    }

    // Lambda Monitoring
    for (const fn of props.lambdaFunctions) {
      dashboard.monitorLambdaFunction({ lambdaFunction: fn });
    }

    // Glue Job Monitoring
    for (const job of props.glueJobs) {
      dashboard.monitorGlueJob({ jobName: job.name as string });
    }
  }
}
