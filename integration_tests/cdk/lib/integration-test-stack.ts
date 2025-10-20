import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import * as path from 'path';

export interface IntegrationTestStackProps extends cdk.StackProps {
  environment: string;
}

export class IntegrationTestStack extends cdk.Stack {
  public readonly testRunnerFunction: lambda.Function;
  public readonly inputBucket: s3.Bucket;
  public readonly outputBucket: s3.Bucket;
  public readonly testResultsBucket: s3.Bucket;

  constructor(scope: Construct, id: string, props: IntegrationTestStackProps) {
    super(scope, id, props);

    const { environment } = props;

    // S3 Buckets for test data
    this.inputBucket = new s3.Bucket(this, 'InputBucket', {
      bucketName: `owl-watch-integration-tests-input-${environment}-${this.region}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      lifecycleRules: [
        {
          id: 'TestDataCleanup',
          enabled: true,
          expiration: cdk.Duration.days(7), // Clean up test data after 7 days
        },
      ],
    });

    this.outputBucket = new s3.Bucket(this, 'OutputBucket', {
      bucketName: `owl-watch-integration-tests-output-${environment}-${this.region}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      lifecycleRules: [
        {
          id: 'TestOutputCleanup',
          enabled: true,
          expiration: cdk.Duration.days(7),
        },
      ],
    });

    this.testResultsBucket = new s3.Bucket(this, 'TestResultsBucket', {
      bucketName: `owl-watch-integration-tests-results-${environment}-${this.region}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      lifecycleRules: [
        {
          id: 'TestResultsCleanup',
          enabled: true,
          expiration: cdk.Duration.days(30), // Keep results longer for analysis
        },
      ],
    });

    // CloudWatch Log Group
    const logGroup = new logs.LogGroup(this, 'TestRunnerLogGroup', {
      logGroupName: `/aws/lambda/integration-tests-${environment}`,
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // IAM Role for Lambda
    const lambdaRole = new iam.Role(this, 'TestRunnerRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
      inlinePolicies: {
        TestRunnerPolicy: new iam.PolicyDocument({
          statements: [
            // S3 permissions
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                's3:GetObject',
                's3:PutObject',
                's3:DeleteObject',
                's3:ListBucket',
              ],
              resources: [
                this.inputBucket.bucketArn,
                `${this.inputBucket.bucketArn}/*`,
                this.outputBucket.bucketArn,
                `${this.outputBucket.bucketArn}/*`,
                this.testResultsBucket.bucketArn,
                `${this.testResultsBucket.bucketArn}/*`,
              ],
            }),
            // Glue permissions for monitoring jobs
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'glue:GetJob',
                'glue:GetJobRun',
                'glue:GetJobRuns',
                'glue:ListJobs',
              ],
              resources: ['*'], // TODO: Restrict to specific Glue jobs
            }),
            // CloudWatch Logs permissions
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'logs:CreateLogGroup',
                'logs:CreateLogStream',
                'logs:PutLogEvents',
              ],
              resources: [logGroup.logGroupArn],
            }),
          ],
        }),
      },
    });

    // Lambda Function
    this.testRunnerFunction = new lambda.Function(this, 'TestRunnerFunction', {
      functionName: `owl-watch-integration-test-runner-${environment}`,
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'index.lambda_handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/test-runner')),
      role: lambdaRole,
      timeout: cdk.Duration.minutes(15), // 15 minute timeout for Glue job completion
      memorySize: 512,
      environment: {
        ENVIRONMENT: environment,
        INPUT_BUCKET: this.inputBucket.bucketName,
        OUTPUT_BUCKET: this.outputBucket.bucketName,
        RESULTS_BUCKET: this.testResultsBucket.bucketName,
        LOG_GROUP_NAME: logGroup.logGroupName,
        // TODO: Add your Glue job names here
        GLUE_JOB_NAME_CSV_TRANSFORM: 'owl-watch-csv-transform-job', // TODO: Replace with actual job name
      },
      logGroup: logGroup,
    });

    // Outputs
    new cdk.CfnOutput(this, 'TestRunnerFunctionName', {
      value: this.testRunnerFunction.functionName,
      description: 'Name of the integration test runner Lambda function',
      exportName: `${id}-TestRunnerFunctionName`,
    });

    new cdk.CfnOutput(this, 'InputBucketName', {
      value: this.inputBucket.bucketName,
      description: 'S3 bucket for test input data',
      exportName: `${id}-InputBucketName`,
    });

    new cdk.CfnOutput(this, 'OutputBucketName', {
      value: this.outputBucket.bucketName,
      description: 'S3 bucket for test output data',
      exportName: `${id}-OutputBucketName`,
    });

    new cdk.CfnOutput(this, 'TestResultsBucketName', {
      value: this.testResultsBucket.bucketName,
      description: 'S3 bucket for test results',
      exportName: `${id}-TestResultsBucketName`,
    });

    // Tags
    cdk.Tags.of(this).add('Project', 'OwlWatch');
    cdk.Tags.of(this).add('Component', 'IntegrationTests');
    cdk.Tags.of(this).add('Environment', environment);
  }
}