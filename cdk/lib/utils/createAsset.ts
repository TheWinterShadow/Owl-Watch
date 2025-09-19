import { Duration, RemovalPolicy, StackProps } from 'aws-cdk-lib';
import { CfnCrawler, CfnDatabase, CfnJob, CfnTable } from 'aws-cdk-lib/aws-glue';
import { Role } from 'aws-cdk-lib/aws-iam';
import { ApplicationLogLevel, Code, Function as LFunction, Runtime } from 'aws-cdk-lib/aws-lambda';
import { BlockPublicAccess, Bucket, BucketEncryption, LifecycleRule } from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import { GlueJobConfig } from './types';
import { Topic } from 'aws-cdk-lib/aws-sns';

export function createS3Bucket(scope: Construct, bucketName: string, lifecycleRules: LifecycleRule[] = []): Bucket {
  return new Bucket(scope, bucketName, {
    bucketName: `${bucketName}`.toLowerCase(),
    blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
    versioned: true,
    encryption: BucketEncryption.S3_MANAGED,
    lifecycleRules: lifecycleRules,
    enforceSSL: true,
    removalPolicy: RemovalPolicy.DESTROY,
  });
}

export function createGlueDatabase(scope: Construct, database_name: string, props: StackProps) {
  return new CfnDatabase(scope, database_name, {
    catalogId: props.env?.account || '',
    databaseInput: {
      name: database_name,
      description: 'Database for Owl Watch crawler data',
    },
    databaseName: database_name,
  });
}

export function createGlueTable(
  scope: Construct,
  data_base: string,
  table_name: string,
  s3_location: string,
  table_columns: CfnTable.ColumnProperty[],
  props: StackProps,
) {
  return new CfnTable(scope, table_name, {
    databaseName: data_base,
    catalogId: props.env?.account || '',
    tableInput: {
      name: table_name,
      description: `Table for schema type ${table_name} data`,
      tableType: 'EXTERNAL_TABLE',
      parameters: {
        classification: 'parquet',
        typeOfData: 'file',
        'partition_filtering.enabled': 'true',
      },
      storageDescriptor: {
        location: s3_location,
        inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        serdeInfo: {
          serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
          parameters: {
            'serialization.format': '1',
          },
        },
        columns: table_columns,
        storedAsSubDirectories: false,
      },
      partitionKeys: [
        { name: 'year', type: 'string' },
        { name: 'month', type: 'string' },
        { name: 'day', type: 'string' },
        { name: 'data_source', type: 'string' },
        { name: 'dataset_name', type: 'string' },
      ],
    },
  });
}

export function createGlueCrawler(
  scope: Construct,
  crawlerName: string,
  crawlerRole: Role,
  databaseName: string,
  schedule: CfnCrawler.ScheduleProperty,
  targetConfig: CfnCrawler.TargetsProperty,
): CfnCrawler {
  return new CfnCrawler(scope, crawlerName, {
    role: crawlerRole.roleArn,
    targets: targetConfig,
    databaseName: databaseName,
    name: crawlerName,
    schemaChangePolicy: {
      updateBehavior: 'LOG',
      deleteBehavior: 'LOG',
    },
    recrawlPolicy: { recrawlBehavior: 'CRAWL_EVERYTHING' },
    // This is the critical configuration to preserve schema
    configuration: JSON.stringify({
      Version: 1.0,
      CrawlerOutput: {
        Partitions: {
          AddOrUpdateBehavior: 'InheritFromTable',
        },
      },
      Grouping: {
        TableGroupingPolicy: 'CombineCompatibleSchemas',
      },
      CreatePartitionIndex: false,
    }),
    schedule: schedule,
  });
}

/**
 * Creates a secure Lambda function with appropriate security settings
 */
export function createLambdaFunction(
  scope: Construct,
  stage: string,
  lambdaName: string,
  filePath: string,
  handlerPath: string,
  memory: number,
  timeout: Duration,
  runTime: Runtime,
  environmentVariables: { [key: string]: string },
): LFunction {
  return new LFunction(scope, lambdaName, {
    functionName: lambdaName,
    description: `Timestamp: ${new Date().toISOString()}`,
    code: Code.fromAsset(filePath),
    applicationLogLevelV2: ApplicationLogLevel.DEBUG,
    handler: handlerPath,
    memorySize: memory,
    timeout: timeout,
    runtime: runTime,
    environment: environmentVariables,
    retryAttempts: 2,
  });
}

export function createGlueJob(scope: Construct, props: StackProps, config: GlueJobConfig, role: Role): CfnJob {
  return new CfnJob(scope, config.name, {
    name: config.name,
    role: role.roleArn,
    command: {
      name: 'glueetl',
      scriptLocation: config.scriptPath,
      pythonVersion: '3',
    },
    description: config.description,
    maxRetries: config.maxRetries || 0,
    timeout: config.timeout || 180,
    workerType: config.workerType || 'G.1X',
    numberOfWorkers: config.numberOfWorkers || 2,
    defaultArguments: {
      '--enable-metrics': '',
      '--enable-continuous-cloudwatch-log': 'true',
      '--job-language': 'python',
      '--job-run-retention-days': '14',
      '--enable-spark-ui': 'true', // Enable Spark UI
      '--spark-event-logs-path': `s3://owl-watch/sparkLogs/`,
      '--extra-py-files': `s3://owl-watch/python-libs/owl_watch.zip`,
      '--POWERTOOLS_LOG_LEVEL': 'DEBUG',
      '--conf': [
        'spark.serializer=org.apache.spark.serializer.KryoSerializer',
        'spark.sql.adaptive.enabled=true',
        'spark.sql.adaptive.coalescePartitions.enabled=true',
        'spark.sql.adaptive.advisoryPartitionSizeInBytes=16MB', // Small partitions
        'spark.sql.adaptive.skewJoin.enabled=true',
        'spark.sql.execution.arrow.pyspark.enabled=true',
        'spark.sql.adaptive.localShuffleReader.enabled=true',
        'spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold=0',
        'spark.sql.parquet.int96RebaseModeInWrite=LEGACY',
      ].join(' --conf '),
      '--additional-python-modules':
        'aws-lambda-powertools==2.0.0,mypy_boto3_s3,mypy_boto3,mypy_boto3_sts,mypy_boto3_athena,mypy_boto3_glue,mypy_boto3_iam,mypy_boto3_sqs',
      ...config.defaultArguments,
    },
    glueVersion: '5.0',
    executionProperty: {
      maxConcurrentRuns: config.concurrentRuns || 3,
    },
  });
}

export function createSnsTopic(scope: Construct, topicName: string): Topic {
  return new Topic(scope, topicName, {
    displayName: topicName,
    topicName: topicName,
  });
}
