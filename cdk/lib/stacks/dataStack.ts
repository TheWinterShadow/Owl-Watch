import { Duration, Stack } from 'aws-cdk-lib';
import * as path from 'path';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import { createS3Bucket } from '../utils/createAsset';
import { BucketDeployment, Source } from 'aws-cdk-lib/aws-s3-deployment';
import { OwlWatchStackProps } from '../utils/types';

export class DataStack extends Stack {
  public readonly dataBucket: Bucket;

  constructor(scope: Construct, id: string, props: OwlWatchStackProps) {
    super(scope, id, props);

    this.dataBucket = createS3Bucket(this, props.stageName, 'owl-watch-bucket', [
      {
        id: 'RawDataLifecycleRule',
        enabled: true,
        expiration: Duration.days(30),
        prefix: 'raw/',
      },
      {
        id: 'CleanedDataLifecycleRule',
        enabled: true,
        expiration: Duration.days(90),
        prefix: 'cleaned/',
      },
      {
        id: 'CuratedDataLifecycleRule',
        enabled: true,
        expiration: Duration.days(365),
        prefix: 'curated/',
      },
    ]);

    new BucketDeployment(this, 'DeployInitialData', {
      sources: [Source.asset(path.resolve(__dirname, '../../../execution/etl'))],
      destinationBucket: this.dataBucket,
      destinationKeyPrefix: 'glue_assets/etl_scripts/', // optional prefix in destination bucket
    });
  }
}
