import { Stack, StackProps } from 'aws-cdk-lib';
import { ManagedPolicy, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import { createGlueCrawler, createGlueJob } from '../utils/createAsset';
import { GlueCrawlerConfig, GlueJobConfig } from '../utils/types';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { CfnCrawler, CfnJob } from 'aws-cdk-lib/aws-glue';

interface GlueStackProps extends StackProps {
  assetBucket: Bucket;
}

export class GlueStack extends Stack {
  public readonly glueJobs: CfnJob[] = [];
  public readonly glueCrawlers: CfnCrawler[] = [];

  constructor(scope: Construct, id: string, props: GlueStackProps) {
    super(scope, id, props);

    // Example IAM role for Glue
    const glueRole = this.createGlueRole();

    // Example: Deploy Glue Jobs
    const glueJobConfigs: GlueJobConfig[] = this.createGlueJobConfigs();

    const glueCrawlerConfigs: GlueCrawlerConfig[] = this.createGlueCrawlerConfigs();

    glueJobConfigs.forEach((jobConfig) => {
      const glueJob = createGlueJob(this, props, jobConfig, glueRole);
      this.glueJobs.push(glueJob);
    });

    glueCrawlerConfigs.forEach((crawlerConfig) => {
      const glueCrawler = createGlueCrawler(
        this,
        crawlerConfig.name + 'Crawler',
        glueRole,
        '',
        crawlerConfig.schedule,
        crawlerConfig.targets,
      );
      this.glueCrawlers.push(glueCrawler);
    });
  }

  private createGlueJobConfigs(): GlueJobConfig[] {
    return [];
  }

  private createGlueCrawlerConfigs(): GlueCrawlerConfig[] {
    return [];
  }

  private createGlueRole(): Role {
    return new Role(this, 'GlueJobRole', {
      assumedBy: new ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')],
    });
  }
}
