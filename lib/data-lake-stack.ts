import * as path from 'node:path';
import * as cdk from 'aws-cdk-lib';
import * as athena from 'aws-cdk-lib/aws-athena';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as events from 'aws-cdk-lib/aws-events';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as assets from 'aws-cdk-lib/aws-s3-assets';
import * as scheduler from 'aws-cdk-lib/aws-scheduler';
import { Construct } from 'constructs';

export type Stage = 'dev' | 'prod';
export interface DataLakeStackProps extends cdk.StackProps {
  stage: Stage;
}

export class DataLakeStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: DataLakeStackProps) {
    super(scope, id, props);
    const removalPolicy = props.stage === 'prod' ? cdk.RemovalPolicy.RETAIN : cdk.RemovalPolicy.DESTROY;
    const bucket = (name: string, days: number) => new s3.Bucket(this, name, {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      lifecycleRules: [{ expiration: cdk.Duration.days(days), abortIncompleteMultipartUploadAfter: cdk.Duration.days(7) }],
      removalPolicy,
    });
    const raw = bucket('RawBucket', 90);
    const curated = bucket('CuratedBucket', 365);
    const rejected = bucket('RejectedBucket', 30);
    const results = bucket('AthenaResultsBucket', 7);

    const database = new glue.CfnDatabase(this, 'CatalogDatabase', {
      catalogId: this.account,
      databaseInput: { name: `data_lake_quality_${props.stage}` },
    });
    const table = (id: string, name: string, location: string, columns: glue.CfnTable.ColumnProperty[],
      inputFormat: string, outputFormat: string, serde: string, partitionKeys?: glue.CfnTable.ColumnProperty[],
      parameters?: Record<string, string>) => {
      const resource = new glue.CfnTable(this, id, {
        catalogId: this.account,
        databaseName: database.ref,
        tableInput: {
          name,
          tableType: 'EXTERNAL_TABLE',
          parameters: { EXTERNAL: 'TRUE', ...parameters },
          partitionKeys,
          storageDescriptor: {
            location,
            columns,
            inputFormat,
            outputFormat,
            serdeInfo: { serializationLibrary: serde },
          },
        },
      });
      resource.addResourceDependency(database);
    };
    const parquetInput = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat';
    const parquetOutput = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';
    const parquetSerde = 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe';
    table('TripsTable', 'trips', `s3://${curated.bucketName}/trips/`, [
      { name: 'trip_id', type: 'string' },
      { name: 'trip_start_timestamp', type: 'timestamp' },
      { name: 'trip_miles', type: 'double' },
      { name: 'fare', type: 'double' },
    ], parquetInput, parquetOutput, parquetSerde, [{ name: 'trip_date', type: 'string' }], {
      'projection.enabled': 'true',
      'projection.trip_date.type': 'date',
      'projection.trip_date.range': '2010/01/01,NOW',
      'projection.trip_date.format': 'yyyy-MM-dd',
      'projection.trip_date.interval': '1',
      'projection.trip_date.interval.unit': 'DAYS',
      'storage.location.template': `s3://${curated.bucketName}/trips/trip_date=\${trip_date}/`,
    });
    table('RejectedTable', 'rejected_trips', `s3://${rejected.bucketName}/trips/`, [
      { name: 'trip_id', type: 'string' }, { name: 'reason', type: 'string' },
      { name: 'raw_record', type: 'string' }, { name: 'source_file', type: 'string' },
    ], parquetInput, parquetOutput, parquetSerde);
    table('QualityTable', 'quality_reports', `s3://${curated.bucketName}/quality/summary/`, [
      { name: 'run_id', type: 'string' }, { name: 'generated_at', type: 'string' },
      { name: 'processed', type: 'bigint' }, { name: 'accepted', type: 'bigint' },
      { name: 'invalid', type: 'bigint' }, { name: 'duplicates', type: 'bigint' },
      { name: 'rejected', type: 'bigint' }, { name: 'rejection_rate', type: 'double' },
    ], 'org.apache.hadoop.mapred.TextInputFormat', 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
    'org.openx.data.jsonserde.JsonSerDe');

    const script = new assets.Asset(this, 'EtlScript', { path: path.join(__dirname, '../glue/job.py') });
    const pythonLibrary = new assets.Asset(this, 'ValidationLibrary', { path: path.join(__dirname, '../glue/quality.py') });
    const role = new iam.Role(this, 'GlueJobRole', { assumedBy: new iam.ServicePrincipal('glue.amazonaws.com') });
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:GetBucketLocation'],
      resources: [raw.bucketArn, curated.bucketArn, rejected.bucketArn, script.bucket.bucketArn, pythonLibrary.bucket.bucketArn],
    }));
    for (const zone of [raw, curated, rejected]) {
      role.addToPolicy(new iam.PolicyStatement({
        actions: ['s3:ListBucket'], resources: [zone.bucketArn],
        conditions: { StringLike: { 's3:prefix': ['trips', 'trips/*'] } },
      }));
    }
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:GetObject'],
      resources: [raw.arnForObjects('trips/*'), curated.arnForObjects('trips/*'),
        rejected.arnForObjects('trips/*'), script.bucket.arnForObjects(script.s3ObjectKey),
        pythonLibrary.bucket.arnForObjects(pythonLibrary.s3ObjectKey)],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:PutObject', 's3:DeleteObject', 's3:AbortMultipartUpload'],
      resources: [curated.arnForObjects('trips/*'), rejected.arnForObjects('trips/*')],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:PutObject'], resources: [curated.arnForObjects('quality/summary/*')],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
      resources: [this.formatArn({ service: 'logs', resource: 'log-group', resourceName: '/aws-glue/jobs/*', arnFormat: cdk.ArnFormat.COLON_RESOURCE_NAME })],
    }));
    const job = new glue.CfnJob(this, 'QualityJob', {
      name: `data-lake-quality-${props.stage}`,
      role: role.roleArn,
      glueVersion: '4.0',
      command: { name: 'glueetl', pythonVersion: '3', scriptLocation: script.s3ObjectUrl },
      defaultArguments: {
        '--job-language': 'python',
        '--enable-job-insights': 'true',
        '--enable-continuous-cloudwatch-log': 'true',
        '--extra-py-files': pythonLibrary.s3ObjectUrl,
        '--RAW_BUCKET': raw.bucketName,
        '--CURATED_BUCKET': curated.bucketName,
        '--REJECTED_BUCKET': rejected.bucketName,
      },
      workerType: 'G.1X', numberOfWorkers: 2,
      executionProperty: { maxConcurrentRuns: 1 },
      timeout: 30,
      maxRetries: 0,
    });
    job.addResourceDependency(role.node.defaultChild as iam.CfnRole);

    const scheduleRole = new iam.Role(this, 'ScheduleRole', {
      assumedBy: new iam.ServicePrincipal('scheduler.amazonaws.com', {
        conditions: { StringEquals: { 'aws:SourceAccount': this.account } },
      }),
    });
    scheduleRole.addToPolicy(new iam.PolicyStatement({
      actions: ['glue:StartJobRun'],
      resources: [this.formatArn({ service: 'glue', resource: 'job', resourceName: job.ref })],
    }));
    const schedule = new scheduler.CfnSchedule(this, 'DisabledSchedule', {
      state: 'DISABLED',
      scheduleExpression: 'cron(0 6 ? * MON *)',
      scheduleExpressionTimezone: 'Etc/UTC',
      flexibleTimeWindow: { mode: 'OFF' },
      target: {
        arn: 'arn:aws:scheduler:::aws-sdk:glue:startJobRun',
        roleArn: scheduleRole.roleArn,
        input: this.toJsonString({ JobName: job.ref }),
      },
    });
    schedule.addResourceDependency(scheduleRole.node.defaultChild as iam.CfnRole);

    const failures = new logs.LogGroup(this, 'JobFailureEvents', {
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const logPolicy = new logs.CfnResourcePolicy(this, 'EventsLogDeliveryPolicy', {
      policyName: `DataLakeQuality-${props.stage}-events`,
      policyDocument: this.toJsonString({
        Version: '2012-10-17',
        Statement: [{
          Sid: 'AllowEventBridgeJobFailures', Effect: 'Allow',
          Principal: { Service: ['events.amazonaws.com', 'delivery.logs.amazonaws.com'] },
          Action: ['logs:CreateLogStream', 'logs:PutLogEvents'],
          Resource: `${failures.logGroupArn}:*`,
          Condition: { StringEquals: { 'aws:SourceAccount': this.account } },
        }],
      }),
    });
    const failureRule = new events.CfnRule(this, 'FailedJobRule', {
      eventPattern: {
        source: ['aws.glue'], 'detail-type': ['Glue Job State Change'],
        detail: { jobName: [job.ref], state: ['FAILED', 'TIMEOUT'] },
      },
      targets: [{ arn: failures.logGroupArn, id: 'JobFailureLogs' }],
    });
    failureRule.addResourceDependency(logPolicy);
    new logs.MetricFilter(this, 'JobFailureMetric', {
      logGroup: failures,
      metricNamespace: `DataLakeQuality/${props.stage}`,
      metricName: 'FailedJobRuns',
      metricValue: '1',
      filterPattern: logs.FilterPattern.allEvents(),
    });
    new cloudwatch.Alarm(this, 'JobFailureAlarm', {
      metric: new cloudwatch.Metric({
        namespace: `DataLakeQuality/${props.stage}`, metricName: 'FailedJobRuns',
        statistic: 'Sum', period: cdk.Duration.minutes(5),
      }),
      threshold: 1, evaluationPeriods: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    new athena.CfnWorkGroup(this, 'AthenaWorkGroup', {
      name: `data-lake-quality-${props.stage}`,
      state: 'ENABLED',
      workGroupConfiguration: {
        enforceWorkGroupConfiguration: true,
        publishCloudWatchMetricsEnabled: true,
        bytesScannedCutoffPerQuery: 1_073_741_824,
        resultConfiguration: {
          outputLocation: `s3://${results.bucketName}/queries/`,
          encryptionConfiguration: { encryptionOption: 'SSE_S3' },
        },
      },
    });
    new cdk.CfnOutput(this, 'RawBucketName', { value: raw.bucketName });
    new cdk.CfnOutput(this, 'CuratedBucketName', { value: curated.bucketName });
    new cdk.CfnOutput(this, 'RejectedBucketName', { value: rejected.bucketName });
    new cdk.CfnOutput(this, 'AthenaResultsBucketName', { value: results.bucketName });
    new cdk.CfnOutput(this, 'DatabaseName', { value: database.ref });
    new cdk.CfnOutput(this, 'GlueJobName', { value: job.ref });
    new cdk.CfnOutput(this, 'WorkGroupName', { value: `data-lake-quality-${props.stage}` });
  }
}
