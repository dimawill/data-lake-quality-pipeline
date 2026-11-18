import * as cdk from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { DataLakeStack } from '../lib/data-lake-stack';

function synth(stage: 'dev' | 'prod') {
  const app = new cdk.App();
  return Template.fromStack(new DataLakeStack(app, `DataLakeQuality-${stage}`, { stage }));
}

describe('data lake quality stack', () => {
  it('protects four distinct buckets and scopes lifecycle and TLS', () => {
    const template = synth('dev');
    template.resourceCountIs('AWS::S3::Bucket', 4);
    template.resourceCountIs('AWS::S3::BucketPolicy', 4);
    for (const resource of Object.values(template.findResources('AWS::S3::Bucket'))) {
      expect(resource.Properties.BucketEncryption.ServerSideEncryptionConfiguration[0]
        .ServerSideEncryptionByDefault.SSEAlgorithm).toBe('AES256');
      expect(resource.Properties.PublicAccessBlockConfiguration).toEqual({
        BlockPublicAcls: true, BlockPublicPolicy: true, IgnorePublicAcls: true, RestrictPublicBuckets: true,
      });
      expect(resource.Properties.LifecycleConfiguration.Rules[0].ExpirationInDays).toBeGreaterThan(0);
    }
    template.hasResourceProperties('AWS::S3::BucketPolicy', {
      PolicyDocument: { Statement: Match.arrayWith([Match.objectLike({
        Effect: 'Deny', Condition: { Bool: { 'aws:SecureTransport': 'false' } },
      })]) },
    });
  });

  it('declares explicit projected catalog tables and bounded Glue ETL permissions', () => {
    const template = synth('dev');
    template.resourceCountIs('AWS::Glue::Database', 1);
    template.resourceCountIs('AWS::Glue::Table', 3);
    template.hasResourceProperties('AWS::Glue::Table', {
      TableInput: Match.objectLike({
        Name: 'trips', PartitionKeys: [{ Name: 'trip_date', Type: 'string' }],
        Parameters: Match.objectLike({ 'projection.enabled': 'true' }),
      }),
    });
    template.hasResourceProperties('AWS::Glue::Job', {
      GlueVersion: '4.0', Command: Match.objectLike({ Name: 'glueetl', PythonVersion: '3' }),
      ExecutionProperty: { MaxConcurrentRuns: 1 }, MaxRetries: 0, Timeout: 30,
      DefaultArguments: Match.objectLike({ '--extra-py-files': Match.anyValue() }),
    });
    const roles = Object.values(template.findResources('AWS::IAM::Role'));
    expect(roles.some(role => JSON.stringify(role.Properties.AssumeRolePolicyDocument).includes('glue.amazonaws.com'))).toBe(true);
    const policies = Object.values(template.findResources('AWS::IAM::Policy'));
    expect(policies.some(policy => JSON.stringify(policy).includes('glue:StartJobRun'))).toBe(true);
    expect(policies.some(policy => JSON.stringify(policy).includes('s3:GetObject'))).toBe(true);
    expect(JSON.stringify(policies)).not.toContain('s3:*');
    expect(JSON.stringify(policies)).not.toContain('glue:*');
  });

  it('has an opt-in schedule, failure signal, and encrypted Athena results with query cap', () => {
    const template = synth('dev');
    template.hasResourceProperties('AWS::Scheduler::Schedule', {
      State: 'DISABLED', Target: Match.objectLike({ Arn: 'arn:aws:scheduler:::aws-sdk:glue:startJobRun' }),
    });
    template.hasResourceProperties('AWS::Events::Rule', {
      EventPattern: Match.objectLike({ source: ['aws.glue'], detail: Match.objectLike({ state: ['FAILED', 'TIMEOUT'] }) }),
    });
    template.resourceCountIs('AWS::Logs::MetricFilter', 1);
    template.resourceCountIs('AWS::CloudWatch::Alarm', 1);
    template.hasResourceProperties('AWS::Athena::WorkGroup', {
      WorkGroupConfiguration: Match.objectLike({
        EnforceWorkGroupConfiguration: true, BytesScannedCutoffPerQuery: 1073741824,
        ResultConfiguration: Match.objectLike({ EncryptionConfiguration: { EncryptionOption: 'SSE_S3' } }),
      }),
    });
  });

  it('retains production data but not transient event logs', () => {
    const template = synth('prod');
    template.resourceCountIs('AWS::S3::Bucket', 4);
    for (const resource of Object.values(template.findResources('AWS::S3::Bucket'))) {
      expect(resource.DeletionPolicy).toBe('Retain');
    }
    template.hasResourceProperties('AWS::Logs::LogGroup', { RetentionInDays: 30 });
  });
});
