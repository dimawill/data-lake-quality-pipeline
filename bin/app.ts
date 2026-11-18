#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { DataLakeStack, type Stage } from '../lib/data-lake-stack';

const app = new cdk.App();
const stage = app.node.tryGetContext('stage') as string;
if (stage !== 'dev' && stage !== 'prod') {
  throw new Error('Set -c stage=dev or -c stage=prod');
}
new DataLakeStack(app, `DataLakeQuality-${stage}`, {
  stage: stage as Stage,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
