#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { SentimentPipelineStack } from "../lib/sentiment-pipeline-stack";

const app = new cdk.App();

new SentimentPipelineStack(app, "SentimentPipelineStack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || "ap-northeast-1",
  },
  description:
    "Real-time social media sentiment analysis pipeline: Kinesis -> Lambda -> Comprehend -> OpenSearch -> CloudWatch",
});
