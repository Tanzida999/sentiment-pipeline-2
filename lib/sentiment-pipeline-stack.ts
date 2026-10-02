import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as kinesis from "aws-cdk-lib/aws-kinesis";
import * as opensearch from "aws-cdk-lib/aws-opensearchservice";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as lambdaEventSources from "aws-cdk-lib/aws-lambda-event-sources";
import * as nodejsLambda from "aws-cdk-lib/aws-lambda-nodejs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as cwActions from "aws-cdk-lib/aws-cloudwatch-actions";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subscriptions from "aws-cdk-lib/aws-sns-subscriptions";
import * as logs from "aws-cdk-lib/aws-logs";
import * as path from "path";

export class SentimentPipelineStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ---------------------------------------------------------------------
    // 1. Ingest: Kinesis Data Stream for incoming social posts
    // ---------------------------------------------------------------------
    const postsStream = new kinesis.Stream(this, "SocialPostsStream", {
      streamName: "social-posts-stream",
      shardCount: 1, // on-demand-ish cost; bump for real load
      retentionPeriod: cdk.Duration.hours(24),
      streamMode: kinesis.StreamMode.PROVISIONED,
    });

    // ---------------------------------------------------------------------
    // 2. Storage & search: OpenSearch domain that holds scored posts
    //    NOTE: t3.small.search keeps this within/near AWS free tier.
    // ---------------------------------------------------------------------
    const openSearchDomain = new opensearch.Domain(this, "SentimentDomain", {
      version: opensearch.EngineVersion.OPENSEARCH_2_11,
      domainName: "sentiment-dashboard",
      capacity: {
        dataNodes: 1,
        dataNodeInstanceType: "t3.small.search",
      },
      ebs: {
        volumeSize: 10,
        volumeType: cdk.aws_ec2.EbsDeviceVolumeType.GP3,
      },
      nodeToNodeEncryption: true,
      encryptionAtRest: { enabled: true },
      enforceHttps: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY, // portfolio project: easy teardown
    });

    // ---------------------------------------------------------------------
    // 3. Compute: Lambda that consumes Kinesis records, calls Comprehend,
    //    writes the scored document into OpenSearch, and emits a custom
    //    CloudWatch metric for negative-sentiment volume.
    // ---------------------------------------------------------------------
    const consumerFn = new nodejsLambda.NodejsFunction(this, "SentimentConsumerFn", {
      functionName: "sentiment-consumer",
      entry: path.join(__dirname, "../lambda/consumer/index.ts"),
      handler: "handler",
      runtime: lambda.Runtime.NODEJS_22_X,
      memorySize: 256,
      timeout: cdk.Duration.seconds(30),
      logGroup: new logs.LogGroup(this, "SentimentConsumerLogGroup", {
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
      environment: {
        OPENSEARCH_ENDPOINT: openSearchDomain.domainEndpoint,
        OPENSEARCH_INDEX: "sentiment-posts",
        METRIC_NAMESPACE: "SentimentPipeline",
      },
      bundling: {
        minify: true,
        sourceMap: true,
      },
    });

    // Comprehend permissions (least privilege: DetectSentiment only)
    consumerFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["comprehend:DetectSentiment"],
        resources: ["*"], // Comprehend does not support resource-level scoping
      })
    );

    // CloudWatch custom metrics
    consumerFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["cloudwatch:PutMetricData"],
        resources: ["*"],
      })
    );

    // Let the Lambda's execution role talk to OpenSearch over HTTPS (IAM auth)
    openSearchDomain.grantReadWrite(consumerFn);

    // Kinesis -> Lambda event source (small batches, short window for near-real-time)
    consumerFn.addEventSource(
      new lambdaEventSources.KinesisEventSource(postsStream, {
        startingPosition: lambda.StartingPosition.LATEST,
        batchSize: 25,
        maxBatchingWindow: cdk.Duration.seconds(5),
        retryAttempts: 2,
        bisectBatchOnError: true,
      })
    );

    // ---------------------------------------------------------------------
    // 4. Alerting: SNS topic + CloudWatch alarms
    // ---------------------------------------------------------------------
    const alarmTopic = new sns.Topic(this, "SentimentAlarmTopic", {
      topicName: "sentiment-pipeline-alarms",
    });
    // Subscribe your own email by uncommenting and editing:
    // alarmTopic.addSubscription(new subscriptions.EmailSubscription("you@example.com"));

    // 4a. Pipeline health: Kinesis iterator age (consumer falling behind)
    const iteratorAgeAlarm = new cloudwatch.Alarm(this, "IteratorAgeAlarm", {
      metric: postsStream.metric("GetRecords.IteratorAgeMilliseconds", {
        statistic: "Maximum",
        period: cdk.Duration.minutes(5),
      }),
      threshold: 60_000, // 1 minute of lag
      evaluationPeriods: 2,
      alarmDescription: "Consumer Lambda is falling behind the Kinesis stream.",
    });
    iteratorAgeAlarm.addAlarmAction(new cwActions.SnsAction(alarmTopic));

    // 4b. Pipeline health: Lambda errors
    const lambdaErrorsAlarm = new cloudwatch.Alarm(this, "ConsumerErrorsAlarm", {
      metric: consumerFn.metricErrors({ period: cdk.Duration.minutes(5) }),
      threshold: 5,
      evaluationPeriods: 1,
      alarmDescription: "Sentiment consumer Lambda is erroring out.",
    });
    lambdaErrorsAlarm.addAlarmAction(new cwActions.SnsAction(alarmTopic));

    // 4c. Business/anomaly signal: spike in negative-sentiment posts
    const negativeSentimentMetric = new cloudwatch.Metric({
      namespace: "SentimentPipeline",
      metricName: "NegativeSentimentCount",
      statistic: "Sum",
      period: cdk.Duration.minutes(5),
    });
    const negativeSpikeAlarm = new cloudwatch.Alarm(this, "NegativeSentimentSpikeAlarm", {
      metric: negativeSentimentMetric,
      threshold: 20, // tune to your simulated traffic volume
      evaluationPeriods: 1,
      alarmDescription: "Spike in negative-sentiment posts detected — possible brand/PR issue.",
    });
    negativeSpikeAlarm.addAlarmAction(new cwActions.SnsAction(alarmTopic));

    // ---------------------------------------------------------------------
    // 5. Dashboard: single-pane view for the demo / interview screenshot
    // ---------------------------------------------------------------------
    const dashboard = new cloudwatch.Dashboard(this, "SentimentDashboard", {
      dashboardName: "sentiment-pipeline",
    });

    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: "Incoming records (Kinesis)",
        left: [postsStream.metric("IncomingRecords", { statistic: "Sum" })],
        width: 8,
      }),
      new cloudwatch.GraphWidget({
        title: "Consumer Lambda invocations / errors",
        left: [consumerFn.metricInvocations(), consumerFn.metricErrors()],
        width: 8,
      }),
      new cloudwatch.GraphWidget({
        title: "Sentiment mix (custom metrics)",
        left: [
          new cloudwatch.Metric({
            namespace: "SentimentPipeline",
            metricName: "PositiveSentimentCount",
            statistic: "Sum",
          }),
          new cloudwatch.Metric({
            namespace: "SentimentPipeline",
            metricName: "NegativeSentimentCount",
            statistic: "Sum",
          }),
          new cloudwatch.Metric({
            namespace: "SentimentPipeline",
            metricName: "NeutralSentimentCount",
            statistic: "Sum",
          }),
        ],
        width: 8,
      }),
      new cloudwatch.GraphWidget({
        title: "Consumer lag (IteratorAge, ms)",
        left: [postsStream.metric("GetRecords.IteratorAgeMilliseconds", { statistic: "Maximum" })],
        width: 12,
      })
    );

    // ---------------------------------------------------------------------
    // Outputs
    // ---------------------------------------------------------------------
    new cdk.CfnOutput(this, "KinesisStreamName", { value: postsStream.streamName });
    new cdk.CfnOutput(this, "OpenSearchDashboardsUrl", {
      value: `https://${openSearchDomain.domainEndpoint}/_dashboards/`,
    });
    new cdk.CfnOutput(this, "CloudWatchDashboardUrl", {
      value: `https://${this.region}.console.aws.amazon.com/cloudwatch/home?region=${this.region}#dashboards:name=${dashboard.dashboardName}`,
    });
  }
}
