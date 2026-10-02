import { KinesisStreamEvent, KinesisStreamHandler } from "aws-lambda";
import {
  ComprehendClient,
  DetectSentimentCommand,
  SentimentType,
} from "@aws-sdk/client-comprehend";
import { CloudWatchClient, PutMetricDataCommand } from "@aws-sdk/client-cloudwatch";
import { Client } from "@opensearch-project/opensearch";
import { AwsSigv4Signer } from "@opensearch-project/opensearch/aws";
import { defaultProvider } from "@aws-sdk/credential-provider-node";

const REGION = process.env.AWS_REGION || "ap-northeast-1";
const OPENSEARCH_ENDPOINT = process.env.OPENSEARCH_ENDPOINT!;
const OPENSEARCH_INDEX = process.env.OPENSEARCH_INDEX || "sentiment-posts";
const METRIC_NAMESPACE = process.env.METRIC_NAMESPACE || "SentimentPipeline";

const comprehend = new ComprehendClient({ region: REGION });
const cloudwatch = new CloudWatchClient({ region: REGION });

const osClient = new Client({
  ...AwsSigv4Signer({
    region: REGION,
    service: "es",
    getCredentials: () => defaultProvider()(),
  }),
  node: `https://${OPENSEARCH_ENDPOINT}`,
});

interface SocialPost {
  id: string;
  author: string;
  topic: string;
  text: string;
  timestamp: string;
}

/**
 * Kinesis batches records as base64-encoded JSON. Each record is expected
 * to be a SocialPost produced by producer/generate-posts.ts.
 */
export const handler: KinesisStreamHandler = async (event: KinesisStreamEvent) => {
  const counts: Record<string, number> = {
    POSITIVE: 0,
    NEGATIVE: 0,
    NEUTRAL: 0,
    MIXED: 0,
  };

  for (const record of event.Records) {
    const payload = Buffer.from(record.kinesis.data, "base64").toString("utf-8");

    let post: SocialPost;
    try {
      post = JSON.parse(payload);
    } catch (err) {
      console.error("Skipping malformed record", { payload, err });
      continue;
    }

    if (!post.text) {
      console.error("Skipping record with no text field", { post });
      continue;
    }

    try {
      const sentimentResult = await comprehend.send(
        new DetectSentimentCommand({
          Text: post.text.slice(0, 5000), // Comprehend's per-request limit
          LanguageCode: "en",
        })
      );

      const sentiment = (sentimentResult.Sentiment ?? "NEUTRAL") as SentimentType;
      counts[sentiment] = (counts[sentiment] ?? 0) + 1;

      await osClient.index({
        index: OPENSEARCH_INDEX,
        body: {
          id: post.id,
          author: post.author,
          topic: post.topic,
          text: post.text,
          timestamp: post.timestamp,
          sentiment,
          sentimentScore: sentimentResult.SentimentScore,
          processedAt: new Date().toISOString(),
        },
      });
    } catch (err) {
      console.error("Failed to process record", { postId: post.id, err });
      // Let this record fail; bisectBatchOnError + retryAttempts in the
      // event source mapping will retry without blocking the whole batch.
      throw err;
    }
  }

  await publishSentimentMetrics(counts);
};

async function publishSentimentMetrics(counts: Record<string, number>) {
  const metricData = Object.entries(counts)
    .filter(([, value]) => value > 0)
    .map(([sentiment, value]) => ({
      MetricName: `${capitalize(sentiment)}SentimentCount`,
      Value: value,
      Unit: "Count" as const,
      Timestamp: new Date(),
    }));

  if (metricData.length === 0) return;

  await cloudwatch.send(
    new PutMetricDataCommand({
      Namespace: METRIC_NAMESPACE,
      MetricData: metricData,
    })
  );
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
}
