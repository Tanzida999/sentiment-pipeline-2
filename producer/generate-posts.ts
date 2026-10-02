/**
 * Simulated social-media post generator.
 *
 * Run locally to push a steady stream of synthetic posts into the
 * Kinesis stream, so the pipeline has something to process end-to-end
 * without needing a real Twitter/X API key.
 *
 *   npm run producer -- --topic="YourBrand" --rate=5
 */
import { KinesisClient, PutRecordsCommand, PutRecordsCommandInput } from "@aws-sdk/client-kinesis";
import { randomUUID } from "crypto";

const REGION = process.env.AWS_REGION || "ap-northeast-1";
const STREAM_NAME = process.env.STREAM_NAME || "social-posts-stream";

const kinesis = new KinesisClient({ region: REGION });

const TOPICS = ["YourBrand", "CompetitorX", "NewFeatureLaunch", "CustomerSupport"];

const POSITIVE_TEMPLATES = [
  "Just tried {topic} and I'm honestly impressed, works exactly as promised.",
  "{topic} customer support fixed my issue in minutes. Great experience!",
  "Been using {topic} for a week now, really happy with it so far.",
  "Shoutout to {topic} for the smooth onboarding, loved it.",
];

const NEGATIVE_TEMPLATES = [
  "{topic} has been down for me all morning, so frustrating.",
  "Not happy with {topic} at all, the app keeps crashing.",
  "Waited 3 days for a reply from {topic} support, unacceptable.",
  "Regret switching to {topic}, way worse than what I had before.",
];

const NEUTRAL_TEMPLATES = [
  "Anyone else using {topic}? Curious what people think.",
  "Saw an ad for {topic} today, might give it a try.",
  "{topic} released an update, here are the release notes.",
  "Comparing {topic} with a couple of alternatives this week.",
];

function randomFrom<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function buildPost() {
  const topic = randomFrom(TOPICS);
  const bucket = Math.random();
  const template =
    bucket < 0.4
      ? randomFrom(POSITIVE_TEMPLATES)
      : bucket < 0.7
      ? randomFrom(NEUTRAL_TEMPLATES)
      : randomFrom(NEGATIVE_TEMPLATES);

  return {
    id: randomUUID(),
    author: `user_${Math.floor(Math.random() * 9999)}`,
    topic,
    text: template.replace("{topic}", topic),
    timestamp: new Date().toISOString(),
  };
}

async function sendBatch(batchSize: number) {
  const records: PutRecordsCommandInput["Records"] = Array.from({ length: batchSize }, () => {
    const post = buildPost();
    return {
      Data: Buffer.from(JSON.stringify(post)),
      PartitionKey: post.topic,
    };
  });

  const result = await kinesis.send(
    new PutRecordsCommand({ StreamName: STREAM_NAME, Records: records })
  );

  console.log(
    `Sent ${records.length} posts, ${result.FailedRecordCount ?? 0} failed.`
  );
}

async function main() {
  const rateArg = process.argv.find((a) => a.startsWith("--rate="));
  const rate = rateArg ? parseInt(rateArg.split("=")[1], 10) : 5; // posts per batch
  const intervalMs = 5000; // one batch every 5 seconds

  console.log(
    `Streaming simulated posts to "${STREAM_NAME}" in ${REGION} — ${rate} posts every ${
      intervalMs / 1000
    }s. Ctrl+C to stop.`
  );

  // Fire once immediately, then on an interval.
  await sendBatch(rate);
  setInterval(() => sendBatch(rate).catch((err) => console.error(err)), intervalMs);
}

main().catch((err) => {
  console.error("Producer crashed:", err);
  process.exit(1);
});
