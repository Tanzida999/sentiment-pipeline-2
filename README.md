# Real-Time Social Media Sentiment Analysis Pipeline

A serverless, real-time pipeline that ingests simulated social media posts, scores their
sentiment with Amazon Comprehend, indexes the results in OpenSearch for a live dashboard,
and monitors itself (and the sentiment trend) with CloudWatch alarms.

Built with **AWS CDK (TypeScript)** — infrastructure and Lambda code live in the same
repo, generated and reviewed with **GitHub Copilot**.

## Architecture

```
 producer/generate-posts.ts          Lambda (sentiment-consumer)
 (simulated posts, local script)      ├─ Amazon Comprehend  → sentiment + score
        │  PutRecords                 ├─ OpenSearch          → indexed document
        ▼                             └─ CloudWatch          → custom metrics
 Amazon Kinesis Data Stream  ───────────────▶ (event source mapping)
 (social-posts-stream)
                                             │
                                             ▼
                                  Amazon OpenSearch Domain
                                  (sentiment-dashboard)
                                  → OpenSearch Dashboards for
                                    live visualization
                                             │
                                             ▼
                                  CloudWatch Dashboard + Alarms
                                  (lag, errors, negative-sentiment
                                   spikes) → SNS
```

**Why this shape:**
- **Kinesis** decouples ingestion from processing and models a real streaming source
  (the code works unmodified against a real Twitter/X or webhook feed later).
- **Lambda + Comprehend** keeps the scoring step serverless and pay-per-use.
- **OpenSearch** gives free-text search plus built-in dashboards, so no separate
  frontend is required for the demo.
- **CloudWatch** covers both *pipeline health* (iterator age, Lambda errors) and a
  *business-relevant anomaly signal* (a spike in negative sentiment), which is the kind
  of monitoring an AI/QA engineer is expected to design, not just infra metrics.

## Repo layout

```
bin/sentiment-pipeline.ts        CDK app entry point
lib/sentiment-pipeline-stack.ts  All AWS resources (Kinesis, Lambda, OpenSearch, CloudWatch, SNS)
lambda/consumer/index.ts         Kinesis consumer: Comprehend + OpenSearch + metrics
producer/generate-posts.ts       Local script simulating social posts into Kinesis
```

## Prerequisites

- Node.js 20+
- An AWS account with credentials configured (`aws configure`)
- AWS CDK CLI bootstrap done once per account/region: `npx cdk bootstrap`

## Setup

```bash
npm install
npm run build
npm run deploy
```

`cdk deploy` will print outputs including the Kinesis stream name, the OpenSearch
Dashboards URL, and the CloudWatch dashboard URL.

> **First-time OpenSearch access:** the domain uses IAM-based access control tied to
> the Lambda's execution role. To browse OpenSearch Dashboards yourself in a browser,
> either add your own IAM user/role to the domain access policy, or use SigV4-signed
> requests / an IAM-auth proxy. This is called out deliberately in interviews as a
> real-world access-control tradeoff (convenience vs. least privilege).

## Run the demo

In one terminal, start the simulated traffic:

```bash
npm run producer -- --rate=5
```

This pushes ~5 synthetic posts every 5 seconds (mix of positive/neutral/negative,
across a few fake "brand" topics) into the Kinesis stream.

Within a few seconds you should see:
- New documents appear in the `sentiment-posts` OpenSearch index
- `PositiveSentimentCount` / `NegativeSentimentCount` / `NeutralSentimentCount` custom
  metrics populate in CloudWatch
- The CloudWatch dashboard (`sentiment-pipeline`) update live

To rehearse the "anomaly alert" story for interviews, run the producer with a skewed
rate (edit `NEGATIVE_TEMPLATES` weighting in `producer/generate-posts.ts`, or just run
it longer) until `NegativeSentimentSpikeAlarm` trips.

## Tear down

Everything here is billable (OpenSearch domain especially). When you're done:

```bash
npm run destroy
```

## Cost notes

- OpenSearch: single `t3.small.search` node — inexpensive but not free; destroy when
  not actively demoing.
- Kinesis: 1 provisioned shard (~$0.015/hr + PUT payload unit costs) — negligible for
  demo traffic.
- Lambda + Comprehend: pay-per-invocation/per-request; trivial at demo volume.
- Budget alarm: consider adding an AWS Budgets alert while you're building this out.

## How this was built (Copilot / AI-assisted workflow)

This project was scaffolded with AI assistance (GitHub Copilot in VS Code) and then
reviewed and tested manually, mirroring the workflow described on the resume:
1. Drafted the CDK stack and Lambda handler with Copilot suggestions.
2. Reviewed IAM policies by hand for least privilege (Comprehend/CloudWatch scoped to
   the specific actions needed, not `*` where avoidable).
3. Tested the consumer Lambda against sample Kinesis event payloads before wiring up
   the real event source mapping.
4. Validated the OpenSearch index end-to-end by cross-checking Comprehend's sentiment
   output against the CloudWatch custom metrics for a known batch of posts.

## Possible extensions

- Swap the simulated producer for a real source (Twitter/X API, RSS, or a webhook).
- Add a small React/CloudWatch-embedded dashboard instead of relying solely on
  OpenSearch Dashboards.
- Add `BatchDetectSentiment` to reduce the number of Comprehend API calls per batch.
- Add unit tests for the Lambda handler (mock the Comprehend/OpenSearch clients) —
  good next step to add if you want testing coverage to show in the QA-focused role.
