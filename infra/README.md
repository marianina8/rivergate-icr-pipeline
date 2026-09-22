# infra/ — AWS SAM (phase 5, not deployed yet)

`template.yaml` describes the AWS version of the pipeline:

| Local stand-in (phases 1–4)            | AWS resource (phase 5)                                   |
|----------------------------------------|----------------------------------------------------------|
| `.icr/dropzone/incoming/*.json`        | `DropZoneBucket` (S3) → event notification → SQS         |
| `POST 127.0.0.1:8081/tickets`          | `WebhookApi` (HTTP API) + `WebhookFunction` (Lambda)     |
| `.icr/queue/` (`ingest.DirQueue`)      | `TicketQueue` (SQS) + `TicketDLQ` (3 receives)           |
| `cmd/worker` loop (`Service.RunOnce`)  | `WorkerFunction` (SQS-triggered Lambda, batch 10)        |
| `.icr/items/*.json` (`store.File`)     | `ItemsTable` (DynamoDB, on-demand, PITR on)              |
| `classify.Mock`                        | `classify.Bedrock` + IAM `bedrock:InvokeModel` on one model |
| `.icr/outbox/*.jsonl` (`router.Outbox`)| Slack / ticketing clients behind `router.ActionSink`     |

**Status:** authored only. It has been checked with `cfn-lint` (offline static
analysis — no AWS calls). It has **not** been run through `sam validate`,
`sam build`, `sam local` or `sam deploy`.

## Phase 5 checklist (once the AWS account is active)

Code to write first (each has a local seam already in place):

1. `cmd/lambda/worker/main.go` — `lambda.Start` handler for `events.SQSEvent`.
   For each record: if it is an S3 event notification, `GetObject` the key and
   call `ingest.Normalize(..., "dropzone")`; otherwise decode the body as an
   `ingest.Ticket`. Then `Service.Process`. Return `SQSEventResponse` with
   `BatchItemFailures` for records that errored (the template enables
   `ReportBatchItemFailures`).
2. `cmd/lambda/webhook/main.go` — API Gateway v2 handler: verify the shared
   secret header, then `Service.Ingest(body, "webhook")`; 202 + id, 400 on
   `ingest.ErrInvalid`.
3. `internal/store/dynamo.go` — `store.Store` on DynamoDB; add it to the
   conformance test in `store_test.go` (run against DynamoDB Local).
   Items with a long audit trail should stay well under the 400 KB item limit.
4. `internal/ingest/sqs.go` — `ingest.Queue` on SQS (only the webhook needs
   `Send`; Lambda's event source mapping does receive/delete).
5. Load `config/rivergate.yaml` inside the Lambda (embed it with `go:embed` or
   ship it in the artifact) and pick the classifier from `ICR_CLASSIFIER`.
6. The Makefile targets `build-WorkerFunction` / `build-WebhookFunction` are
   already defined; they build `./cmd/lambda/...` for `linux/arm64`.

Then, in order:

```sh
aws ssm put-parameter --name /rivergate-icr/webhook-shared-secret --type SecureString --value "$(openssl rand -hex 32)"
# Enable model access for the Bedrock model in the console, and confirm the
# inference profile ID in template.yaml / config/rivergate.yaml.
cd infra
sam validate --lint
sam build
sam local invoke WorkerFunction -e events/sqs-ticket.json   # write sample events first
sam local start-api                                          # POST demo tickets to the webhook
sam deploy --guided   # first time; afterwards: sam deploy
```

Things to double-check in phase 5:

- **Model ID / inference profile** (`BedrockInferenceProfileId`,
  `BedrockFoundationModelId`) are placeholders — use whatever is enabled in
  the account and region. The IAM statement is scoped to exactly those ARNs.
- **Region** is `us-west-2` in `samconfig.toml` and `config/rivergate.yaml`.
- **Webhook auth**: the HTTP API has no authorizer; the plan is a shared
  secret checked in the Lambda (SSM SecureString). Swap for an IAM or JWT
  authorizer if callers support it.
- **SecureString decrypt**: reading the secret with the default `aws/ssm` KMS
  key may also need `kms:Decrypt`; add it if `GetParameter` is denied.
- The bucket name is fixed (`<stack>-dropzone-<account>`) to avoid a circular
  dependency between the bucket notification and the queue policy.
- `ScalingConfig.MaximumConcurrency: 2` caps worker concurrency (and Bedrock
  spend) without reserving account concurrency, which fails on new accounts
  with a low Lambda concurrency quota.
- The review dashboard stays a local Go service in phase 5, pointed at the
  DynamoDB table with your own credentials.
