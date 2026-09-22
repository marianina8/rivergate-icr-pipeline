# rivergate-icr-pipeline

An **Ingest → Classify → Route** pipeline for support-ticket triage, built in Go
with an AWS SAM deployment target. It's an instance of the ICR reference
architecture ([ARCHITECTURE.md](ARCHITECTURE.md)), set up for one prospect
scenario.

> **Rivergate Technologies is fictional.** It's a made-up mid-size B2B
> project-management SaaS company (~450 employees, no in-house automation
> team). Every ticket, customer, company, email address and product detail in
> this repo is synthetic. None of it refers to a real company or person.

## The problem

Rivergate's support tickets arrive by **email, chat and a web form**. They all
land in one shared inbox, and a small team triages them by hand. This
pipeline:

1. normalizes all three channels into one ticket shape,
2. classifies each ticket (category, priority, a one-to-two sentence summary,
   and a confidence score),
3. routes it by configurable rules: page on-call for outages, send billing to
   billing, open engineering tickets for bugs, and so on,
4. sends anything the model isn't sure about to a **human review queue** with
   a dashboard for approve/override,
5. records every step in an audit trail, and exposes all of it to AI agents
   through **MCP**, with write access that is opt-in per tool.

## Status

| Phase | What | State |
|---|---|---|
| 1 | Local-first MVP: CLI + classify (mock + Bedrock behind an interface), local store, tests | ✅ done |
| 2 | Worker + queue against local stand-ins for S3 (drop-zone folder), API Gateway (webhook on :8081) and SQS (directory queue) | ✅ done |
| 3 | Router rules engine + human-review dashboard (approve/override) | ✅ done |
| 4 | MCP server: read tools always on, write tools opt-in and scoped | ✅ done |
| 5 | Real AWS via SAM: Lambda handlers, DynamoDB store, SQS queue, Bedrock classifier | ✅ deployed (us-west-2). See [infra/README.md](infra/README.md) |
| + | Hosted, password-protected demo UI: submit a ticket, watch it get classified and routed, review the queue | ✅ code done; served at `marian.online/demos/rivergate/` |

Locally, everything runs with **zero AWS calls**: the default classifier is a
deterministic keyword mock that stands in for Bedrock. Against AWS, the same
pipeline uses Bedrock (Claude Haiku 4.5), DynamoDB and SQS.

## Quick start

Requires Go 1.24+.

```sh
make test            # all unit tests; no network, no AWS
make demo            # reset .icr/, ingest demo/tickets, show routing + outbox + review queue
make dashboard       # http://127.0.0.1:8080 — submit tickets, see results, review queue
```

Run the pipeline with its local entry points:

```sh
make worker                                   # drop zone + webhook + queue consumer
cp demo/tickets/03-chat-double-charge.json .icr/dropzone/incoming/   # "S3" drop
./demo/webhook-example.sh demo/tickets/06-chat-export-csv.json       # "API Gateway" POST
./bin/icr status
```

## Deploy to AWS

```sh
aws sso login --profile demos-admin
make sam-validate && make sam-build
cd infra && sam deploy --guided     # first time; afterwards: make sam-deploy
```

Full runbook, including how to send tickets to the deployed stack and point the
CLI and dashboard at it: [infra/README.md](infra/README.md).

## CLI

```
icr ingest [-process] <file|dir|->...   normalize tickets and enqueue them
icr classify -file <ticket.json>        preview classification + routing (dry run, no writes)
icr classify <id>                       re-run classification on a stored item and re-route it
icr route [-dry-run] <id>               (re-)apply routing rules to a classified item
icr status [-queue q] [-review] [id]    summary, filtered list, or one item's audit trail
icr review approve|override <id> -reviewer NAME [-category C] [-priority P] [-note TEXT]
icr outbox                              stubbed Slack posts / tickets / KB suggestions, with reasons
icr mcp [-allow-write tools]            serve the pipeline as MCP tools on stdio
```

Global flags: `-config` (default `config/rivergate.yaml`), `-data` (default
`.icr`), `-classifier mock|bedrock`, `-json`. Each also has an environment
variable: `ICR_CONFIG`, `ICR_DATA_DIR`, `ICR_CLASSIFIER`.

Against the deployed stack, add `-store dynamo -table <ItemsTableName>
-queue-url <TicketQueueUrl> -profile demos-admin` (or set `ICR_STORE`,
`ICR_ITEMS_TABLE`, `ICR_QUEUE_URL`, `AWS_PROFILE`). The dashboard takes the same
`-store dynamo -table … -profile …` flags.

## MCP (agent layer)

`icr mcp` speaks MCP over stdio (JSON-RPC 2.0, protocol revisions 2024-11-05
through 2025-06-18).

| Tool | Kind | Available |
|---|---|---|
| `status`, `get_item`, `list_queue` | read | always |
| `route_item`, `reclassify`, `approve` | write | only when named in `-allow-write` (no `all` shortcut) |

- Write tools that aren't enabled are hidden from `tools/list`. Calling one
  anyway returns an error.
- Write calls are recorded in the audit trail as `mcp:<actor>`.
- `route_item` runs the same rules as everything else. An agent can't push a
  low-confidence ticket past the human-review gate.
- `approve` requires a note, and should only be enabled for an agent acting on
  a human reviewer's instructions.

See `demo/mcp-client-config.example.json` for a desktop-client config.

The MCP layer is a small implementation with no dependencies (one ~440-line file
in `internal/mcp`). The tool surface is designed to move to the official Go
SDK (`github.com/modelcontextprotocol/go-sdk`) without changes.

## What was customized for Rivergate

Only the three per-prospect inputs changed. Everything else is the generic ICR
pipeline.

1. **Classification prompt**: `config/rivergate.yaml` → `taxonomy` + `prompt`.
   Categories are `bug`, `billing`, `feature_request`, `outage`, `how_to`,
   `other`. Priorities are `low`, `medium`, `high`, `critical`. The output is
   `{category, priority, summary, confidence}` plus a one-line `rationale`
   kept for the audit trail.
2. **Routing rules**: `config/rivergate.yaml` → `routing`. They are evaluated
   top to bottom, first match wins:

   | Rule | When | Then |
   |---|---|---|
   | outage-or-critical | category = outage **or** priority = critical, *any* confidence | notify `#rivergate-oncall` + high-priority `ENG` ticket → `incident` (also flagged for review if confidence < 0.6) |
   | billing | billing ≥ 0.75 | → `billing` |
   | bug | bug ≥ 0.75 | `ENG-BACKLOG` ticket → `bug` |
   | feature-request | feature_request (0.6 floor only) | → `product` |
   | how-to | how_to ≥ 0.6 | KB suggestion → `self-serve` |
   | *(default)* | confidence < 0.6, `other`, or below a rule's threshold | → `human-review` |

3. **Demo fixtures**: `demo/tickets/`, 10 synthetic tickets:

   | Fixture | Channel | Mock result | Lands in |
   |---|---|---|---|
   | 01 all boards 503 | web form | outage / critical @ 0.85 | incident (paged + ENG ticket) |
   | 02 tasks disappeared, data loss | email | bug / critical @ 0.75 | incident (paged + ENG ticket) |
   | 03 double charge | chat | billing / medium @ 0.90 | billing |
   | 04 Timeline view crash | email | bug / medium @ 0.90 | bug (+ ENG-BACKLOG ticket) |
   | 05 recurring tasks request | web form | feature_request / low @ 0.90 | product |
   | 06 export to CSV | chat | how_to / medium @ 0.75 | self-serve (+ KB link) |
   | 07 guests vs. seats (billing + how-to) | email | billing / medium @ 0.55 | human review |
   | 08 partnership inquiry | web form | other / low @ 0.90 | human review |
   | 09 "it's acting up again, asap" | chat | other / high @ 0.35 | human review |
   | 10 notification emails late | email | bug / medium @ 0.60 | human review (below bug's 0.75) |

   `internal/classify` and `internal/pipeline` tests pin these outcomes, so a
   keyword or rule change that moves a demo ticket fails loudly.

## Safety rules (enforced in code and tests)

- Synthetic data only. Fixtures use invented names and `.example` domains.
- Low confidence always defers to a human. A classifier error, an invalid
  label or malformed model output becomes `other @ 0.0` and goes to
  `human-review`. It is never a guess.
- Every automatic action is logged with the classification and rule reasoning
  that triggered it, both in the item's audit trail and in the outbox record.
- Actions are idempotent per item. Re-routing, reclassifying or approving never
  pages twice.
- MCP write tools are opt-in, scoped per tool, and attributed.
- The model prompt treats ticket text as untrusted data. The dashboard binds to
  localhost and rejects cross-origin form posts.

## Layout

```
cmd/cli          icr binary: ingest, classify, route, status, review, outbox, mcp
cmd/worker       queue consumer + local drop zone + local webhook receiver
cmd/dashboard    local web UI (submit, results, review queue); UI code in internal/dashboard
cmd/lambda/dashboard  the same UI hosted on Lambda, behind a shared password
cmd/lambda/worker   SQS-triggered Lambda (phase 5)
cmd/lambda/webhook  API Gateway webhook Lambda (phase 5)
internal/classify  Classifier interface, prompt templates, Mock, Bedrock (Converse API)
internal/store     item state + audit trail: Memory, File (DynamoDB in phase 5)
internal/router    rules engine + action sink (stub outbox)
internal/mcp       MCP stdio server over the pipeline
internal/ingest    channel normalization + local SQS/S3/API Gateway stand-ins
internal/pipeline  the shared lifecycle: normalize → classify → store → route → act
internal/config    loads + validates config/rivergate.yaml
internal/awsapp    AWS wiring + Lambda handlers (DynamoDB, SQS, S3, SSM, Bedrock)
internal/dashboard web UI: submit page, results, review queue, password login
internal/httplambda  runs a net/http handler behind API Gateway HTTP APIs
config/          the per-prospect inputs (prompt, taxonomy, rules)
infra/           SAM template, samconfig, sample Lambda events, deploy runbook
demo/            synthetic tickets, demo scripts, MCP client config example
```

`internal/ingest`, `internal/pipeline`, `internal/config` and `internal/awsapp`
go beyond the reference layout. They hold the ingestion stand-ins, the lifecycle shared by
the CLI, worker, dashboard and MCP server, and config loading, so none of that
is duplicated across binaries.
