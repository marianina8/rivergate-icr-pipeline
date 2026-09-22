# Ingest → Classify → Route (ICR) Pipeline: Reference Architecture

## Purpose

This system is a generic, reusable pattern for a problem almost every
mid-market company has, even if they've never described it this way:
something arrives, a person has to read it and decide what happens next,
and that deciding step is manual, inconsistent, and the first thing that
breaks under volume.

Each instance of this repo serves three purposes at once:

- **A public demo** — proof of hands-on Go + AWS + applied-LLM engineering.
- **Course material** — the worked example for *From Go CLI to AI Agent*,
 showing the actual arc from a plain CLI to an agent-operable one.
- **A consulting demo** — a single build that gets reframed per prospect
 rather than rebuilt from scratch each time.

Because it's reframed per prospect and shown publicly, **no real client or
personal data ever goes in this repo.** All demo data is synthetic.

## The Problem Pattern

```
 something arrives -> someone decides what it is and what to do with it -> it gets handled
 (ticket, (currently: manual, (currently:
 alert, inconsistent, inconsistent,
 document, doesn't scale) undocumented)
 handoff)
```

Same pipeline shape, different classification prompt and routing rules per
prospect — that's the whole point of building it generically instead of
vertically.

## High-Level Architecture

```mermaid
flowchart LR
 subgraph Ingestion
 A1[S3 drop-zone bucket] --> Q[SQS queue]
 A2[Webhook via API Gateway + Lambda] --> Q
 end

 Q --> W[Go Worker Service]
 W --> C[Bedrock Classification]
 C --> W
 W --> D[(DynamoDB item state and audit trail)]
 W --> R{Router}

 R -->|high confidence| AUTO[Automatic action Slack / ticket / webhook]
 R -->|low confidence| HR[Human Review Queue]
 HR --> DB[Dashboard approve / override]
 DB --> D

 CLI[Go CLI] --> W
 CLI --> D
 MCP[MCP Server] --> CLI
 AGENT[AI Agent] --> MCP
```

## Components

**1. Ingestion.** Two entry points feed one queue: an S3 drop-zone bucket
for anything file-based (S3 event notifications push new-object events onto
the queue), and an API Gateway + Lambda webhook receiver for anything
event-based. Both normalize into the same message shape on SQS.

**2. Worker (Go).** Consumes the queue; for each item, pulls the raw content,
normalizes it, calls the classification step, writes the result to the
state store, hands the classified item to the router.

**3. Classification (AWS Bedrock).** A single, bounded call — not a
chatbot. Structured prompt in, structured JSON out:
`{category, priority, summary, confidence}`. Low confidence is a first-class
outcome, not an error — it's what routes an item to a human instead of
guessing.

**4. State Store (DynamoDB).** Every item's full lifecycle is recorded:
received -> classified -> routed -> actioned, with timestamps and the
model's reasoning attached at each step. This audit trail is often the
actual thing a compliance-minded buyer is paying for.

**5. Router.** A small rules-driven layer, not hardcoded branching logic.
High confidence + known category -> automatic action. Low confidence, or a
category marked always-review -> human review queue.

**6. Dashboard (Go web service).** Shows the human review queue: item
detail, the model's classification and confidence, and an approve/override
control — the human-in-the-loop layer.

**7. CLI (Go).** The same core operations exposed as a standalone CLI:
`ingest`, `classify`, `route`, `status`. The direct descendant of the
existing *Modern Go CLI* book/course.

**8. MCP Server (agent-ready layer).** Wraps the CLI's commands as MCP
tools so an AI agent can operate the pipeline the same way a human would.
Read tools (`status`, `get-item`, `list-queue`) are always available;
write/routing tools (`route-item`, `reclassify`, `approve`) are scoped,
logged, and require an explicit permission flag. No implicit write access.

**9. Infrastructure as Code (AWS SAM).** S3, SQS, DynamoDB, API Gateway, the
Lambda worker, and Bedrock IAM permissions are all defined in a single AWS
SAM template (`infra/template.yaml`). SAM was picked over Terraform/CDK
because this pipeline is almost entirely event-driven Lambda — exactly the
shape SAM was built for, with native local testing (`sam local invoke`,
`sam local start-api`).

## Safety & Data Handling

- No real client or personal data in this repo, ever — synthetic fixtures
 only.
- Every automatic (non-reviewed) action is logged with the classification
 reasoning that triggered it.
- MCP write/routing tools are opt-in and scoped, never on by default.
- Low-confidence classification routes to a human by design.

## Repo Structure

```
/cmd
 /cli -> the CLI binary (ingest, classify, route, status)
 /worker -> the SQS consumer / worker service
 /dashboard -> the review-queue web service
/internal
 /classify -> Bedrock client + prompt templates (+ mock fallback)
 /store -> DynamoDB access layer (+ local/in-memory fallback)
 /router -> routing rules engine
 /mcp -> MCP server wrapping the CLI as agent tools
/infra
 template.yaml -> AWS SAM template
 samconfig.toml -> SAM CLI deployment config
/demo -> synthetic fixtures and sample data for demos
README.md
ARCHITECTURE.md -> this file
```

## Reframing Notes

When pitching this to a specific prospect, only three things change: the
classification prompt, the routing rules, and the demo fixtures. The
ingestion, worker, state, dashboard, CLI, and MCP layers stay identical.
That's the reusability this whole architecture is built around.

## This instance: Rivergate Technologies (support-ticket triage)

Rivergate Technologies is a **fictional** mid-size B2B project-management SaaS
company (~450 employees, no in-house automation team). Tickets arrive by
email, chat and a web form and land in one shared inbox that a small team
triages by hand.

| Input | Rivergate value |
|---|---|
| Items | Support tickets (email, chat, web form) |
| Categories | `bug`, `billing`, `feature_request`, `outage`, `how_to`, `other` |
| Priorities | `low`, `medium`, `high`, `critical` |
| Confidence floor | 0.6 — nothing below it is auto-routed except outage/critical paging |

| Rule (first match wins) | Condition | Action |
|---|---|---|
| `outage-or-critical` | category = outage **or** priority = critical, any confidence | Page on-call channel + open high-priority ENG ticket → `incident` queue (also human review if confidence < 0.6) |
| `billing` | billing, confidence ≥ 0.75 | → `billing` queue |
| `bug` | bug, confidence ≥ 0.75 | Open ENG-BACKLOG ticket → `bug` queue |
| `feature-request` | feature_request, confidence ≥ 0.6 (floor only) | → `product` queue |
| `how-to` | how_to, confidence ≥ 0.6 | KB article suggestion → `self-serve` queue |
| default | anything else, incl. `other` | → `human-review` queue |

All three inputs live in `config/rivergate.yaml` (prompt + taxonomy + rules)
and `demo/tickets/` (fixtures).
