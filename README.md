<p align="center">
  <a><img src="ui/static/img/crewai_logo.png" alt="CrewAI" height="44" style="background:#ffffff; padding:14px 18px; border-radius:12px;" /></a>
  &nbsp;
  <a><img src="ui/static/img/apache_kafka_logo.png" alt="Apache Kafka" height="44" style="background:#ffffff; padding:14px 18px; border-radius:12px;" /></a>
  &nbsp;
  <a><img src="ui/static/img/confluent_ogo.png" alt="Confluent" height="40" style="background:#ffffff; padding:16px 18px; border-radius:12px;" /></a>
</p>

# CrewAI × Kafka — A Choreographed Deep-Research System

Give this system a field (for example *finance*) and a process (for example
*procure-to-pay*), and it produces an executive-ready research report on that
topic. Three AI agents do the work: one researches the web, one validates the
findings, and one writes the report.

The interesting part is how they cooperate. The agents never call each other.
Each one runs in its own Docker container and communicates only by reading and
writing messages on Apache Kafka topics. There is no central orchestrator
deciding what runs when — the workflow emerges from what each agent does when a
message lands on the topic it listens to. The result is a small but complete
distributed system that runs on a laptop.

This repository is meant as a learning example: a clear, hackable demonstration
of how CrewAI agents and Apache Kafka combine into an event-driven pipeline. It
is intentionally simple. The [Extend it](#extend-it) section suggests ways to
turn it into something more capable.

## Table of contents
- [Background](#background)
- [Architecture](#architecture)
- [The agents](#the-agents)
- [Kafka topics and schemas](#kafka-topics-and-schemas)
- [How a request flows through the system](#how-a-request-flows-through-the-system)
- [Which LLMs it uses, and why](#which-llms-it-uses-and-why)
- [AWS Bedrock setup](#aws-bedrock-setup)
- [Running it](#running-it)
- [Using the web UI](#using-the-web-ui)
- [Stopping it](#stopping-it)
- [Observability](#observability)
- [Extend it](#extend-it)
- [Repository layout](#repository-layout)

## Background

If some of these terms are new, here is the short version.

**Agentic AI.** An *agent* is a large language model given a role, a goal, and a
set of tools. Instead of answering a single prompt, it works in a loop — reason,
take an action with a tool, observe the result, repeat — until the task is done.
The agents here use a web-search tool to gather evidence before they write.

**CrewAI.** An open-source Python framework for building agents and grouping them
into "crews." The common pattern is one crew of several agents running together
in a single process. This project uses CrewAI differently: each service is a crew
of just one agent, and the larger "team" is assembled across the network by Kafka.

**Apache Kafka.** A distributed, append-only log. Producers publish messages to
named *topics*; consumers read from those topics independently and at their own
pace. It is the durable backbone that lets the agents stay decoupled — they don't
need to be running at the same time or know about one another.

**Choreography vs. orchestration.** With orchestration, a central controller
tells each service what to do and when. With choreography, there is no
controller: each service follows one local rule — "when I see message X, do my
job and emit message Y" — and the overall behaviour emerges from those rules.
This system is pure choreography. Adding or removing an agent is just adding or
removing a consumer on a topic; nothing central has to change.

Running every agent as its own container (rather than as one in-process crew)
keeps their lifecycles, scaling, and failures independent, and it shows that the
agents really can operate as standalone services. Docker Compose keeps it all on
one machine; the same topology would move to Kubernetes and a managed broker
(such as Confluent Cloud) without code changes.

## Architecture

```
                ┌───────────────────────────────────────────────────────────┐
                │                   Apache Kafka (topics)                   │
                └───────────────────────────────────────────────────────────┘

  Browser (React, SSE)
        │  submit field + process
        ▼
  ┌───────────┐   crewai-ui-request-report    ┌──────────────────────┐
  │ Flask UI  │ ────────────────────────────▶ │ Scout                │
  │ (flask-ui)│                               │ (agent-market-       │── MCP ─▶ SearXNG ─▶ web
  └───────────┘ ◀───── SSE: logs + report     │  research)           │
        ▲  ▲                                  └──────────┬───────────┘
        │  │ crewai-agent-report-ready                   │ crewai-agent-market-research
        │  │                                             ▼
        │  │                                    ┌──────────────────────┐
        │  │   re-request (extra_context,       │ Auditor              │── MCP ─▶ SearXNG
        │  │    counter+1)                      │ (agent-validator)    │   (re-verify URLs)
        │  └──────── crewai-ui-request-report ◀─┤                      │
        │                                       └──────────┬───────────┘
        │                                                  │ crewai-agent-market-research-ready
        │                                                  ▼   (valid OR counter == 2)
        │                                       ┌────────────────────────┐
        │            crewai-agent-report-ready  │ Scribe                 │── MCP ─▶ SearXNG
        └───────────────────────────────────────┤ (agent-report-creator) │   (supplementary facts)
                                                └────────────────────────┘

  Every LLM prompt/response from every agent ──▶ crewai-logs ──▶ Flask UI (live feed)
```

The only control flow is "consume a topic, act, produce to a topic." No agent
imports or invokes another.

## The agents

Each agent is a single-agent CrewAI crew running in its own container. The names
(used in the code and the UI) describe what each one does.

| Container | Name | Role | Consumes | Produces |
|---|---|---|---|---|
| `agent-market-research` | Scout | Researches the field/process on the web — latest improvements, how leaders innovate, new entrants, where VCs invest — and records source URLs | `crewai-ui-request-report` | `crewai-agent-market-research` |
| `agent-validator` | Auditor | Checks the research for coverage, coherence, evidence and specificity, re-checking cited URLs. Approves it or sends it back for another pass | `crewai-agent-market-research` | `crewai-agent-market-research-ready`, or `crewai-ui-request-report` to re-request |
| `agent-report-creator` | Scribe | Writes the executive report in Markdown from validated research | `crewai-agent-market-research-ready` | `crewai-agent-report-ready` |

All three can use the web-search tool exposed by the MCP server: Scout to gather
material, Auditor to re-verify references, Scribe to fill an occasional gap while
writing.

The validation loop is bounded so the system always finishes. Each request
carries a `counter`. When the Auditor rejects research, it republishes the
request with its feedback in `extra_context` and `counter + 1`, sending Scout
back for another pass. The Auditor is tuned to approve workable research on the
first pass, so the loop is a safety valve rather than the normal path. Once the
research passes review, or `counter` reaches `MAX_RESEARCH_ITERATIONS` (2), it
moves on to the Scribe.

## Kafka topics and schemas

Every topic has one partition. The message key is the username (UTF-8). The value
is Avro, with schemas registered in the Confluent Schema Registry using
`TopicNameStrategy` (the subject for each topic is `<topic>-value`). The schema
files are in [`schemas/`](schemas/).

| Topic | Value schema | Produced by |
|---|---|---|
| `crewai-ui-request-report` | `ui_request_report.avsc` | the UI, and the Auditor when re-requesting |
| `crewai-agent-market-research` | `agent_market_research.avsc` | Scout |
| `crewai-agent-market-research-ready` | `agent_market_research_ready.avsc` | Auditor |
| `crewai-agent-report-ready` | `agent_report_ready.avsc` | Scribe |
| `crewai-logs` | `logs.avsc` | every agent (one message per LLM prompt and response) |

`start_demo.sh` creates the topics (one partition each) and registers the schemas
before any traffic flows, so they appear in Control Center immediately. Producers
also auto-register their schema on first publish.

## How a request flows through the system

1. A user logs in (username only) and submits a field and a process. The Flask
   backend publishes a request to `crewai-ui-request-report` with `counter = 0`
   and `extra_context = null`.
2. Scout consumes the request, researches the topic with the `web_search` tool
   (backed by SearXNG), and publishes its findings and source URLs to
   `crewai-agent-market-research`.
3. Auditor consumes the findings and reviews them, re-checking some of the cited
   URLs. If the research holds up — or the iteration cap is reached — it publishes
   to `crewai-agent-market-research-ready`. Otherwise it republishes the request
   with feedback and an incremented counter, and Scout researches again.
4. Scribe consumes the validated research and writes the report, publishing
   Markdown to `crewai-agent-report-ready`.
5. The Flask backend runs background consumers on `crewai-agent-report-ready` and
   `crewai-logs`. It matches each message to the right user by the Kafka key
   (the username) and pushes them to the browser over Server-Sent Events, so the
   user sees a live activity feed and then the finished report.

## Which LLMs it uses, and why

The agents run on Anthropic's Claude models hosted on Amazon Bedrock. CrewAI 1.x
talks to Bedrock through its native provider (boto3); no LiteLLM is involved. The
model is chosen per agent to match the work and balance cost against quality.

| Agent | Default model | Reasoning |
|---|---|---|
| Scout (research) | Claude Sonnet 4.6 | Research is a long loop of many search calls and synthesis. Sonnet gives a strong balance of speed and capability without paying Opus rates on every step. |
| Auditor (validation) | Claude Sonnet 4.6 | Validation is focused reasoning over a bounded input. Sonnet judges coverage and re-checks URLs reliably. Swap to Haiku if cost matters more than thoroughness. |
| Scribe (report) | Claude Opus 4.8 | The report is the graded deliverable, judged on clarity and structure. Opus writes the best long-form prose, and it runs only once per report. |

Each model is set by an environment variable, with defaults in
[`common/settings.py`](common/settings.py). Override them in `.env` or
`docker-compose.yml` to match what your account has enabled:

```
BEDROCK_MODEL_RESEARCH   (agent-market-research)
BEDROCK_MODEL_VALIDATOR  (agent-validator)
BEDROCK_MODEL_REPORT     (agent-report-creator)
```

## AWS Bedrock setup

The agents call Claude on Amazon Bedrock in the `eu-west-1` region.

1. **Provide credentials.** Copy the template and fill in your keys:
   ```bash
   cp .env_example .env
   ```
   Then set these in `.env`:
   ```
   export AWS_ACCESS_KEY_ID="..."
   export AWS_SECRET_ACCESS_KEY="..."
   export AWS_REGION_NAME="eu-west-1"
   ```
   `.env` is git-ignored; `.env_example` is the committed template and also holds
   the Confluent Platform image versions (`CP_*`).

2. **Enable model access.** In the AWS console, open Bedrock → Model access in
   `eu-west-1` and enable the Claude models you plan to use (Sonnet and Opus).
   Access is granted per region.

3. **Use EU inference-profile model IDs.** In `eu-west-1`, Claude is reached
   through EU cross-region inference profiles, so model IDs carry an `eu.` prefix,
   for example `bedrock/eu.anthropic.claude-sonnet-4-6`. Newer Claude models are
   often callable *only* through an inference profile; the bare
   `anthropic.claude-…` id returns an error.

4. **Grant IAM permissions.** The credentials need at least:
   ```
   bedrock:InvokeModel
   bedrock:InvokeModelWithResponseStream
   ```
   on the model and inference-profile ARNs.

Model availability differs by region and changes over time. Older models may be
marked legacy and rejected even when "enabled." If an agent logs an
`AccessDenied`, `ResourceNotFound`, or `ValidationException` from Bedrock, set the
`BEDROCK_MODEL_*` variables to a currently active model in your region. You can
list what is active with:

```bash
aws bedrock list-foundation-models --by-provider Anthropic --region eu-west-1 \
  --query "modelSummaries[?modelLifecycle.status=='ACTIVE'].modelId"
```

## Running it

Prerequisites: Docker Desktop with Compose v2, an AWS account with Bedrock Claude
access in `eu-west-1`, and roughly 8 GB of free RAM for the Confluent stack.

```bash
cp .env_example .env     # then add your AWS credentials (see above)
./start_demo.sh
```

`start_demo.sh` checks that Docker is running and `.env` exists, builds and starts
all containers (Confluent Platform, SearXNG, the MCP server, the three agents, and
the UI), waits for the Schema Registry and Control Center, creates the topics,
registers the schemas, and prints the service URLs. The first run builds images
and pulls the Confluent stack, so it takes a few minutes.

| Service | URL |
|---|---|
| Research console (the UI) | http://localhost:8088 |
| Confluent Control Center | http://localhost:9021 |
| Schema Registry | http://localhost:8081 |
| Prometheus | http://localhost:9090 |

## Using the web UI

1. Open http://localhost:8088 and log in with any username. There is no password;
   the username creates a session and becomes the Kafka message key.
2. Choose a field from the dropdown and describe the process in the text box (for
   example, *Technology* + *AI code review*, or *Finance* + *procure-to-pay*).
   Submit stays disabled until a field is selected and the process has at least a
   few characters.
3. Click Submit. Use Clear to reset the form.
4. Watch the Agent activity panel: every LLM prompt and response from Scout,
   Auditor, and Scribe streams in live from `crewai-logs`.
5. When the Scribe finishes, the report renders on the right. Use the Download
   button to save it as Markdown.

A full run takes a few minutes — it includes real web research, validation, a
possible re-research loop, and writing.

## Stopping it

```bash
./stop_demo.sh             # stop and remove containers, keep Kafka data
./stop_demo.sh --volumes   # also remove volumes for a clean slate
```

## Observability

- **In the UI:** the activity feed shows every LLM input and output, with the
  model name and token counts, read from `crewai-logs`.
- **In Control Center** (http://localhost:9021): topics, message flow, consumer
  groups, and the registered Avro schemas.
- **In the container logs:**
  `docker compose logs -f agent-market-research agent-validator agent-report-creator`.

The log feed is produced by a listener on CrewAI's event bus
([`common/logging_bus.py`](common/logging_bus.py)). It captures every LLM call —
prompt, response, model, and token usage — and publishes it to `crewai-logs`,
tagged with the agent name, `report_id`, and username.

## Extend it

This is a deliberately small example: enough to show the pattern, not a production
system. That is also what makes it a good starting point. Because there is no
orchestrator, adding a capability means adding one more Kafka consumer — nothing
central has to be rewired. If you want to go further, try building one of these.
Each is roughly a weekend-sized project.

- **Source curator.** A new agent on `crewai-agent-market-research` that dedupes
  and ranks references and scores their credibility before validation.
- **Competitor intelligence.** An agent that deep-dives the companies Scout names
  (funding, headcount, products) through a Crunchbase or CB Insights MCP tool.
- **Compliance / PII check.** An agent that screens the report against a policy
  before it reaches the user.
- **Publisher.** An agent that renders the Markdown to PDF or PPTX and delivers it
  (email, object storage, a Slack message).
- **Human in the loop.** A `report-review` topic and a UI control that lets a
  person approve a report before it is published.
- **Make it elastic.** Increase a topic's partitions and run several copies of an
  agent in the same consumer group to process requests in parallel.

If you build something interesting, the architecture is designed to welcome it:
write the consumer, give it a topic, add it to `docker-compose.yml`, and the rest
of the system carries on as before.

## Repository layout

```
.
├── common/                     Shared library (settings, Kafka+Avro, Bedrock LLM, MCP, logging)
├── schemas/                    Avro value schemas, one per topic
├── scripts/register_schemas.sh Registers the schemas with the Schema Registry
├── mcp_server/                 MCP server exposing a web_search tool over SearXNG
├── searxng/settings.yml        SearXNG configuration (JSON API enabled)
├── agents/
│   ├── market_research/        Scout   (request → research)
│   ├── validator/              Auditor (research → validated, or re-request)
│   └── report_creator/         Scribe  (validated → report)
├── ui/                         Flask + SSE backend, static React (CDN, no build step)
├── docker-compose.yml          Confluent Platform + SearXNG + MCP + agents + UI
├── start_demo.sh / stop_demo.sh
├── .env_example                Copy to .env and add your AWS credentials
└── samples/                    An example generated report
```
