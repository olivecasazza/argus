# argus

HolmesGPT-based SRE triage agent for Kubernetes. Receives Prometheus
Alertmanager webhooks, investigates firing alerts via
[HolmesGPT](https://github.com/robusta-dev/holmesgpt) (pointed at an
OpenAI-compatible LLM gateway), and posts concise triage summaries to
Discord and/or Slack — optionally proposing fixes as GitHub PRs that a
human approves from chat.

## Why

HolmesGPT is the strongest open-source k8s-native SRE investigator (ReAct
loop over kubectl, logs, Prometheus, Grafana, Loki), but its notification
sinks are narrow: the bundled operator only ships `slack` (bot token) and
`pagerduty` destinations, and Robusta gates richer routing behind their
SaaS. There is **no Discord sink and no generic webhook destination** at
all. This chart fills that gap with a forwarder that wires Holmes's
`/api/chat` to Discord *and* Slack, adds severity routing, dedupe, a
changelog feed, and a human-in-the-loop approve/merge flow.

## Architecture

```
                    ┌──> Discord webhook (per severity)
Alertmanager        │
     │              ├──> Slack chat.postMessage (per severity)
     ▼              │
argus-forwarder ────┼──> #changelog  (activity feed + change ledger)
     │              │
     │              └──> GitHub PR ──> approve in chat ──> merge ──> Flux
     ▼
  holmes  /api/chat        (investigation: kubectl, logs, Prometheus, MCP)
     ▲
     │
holmes-operator ──> ScheduledHealthCheck CRs (proactive cron investigations)
```

- **holmes** (subchart `robusta/holmes`): owns the investigation. Read-only
  k8s RBAC (`view` ClusterRole), pointed at the LLM gateway via
  `OPENAI_API_BASE` + `modelList`.
- **forwarder** (this chart, `files/forwarder.py`): webhook receiver,
  fingerprint dedupe, severity filter, calls holmes, fans out to Discord +
  Slack, owns the HITL proposal store. **No cluster RBAC.**
- **holmes-operator** (subchart, opt-in via `holmes.operator.enabled`):
  reconciles `scheduledhealthchecks` / `healthchecks` /
  `triggeredhealthchecks` CRs (`holmesgpt.dev`) for *proactive* checks on a
  cron, in addition to reactive alert triage. Read-only; no remediation.

### Reactive vs proactive

| | Reactive triage | Proactive healthchecks |
|---|---|---|
| Trigger | Alertmanager webhook | cron in a `ScheduledHealthCheck` |
| Runs in | forwarder | holmes-operator |
| Output | Discord + Slack + changelog | CR `.status`, plus `slack`/`pagerduty` on failure in `alert` mode |
| Needs | `discordSecret` / `slackSecret` | `holmes.operator.enabled` + a CR |

A minimal check:

```yaml
apiVersion: holmesgpt.dev/v1alpha1
kind: ScheduledHealthCheck
metadata: { name: cluster-pod-health, namespace: apps }
spec:
  schedule: "0 */3 * * *"
  mode: alert                 # alert = notify on failure; monitor = status only
  query: |
    Are any pods stuck in CrashLoopBackOff, Error, or ImagePullBackOff?
  destinations:
    - type: slack             # slack + pagerduty are the ONLY operator sinks
      config: { channel: "C0123456789" }
```

The operator's slack destination uses a bot token (`SLACK_TOKEN` env, or
per-destination `config.token`) — **not** a webhook URL. It has no Discord
sink; Discord delivery goes through the forwarder.

## Install

```bash
helm dependency build ./charts/argus
helm install argus ./charts/argus -n monitoring -f my-values.yaml
```

Point Alertmanager at `http://argus-forwarder.monitoring.svc/webhook`.

### Secrets

Everything is optional and degrades gracefully — the forwarder skips any
sink whose credentials are absent.

```bash
# Discord (default sink)
kubectl create secret generic argus-forwarder -n monitoring \
  --from-literal=DISCORD_WEBHOOK=https://discord.com/api/webhooks/... \
  --from-literal=DISCORD_WEBHOOK_CHANGELOG=https://discord.com/api/webhooks/...

# Slack (dual-emit; channel IDs, not names)
kubectl create secret generic slack-credentials -n monitoring \
  --from-literal=SLACK_BOT_TOKEN=xoxb-... \
  --from-literal=SLACK_CHANNEL_CRITICAL=C... \
  --from-literal=SLACK_CHANNEL_WARNING=C... \
  --from-literal=SLACK_CHANNEL_INFO=C... \
  --from-literal=SLACK_CHANNEL_CHANGELOG=C...
```

Slack needs the `chat:write` scope (plus `chat:write.public` to post to
channels the bot hasn't joined).

## Feeds

- **Severity channels** — each alert's triage goes to the channel matching
  its `severity` label (`discordChannels` / `SLACK_CHANNEL_*`), falling back
  to the default webhook.
- **Changelog** — a running ledger separate from the alert channels: one
  line per triage (activity feed) plus every proposal lifecycle transition
  (proposed / approved / merged / rejected / revision-requested).

## Human-in-the-loop

When the GitHub MCP addon is enabled, Holmes can open a PR with a proposed
fix. The forwarder records it in a SQLite proposal store and posts it with
Approve / Reject / Revise buttons. Approving merges the PR via the GitHub
API (or records `approved-pending-merge` when `githubToken` is unset), and
GitOps takes it from there.

Degradation is per-credential:

| Missing | Behavior |
|---------|----------|
| `discordBotToken` | Webhook-only; buttons dormant, use the REST API below |
| `githubToken` | Proposal marked `approved-pending-merge`, PR left open |
| `mcpAddons.github.enabled=false` | Holmes never proposes; plain triage only |

REST fallback (mirrors the buttons):

```
GET  /proposals
POST /proposals/{id}/approve   {"actor": "...", "note": "..."}
POST /proposals/{id}/reject
```

Other endpoints: `POST /webhook` (Alertmanager), `GET /healthz`.

## Values

See [`values.yaml`](charts/argus/values.yaml). Key knobs:

| Value | Purpose |
|-------|---------|
| `forwarder.holmesUrl` | Holmes `/api/chat` base URL |
| `forwarder.holmesModel` | Model id passed to Holmes |
| `forwarder.investigateSeverities` | Comma list; others ignored |
| `forwarder.dedupeTtlSec` | Fingerprint dedupe window |
| `forwarder.discordSecret` | Secret holding `DISCORD_WEBHOOK` |
| `forwarder.discordChannels` | Per-severity webhook overrides |
| `forwarder.changelogWebhookSecret` | Secret key for the changelog webhook |
| `forwarder.slackSecret` | Secret holding `SLACK_BOT_TOKEN` + `SLACK_CHANNEL_*` |
| `forwarder.discordBotToken` | Enables interactive approve/reject buttons |
| `forwarder.githubToken` | Enables merge-on-approve |
| `forwarder.repoMappings` | Namespace → repo for proposed PRs |
| `forwarder.proposalsStorage` | PVC for the HITL proposal store |
| `holmes.operator.enabled` | Deploy holmes-operator (proactive healthchecks) |
| `holmes.toolsets` | Which investigation toolsets Holmes may use |
| `holmes.customSkillPaths` | Mounted `SKILL.md` dirs (see `files/skills/`) |
| `holmes.mcpAddons.github` | GitHub MCP addon (lets Holmes open PRs) |
| `holmes.additionalEnvVars` | e.g. `OPENAI_API_BASE` — LLM gateway URL |
| `holmes.modelList` | Models Holmes can use |

## Operational notes

- **The forwarder uses `strategy: Recreate`.** Its proposal store is an RWO
  PVC; the default RollingUpdate surges a second pod that cannot
  Multi-Attach the volume, deadlocking every rollout (new pod stuck
  `ContainerCreating`, old pod never terminating). Keep Recreate if you
  keep the PVC.
- **Triage quality is only as good as the gateway.** A 500 from
  `/api/chat` surfaces in chat as the upstream error text rather than an
  analysis; check the LLM gateway first when triage output looks wrong.

## Consumed by

[nixlab](https://github.com/olivecasazza/nixlab) via a Flux `HelmRelease`
pointing at this repo's `charts/argus` directory.
