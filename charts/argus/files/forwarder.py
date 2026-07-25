"""Argus forwarder — Alertmanager -> HolmesGPT -> Discord, with HITL.

Phase 2: Routes triage to Discord channels by severity.
Phase 3: Resolves alert namespace to GitHub repo; Holmes opens fix PRs.
Phase 5: Alert-tuning assessment in every triage.
Phase 4: Human-in-the-loop approve/reject via Discord buttons.

Phase 4 architecture:
  - discord.py Client (gateway) receives button interactions natively.
  - aiohttp web server (same loop) receives Alertmanager webhooks.
  - SQLite proposals store with audit trail (lifts the pnf-ops pattern).
  - On triage containing a PR URL: insert proposal, post buttons.
  - Approve → merge PR via GitHub API (if GITHUB_TOKEN set) or mark
    approved-pending-merge.
  - Reject → record reason, optionally close the PR.

Graceful degradation: if DISCORD_BOT_TOKEN is unset, runs webhook-only
(no buttons, no interactions) — backwards compatible with phase 2/3/5.

Safety: forwarder has NO cluster RBAC. Holmes owns read-only investigation.
This service speaks HTTP to Holmes, Discord (gateway + REST), and GitHub.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import signal
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

from aiohttp import ClientSession, ClientTimeout, web

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("argus-forwarder")

HOLMES_URL = os.environ.get("HOLMES_URL", "http://argus-holmes:80")
HOLMES_MODEL = os.environ.get("HOLMES_MODEL", "auto")
HOLMES_TIMEOUT = int(os.environ.get("HOLMES_TIMEOUT_SEC", "300"))
INVESTIGATE_SEVERITIES = {
    s.strip().lower()
    for s in os.environ.get("INVESTIGATE_SEVERITIES", "critical,warning").split(",")
    if s.strip()
}
DEDUPE_TTL = int(os.environ.get("DEDUPE_TTL_SEC", "3600"))
DISCORD_MAX = 1900

# --- Phase 2: multi-channel routing --------------------------------------- #
_DEFAULT_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_DEFAULT", "")
CHANNEL_WEBHOOKS: dict[str, str] = {
    "critical": os.environ.get("DISCORD_WEBHOOK_CRITICAL", _DEFAULT_WEBHOOK),
    "warning": os.environ.get("DISCORD_WEBHOOK_WARNING", _DEFAULT_WEBHOOK),
    "info": os.environ.get("DISCORD_WEBHOOK_INFO", ""),
    "deals": os.environ.get("DISCORD_WEBHOOK_DEALS", ""),
}

# --- Changelog channel ---------------------------------------------------- #
# A running ledger of argus activity, separate from the severity-routed alert
# channels: every triage (activity feed) + every proposal lifecycle transition
# (change ledger). No-op when unset (empty webhook -> _post_discord returns).
CHANGELOG_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_CHANGELOG", "")


async def _post_changelog(session: ClientSession, line: str) -> None:
    await _post_discord(session, CHANGELOG_WEBHOOK, line)

# --- Phase 3: project-agnostic repo mapping ------------------------------- #
REPO_MAPPINGS: dict[str, str] = json.loads(os.environ.get("REPO_MAPPINGS", "{}"))

# --- Phase 4: HITL bot ---------------------------------------------------- #
DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")
DISCORD_CONTROL_CHANNEL_ID = os.environ.get("DISCORD_CONTROL_CHANNEL_ID", "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
PROPOSALS_DB = os.environ.get("PROPOSALS_DB", "/data/proposals.db")

_PR_URL_RE = re.compile(
    r"https?://github\.com/([\w.-]+/[\w.-]+)/pull/(\d+)", re.IGNORECASE
)

_recent: dict[str, float] = {}


def _resolve_webhook(severity: str) -> str:
    return CHANNEL_WEBHOOKS.get(severity, _DEFAULT_WEBHOOK)


def _resolve_repo(namespace: str) -> str | None:
    if not REPO_MAPPINGS:
        return None
    return REPO_MAPPINGS.get(namespace) or REPO_MAPPINGS.get("default")


# --------------------------------------------------------------------------- #
#  Dedupe
# --------------------------------------------------------------------------- #


def _is_duplicate(fingerprint: str) -> bool:
    now = time.time()
    for k in [k for k, v in _recent.items() if now - v > DEDUPE_TTL]:
        _recent.pop(k, None)
    if fingerprint in _recent:
        return True
    _recent[fingerprint] = now
    return False


def _fingerprint(alert: dict[str, Any]) -> str:
    if fp := alert.get("fingerprint"):
        return fp
    labels = alert.get("labels", {})
    return hashlib.sha256(json.dumps(labels, sort_keys=True).encode()).hexdigest()[:16]


# --------------------------------------------------------------------------- #
#  Phase 4: SQLite proposals store (lifts pnf-ops pattern, made generic)
# --------------------------------------------------------------------------- #


def _db() -> sqlite3.Connection:
    Path(PROPOSALS_DB).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(PROPOSALS_DB, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS proposals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fingerprint TEXT NOT NULL,
            alertname TEXT NOT NULL,
            severity TEXT,
            namespace TEXT,
            repo TEXT,
            pr_url TEXT,
            pr_number INTEGER,
            triage TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            actor TEXT,
            note TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_proposals_status ON proposals(status);
        CREATE INDEX IF NOT EXISTS idx_proposals_fingerprint ON proposals(fingerprint);

        CREATE TABLE IF NOT EXISTS approvals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proposal_id INTEGER NOT NULL,
            state TEXT NOT NULL,
            actor TEXT NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (proposal_id) REFERENCES proposals(id)
        );
        """
    )
    conn.commit()
    return conn


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _create_proposal(
    *,
    fingerprint: str,
    alertname: str,
    severity: str,
    namespace: str,
    repo: str,
    pr_url: str,
    pr_number: Optional[int],
    triage: str,
) -> int:
    with closing(_db()) as conn:
        cur = conn.execute(
            """INSERT INTO proposals
               (fingerprint, alertname, severity, namespace, repo, pr_url,
                pr_number, triage, status, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,'pending',?,?)""",
            (
                fingerprint,
                alertname,
                severity,
                namespace,
                repo,
                pr_url,
                pr_number,
                triage,
                _now(),
                _now(),
            ),
        )
        conn.commit()
        proposal_id = cur.lastrowid
        assert proposal_id is not None, "INSERT succeeded but lastrowid is None"
        return proposal_id


def _get_proposal(proposal_id: int) -> Optional[sqlite3.Row]:
    with closing(_db()) as conn:
        return conn.execute(
            "SELECT * FROM proposals WHERE id=?", (proposal_id,)
        ).fetchone()


def _set_proposal_status(
    proposal_id: int, status: str, actor: str, note: str
) -> Optional[sqlite3.Row]:
    with closing(_db()) as conn:
        row = conn.execute(
            "SELECT * FROM proposals WHERE id=?", (proposal_id,)
        ).fetchone()
        if not row:
            return None
        conn.execute(
            "INSERT INTO approvals(proposal_id,state,actor,note,created_at) VALUES (?,?,?,?,?)",
            (proposal_id, status, actor, note, _now()),
        )
        conn.execute(
            "UPDATE proposals SET status=?, actor=?, note=?, updated_at=? WHERE id=?",
            (status, actor, note, _now(), proposal_id),
        )
        conn.commit()
        return conn.execute(
            "SELECT * FROM proposals WHERE id=?", (proposal_id,)
        ).fetchone()


def _extract_pr(triage: str) -> Optional[tuple[str, str, int]]:
    """Pull (repo, pr_url, pr_number) out of Holmes's triage response."""
    m = _PR_URL_RE.search(triage)
    if not m:
        return None
    repo, num = m.group(1), int(m.group(2))
    return repo, m.group(0), num


# --------------------------------------------------------------------------- #
#  Holmes interaction (prompt from phase 2/3/5)
# --------------------------------------------------------------------------- #


def _build_prompt(alert: dict[str, Any]) -> str:
    labels = alert.get("labels", {})
    annotations = alert.get("annotations", {})
    namespace = labels.get("namespace", "")
    repo = _resolve_repo(namespace)

    prompt = (
        "A Prometheus alert is firing in this Kubernetes cluster. "
        "Investigate the root cause using your tools (kubectl, logs, "
        "Prometheus metrics) and produce a terse triage.\n\n"
        f"Alertname: {labels.get('alertname', 'unknown')}\n"
        f"Severity:  {labels.get('severity', 'unknown')}\n"
        f"Namespace: {namespace or 'n/a'}\n"
        f"Started:   {alert.get('startsAt', 'n/a')}\n\n"
        f"Labels:\n{json.dumps(labels, indent=2)}\n\n"
        f"Annotations:\n{json.dumps(annotations, indent=2)}\n\n"
        "Output format (keep the triage under ~600 chars):\n"
        "- **Verdict**: real / cascade / transient -- one sentence.\n"
        "- **Root cause**: one or two sentences with concrete evidence "
        "(log lines, metric values).\n"
        "- **Action**: specific -- a command to run or the next diagnostic step.\n"
        "- **Confidence**: low / medium / high.\n"
    )

    if repo:
        prompt += (
            f"\n**Proposed fix**: If you can identify a specific code or config "
            f"fix, open a pull request against `{repo}` using your GitHub tools. "
            f"Target the repo's default branch. Keep the change minimal and "
            f"focused. Add a clear PR description linking back to this alert. "
            f"Include the PR URL in your triage under **Proposed fix**. "
            f"If the fix is unclear or risky, skip this — do not open "
            f"speculative PRs.\n"
        )

    prompt += (
        "\n**Alert tuning**: Also assess whether this alert is well-tuned. "
        "Query Prometheus for its firing history over the last 7 days "
        '(use metric `ALERTS{alertname="<name>"}`). If it fires frequently '
        "with transient or cascade verdicts, propose a severity downgrade, "
        "threshold adjustment, or `for:` duration increase. If it rarely "
        "fires but represents real impact, consider a severity upgrade. "
        "State your tuning recommendation under **Tuning** (or 'no change')."
    )

    return prompt


def _extract_analysis(data: Any) -> str:
    if isinstance(data, str):
        return data
    if isinstance(data, dict):
        for key in ("response", "analysis", "answer", "content", "text", "output"):
            if val := data.get(key):
                return val if isinstance(val, str) else json.dumps(val, indent=2)
    return json.dumps(data, indent=2)


async def _investigate(session: ClientSession, alert: dict[str, Any]) -> str:
    try:
        resp = await session.post(
            f"{HOLMES_URL}/api/chat",
            json={"ask": _build_prompt(alert), "model": HOLMES_MODEL},
            timeout=ClientTimeout(total=HOLMES_TIMEOUT),
        )
        if resp.status >= 400:
            body = await resp.text()
            return f"_Holmes returned HTTP {resp.status}: {body[:300]}_"
        return _extract_analysis(await resp.json())
    except asyncio.TimeoutError:
        return f"_Holmes timed out after {HOLMES_TIMEOUT}s_"
    except Exception as exc:  # noqa: BLE001
        return f"_Holmes unreachable: {exc}_"


# --------------------------------------------------------------------------- #
#  Discord (webhook posts + bot interactions)
# --------------------------------------------------------------------------- #


async def _post_discord(session: ClientSession, webhook: str, content: str) -> None:
    if not webhook:
        return
    pos = 0
    while pos < len(content):
        chunk = content[pos : pos + DISCORD_MAX]
        pos += DISCORD_MAX
        for attempt in range(3):
            try:
                resp = await session.post(
                    webhook,
                    json={"content": chunk, "username": "argus"},
                    timeout=ClientTimeout(total=30),
                )
                if resp.status == 429:
                    retry = float(resp.headers.get("Retry-After", "5"))
                    await asyncio.sleep(retry)
                    continue
                if resp.status >= 400:
                    log.warning("discord %s: %s", resp.status, await resp.text())
                break
            except Exception as exc:  # noqa: BLE001
                log.warning("discord post failed (attempt %d): %s", attempt + 1, exc)
                await asyncio.sleep(2)


async def _merge_pr(
    session: ClientSession, repo: str, pr_number: int
) -> tuple[bool, str]:
    """Merge a GitHub PR. Returns (success, message)."""
    if not GITHUB_TOKEN:
        return False, "GITHUB_TOKEN not set — proposal marked approved-pending-merge"
    try:
        resp = await session.put(
            f"https://api.github.com/repos/{repo}/pulls/{pr_number}/merge",
            headers={
                "Authorization": f"token {GITHUB_TOKEN}",
                "Accept": "application/vnd.github+json",
            },
            json={"merge_method": "squash"},
            timeout=ClientTimeout(total=30),
        )
        if resp.status in (200, 202):
            return True, f"merged {repo}#{pr_number}"
        body = await resp.text()
        return False, f"GitHub {resp.status}: {body[:200]}"
    except Exception as exc:  # noqa: BLE001
        return False, f"merge request failed: {exc}"


async def _close_pr(
    session: ClientSession, repo: str, pr_number: int
) -> tuple[bool, str]:
    if not GITHUB_TOKEN:
        return False, "GITHUB_TOKEN not set — PR left open"
    try:
        resp = await session.patch(
            f"https://api.github.com/repos/{repo}/pulls/{pr_number}",
            headers={
                "Authorization": f"token {GITHUB_TOKEN}",
                "Accept": "application/vnd.github+json",
            },
            json={"state": "closed"},
            timeout=ClientTimeout(total=30),
        )
        if resp.status in (200, 202):
            return True, f"closed {repo}#{pr_number}"
        body = await resp.text()
        return False, f"GitHub {resp.status}: {body[:200]}"
    except Exception as exc:  # noqa: BLE001
        return False, f"close request failed: {exc}"


# --------------------------------------------------------------------------- #
#  Phase 4: discord.py bot (gateway client for button interactions)
# --------------------------------------------------------------------------- #


async def _run_bot(http_session: ClientSession) -> None:
    """Start the discord.py gateway client. No-op if no token."""
    if not DISCORD_BOT_TOKEN:
        log.info("DISCORD_BOT_TOKEN unset — HITL bot dormant (webhook-only mode)")
        return

    try:
        import discord
    except ImportError:
        log.error(
            "discord.py not installed but DISCORD_BOT_TOKEN set — "
            "ensure the venv installs it"
        )
        return

    intents = discord.Intents.default()
    # message_content requires the privileged intent (Bot tab in dev portal)
    # but button interactions don't need it.
    intents.message_content = False
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready() -> None:
        log.info(
            "argus HITL bot connected as %s (id=%s)",
            client.user,
            client.user.id if client.user else "?",
        )

    @client.event
    async def on_interaction(interaction: discord.Interaction) -> None:
        # Button clicks arrive here. custom_id format: argus:<action>:<id>
        try:
            await _handle_button(interaction, http_session)
        except Exception as exc:  # noqa: BLE001
            log.exception("button handler error: %s", exc)
            try:
                await interaction.response.send_message(
                    f":x: Internal error: {exc}", ephemeral=True
                )
            except Exception:
                pass

    # discord.py 2.x uses an explicit setup_hook for slash commands; we
    # only use raw on_interaction for buttons, so no hook needed.
    token_task = asyncio.create_task(client.start(DISCORD_BOT_TOKEN))
    log.info("starting discord gateway client")
    try:
        await token_task
    except asyncio.CancelledError:
        await client.close()
        raise


async def _handle_button(interaction: "Any", http_session: ClientSession) -> None:
    """Handle a Discord button interaction."""
    custom_id = (
        getattr(interaction, "data", {}).get("custom_id", "")
        if hasattr(interaction, "data")
        else ""
    )
    # discord.py normalizes custom_id onto interaction.data; be defensive
    if not custom_id and hasattr(interaction, "message"):
        return

    parts = custom_id.split(":")
    if len(parts) != 3 or parts[0] != "argus":
        return

    _, action, proposal_id_str = parts
    try:
        proposal_id = int(proposal_id_str)
    except ValueError:
        return

    row = _get_proposal(proposal_id)
    if not row:
        await interaction.response.send_message(
            f":x: proposal {proposal_id} not found", ephemeral=True
        )
        return

    if row["status"] != "pending":
        await interaction.response.send_message(
            f":information_source: proposal {proposal_id} already "
            f"{row['status']} by {row['actor']}",
            ephemeral=True,
        )
        return

    actor = (
        getattr(getattr(interaction, "user", None), "name", None)
        or getattr(getattr(interaction, "member", None), "nick", None)
        or "discord-user"
    )

    if action == "approve":
        merged, msg = await _merge_pr(http_session, row["repo"], row["pr_number"])
        status = "merged" if merged else "approved-pending-merge"
        _set_proposal_status(proposal_id, status, actor, msg)
        await interaction.response.send_message(
            f":white_check_mark: **Approved** by {actor} — {msg}\n"
            f"`{row['alertname']}` → {row['pr_url']}"
        )
        await _post_changelog(
            http_session,
            f":white_check_mark: **{status}** #{proposal_id} "
            f"`{row['alertname']}` by {actor} → {row['pr_url']}",
        )
    elif action == "reject":
        _, msg = await _close_pr(http_session, row["repo"], row["pr_number"])
        _set_proposal_status(proposal_id, "rejected", actor, msg)
        await interaction.response.send_message(
            f":no_entry: **Rejected** by {actor} — {msg}\n"
            f"`{row['alertname']}` → {row['pr_url']}"
        )
        await _post_changelog(
            http_session,
            f":no_entry: **rejected** #{proposal_id} "
            f"`{row['alertname']}` by {actor}",
        )
    elif action == "revise":
        _set_proposal_status(
            proposal_id, "needs-revision", actor, "human requested revision"
        )
        await _post_changelog(
            http_session,
            f":pencil: **revision requested** #{proposal_id} "
            f"`{row['alertname']}` by {actor}",
        )
        await interaction.response.send_message(
            f":pencil: **Revision requested** by {actor} for proposal "
            f"{proposal_id} (`{row['alertname']}`). Holmes will be asked "
            f"to revise on the next firing."
        )
    else:
        await interaction.response.send_message(
            f":x: unknown action '{action}'", ephemeral=True
        )


def _button_view(proposal_id: int) -> dict[str, Any]:
    """Discord message components for a proposal. custom_id = argus:<action>:<id>.

    Returns the components payload for the webhook POST. (discord.py isn't
    needed to *render* buttons — only to *receive* their interactions.)
    """
    return {
        "components": [
            {
                "type": 1,  # ActionRow
                "components": [
                    {
                        "type": 2,  # Button
                        "style": 3,  # Success (green)
                        "label": "Approve",
                        "custom_id": f"argus:approve:{proposal_id}",
                    },
                    {
                        "type": 2,
                        "style": 4,  # Danger (red)
                        "label": "Reject",
                        "custom_id": f"argus:reject:{proposal_id}",
                    },
                    {
                        "type": 2,
                        "style": 1,  # Primary (blue)
                        "label": "Revise",
                        "custom_id": f"argus:revise:{proposal_id}",
                    },
                ],
            }
        ]
    }


# --------------------------------------------------------------------------- #
#  Triage flow (creates proposals when Holmes opens a PR)
# --------------------------------------------------------------------------- #


async def _triage(session: ClientSession, alert: dict[str, Any]) -> None:
    labels = alert.get("labels", {})
    alertname = labels.get("alertname", "unknown")
    severity = labels.get("severity", "unknown")
    namespace = labels.get("namespace", "")
    fp = _fingerprint(alert)
    webhook = _resolve_webhook(severity)

    if _is_duplicate(fp):
        log.info("dedupe %s %s", alertname, fp)
        return

    log.info("triage start: %s severity=%s fp=%s", alertname, severity, fp)
    await _post_discord(
        session,
        webhook,
        f":mag: **Investigating** `{alertname}` ({severity}) `{fp[:12]}`",
    )

    analysis = await _investigate(session, alert)

    # Phase 4: if Holmes opened a PR, create a proposal row and post the
    # triage with Approve/Reject/Revise buttons. Otherwise plain triage.
    pr = _extract_pr(analysis)
    if pr:
        repo, pr_url, pr_number = pr
        proposal_id = _create_proposal(
            fingerprint=fp,
            alertname=alertname,
            severity=severity,
            namespace=namespace,
            repo=repo,
            pr_url=pr_url,
            pr_number=pr_number,
            triage=analysis,
        )
        await _post_discord_with_components(
            session,
            webhook,
            f":white_check_mark: **Triage + proposed fix** `{alertname}` "
            f"(proposal #{proposal_id})\n{analysis}",
            _button_view(proposal_id),
        )
        log.info(
            "triage done: %s proposal #%d pr=%s#%d",
            alertname,
            proposal_id,
            repo,
            pr_number,
        )
        await _post_changelog(
            session,
            f":clipboard: triaged `{alertname}` ({severity}) → "
            f"proposed fix #{proposal_id} {pr_url}",
        )
    else:
        await _post_discord(
            session,
            webhook,
            f":white_check_mark: **Triage** `{alertname}`\n{analysis}",
        )
        log.info("triage done: %s (no PR proposed)", alertname)
        await _post_changelog(
            session,
            f":memo: triaged `{alertname}` ({severity}) → no change proposed",
        )


async def _post_discord_with_components(
    session: ClientSession,
    webhook: str,
    content: str,
    components: dict[str, Any],
) -> None:
    """Post a Discord webhook message with button components.

    Discord caps content at 2000 chars; if we'd overflow, post the long
    triage first (plain), then a short follow-up with the buttons.
    """
    if not webhook:
        return
    if len(content) <= DISCORD_MAX:
        payload = {"content": content, "username": "argus", **components}
        for attempt in range(3):
            try:
                resp = await session.post(
                    webhook, json=payload, timeout=ClientTimeout(total=30)
                )
                if resp.status == 429:
                    await asyncio.sleep(float(resp.headers.get("Retry-After", "5")))
                    continue
                if resp.status >= 400:
                    log.warning("discord %s: %s", resp.status, await resp.text())
                return
            except Exception as exc:  # noqa: BLE001
                log.warning("discord post failed (attempt %d): %s", attempt + 1, exc)
                await asyncio.sleep(2)
        return

    # Split: long triage first (plain chunks), then short prompt with buttons
    await _post_discord(session, webhook, content)
    await _post_discord_with_components(
        session,
        webhook,
        ":point_down: **Review the proposed fix above**",
        components,
    )


# --------------------------------------------------------------------------- #
#  HTTP server (Alertmanager webhook + admin endpoints)
# --------------------------------------------------------------------------- #


async def _handle_webhook(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        return web.Response(status=400, text="bad json")

    session = request.app["session"]
    for alert in payload.get("alerts", []):
        if alert.get("status") != "firing":
            continue
        severity = alert.get("labels", {}).get("severity", "").lower()
        if severity not in INVESTIGATE_SEVERITIES:
            continue
        asyncio.create_task(_triage(session, alert))

    return web.Response(text="queued")


async def _handle_proposals(request: web.Request) -> web.Response:
    """Admin: list pending proposals. GET /proposals[?status=pending]."""
    status = request.query.get("status", "pending")
    with closing(_db()) as conn:
        rows = conn.execute(
            "SELECT id, alertname, severity, repo, pr_url, pr_number, "
            "status, actor, created_at FROM proposals WHERE status=? "
            "ORDER BY created_at DESC LIMIT 50",
            (status,),
        ).fetchall()
    return web.json_response([dict(r) for r in rows])


async def _handle_proposal_action(request: web.Request) -> web.Response:
    """REST fallback (mirrors pnf-ops): POST /proposals/{id}/{approve|reject}.

    Body: {"note": "...", "actor": "..."} (both optional).
    Use this when Discord buttons aren't available (no bot token).
    """
    parts = request.path.strip("/").split("/")
    if len(parts) != 3 or parts[0] != "proposals":
        return web.Response(status=404, text="not found")
    try:
        proposal_id = int(parts[1])
    except ValueError:
        return web.Response(status=400, text="bad proposal id")
    action = parts[2]
    if action not in ("approve", "reject"):
        return web.Response(status=400, text="action must be approve or reject")

    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        body = {}
    actor = body.get("actor", "api")
    note = body.get("note", "")

    row = _get_proposal(proposal_id)
    if not row:
        return web.Response(status=404, text="proposal not found")
    if row["status"] != "pending":
        return web.json_response(
            {"error": f"already {row['status']}", "proposal": dict(row)},
            status=409,
        )

    session = request.app["session"]
    if action == "approve":
        merged, msg = await _merge_pr(session, row["repo"], row["pr_number"])
        status = "merged" if merged else "approved-pending-merge"
    else:
        _, msg = await _close_pr(session, row["repo"], row["pr_number"])
        status = "rejected"

    updated = _set_proposal_status(
        proposal_id, status, actor, f"{note} ({msg})".strip()
    )
    emoji = ":white_check_mark:" if action == "approve" else ":no_entry:"
    await _post_changelog(
        session,
        f"{emoji} **{status}** #{proposal_id} `{row['alertname']}` by {actor}",
    )
    return web.json_response(
        {"proposal": dict(updated) if updated else None, "result": msg}
    )


async def _health(_: web.Request) -> web.Response:
    return web.Response(text="ok")


# --------------------------------------------------------------------------- #
#  Main: bot + http server in one asyncio loop
# --------------------------------------------------------------------------- #


async def _main() -> None:
    if not _DEFAULT_WEBHOOK and not any(CHANNEL_WEBHOOKS.values()):
        raise SystemExit(
            "no Discord webhook configured — set DISCORD_WEBHOOK_DEFAULT "
            "or DISCORD_WEBHOOK_{CRITICAL,WARNING,INFO,DEALS}"
        )

    # Initialize DB on startup (creates tables if missing)
    with closing(_db()) as conn:
        log.info("proposals db ready at %s", PROPOSALS_DB)

    app = web.Application()
    app["session"] = ClientSession()
    app.router.add_post("/webhook", _handle_webhook)
    app.router.add_get("/healthz", _health)
    app.router.add_get("/proposals", _handle_proposals)
    app.router.add_post("/proposals/{id}/{action}", _handle_proposal_action)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    log.info(
        "listening :8080 holmes=%s model=%s severities=%s dedupe=%ds timeout=%ds "
        "channels=%s repos=%d bot=%s github_merge=%s",
        HOLMES_URL,
        HOLMES_MODEL,
        INVESTIGATE_SEVERITIES,
        DEDUPE_TTL,
        HOLMES_TIMEOUT,
        {k: bool(v) for k, v in CHANNEL_WEBHOOKS.items()},
        len(REPO_MAPPINGS),
        "on" if DISCORD_BOT_TOKEN else "off",
        "on" if GITHUB_TOKEN else "off",
    )

    # Start the discord bot in the same loop. Returns immediately if no token.
    bot_task = asyncio.create_task(_run_bot(app["session"]))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    bot_task.cancel()
    try:
        await bot_task
    except asyncio.CancelledError:
        pass
    await app["session"].close()
    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(_main())
