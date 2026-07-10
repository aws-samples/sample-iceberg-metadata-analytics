"""
iceberg_advisor.py  --  Agentic Iceberg advisory layer (Amazon Bedrock + Claude)
================================================================================

A small, self-contained agentic layer for the Iceberg Metadata Analytics
notebook. It runs a Bedrock Converse tool-use loop with Claude and exposes four
tools over the table metadata:

  1. get_metrics(table?)         - read the structured metrics (sections A-G)
  2. lookup_best_practice(topic) - retrieve CITED Iceberg guidance from a local pack
  3. propose_remediation(table)  - candidate OPTIMIZE/expire/rewrite CALLs (no execution)
  4. execute_remediation(...)    - run a CALL procedure -- GATED behind explicit approval

Three actions the user can ask for:
  - "summarize"  : plain summary of table state
  - "advise"     : highlight issues + advisory, grounded in cited best practices
  - "remediate"  : propose remediation, then execute ONLY if the user approves

Design notes
------------
* Bedrock has no server-side agent loop, so we host a manual Converse tool-use
  loop (loop until stop_reason != "tool_use").
* `execute_remediation` refuses unless `AdvisorConfig.allow_execute` is True AND
  the specific action was approved -- the notebook sets this only after a human
  sets APPROVE=True. The agent can never mutate a table on its own.
* Only table METADATA (counts, sizes, partition values, snapshot summaries) is
  sent to Bedrock -- never row data. Partition values can be sensitive in real
  deployments; AdvisorConfig.redact_partitions masks them if needed.

This module is engine-agnostic about *how* metrics are produced: you pass in a
`metrics_provider` callable (the notebook wires it to Spark; tests wire a stub).
"""
import json
from dataclasses import dataclass, field
from typing import Callable, Optional

import boto3

# Bedrock Converse is model-agnostic, so this loop works unchanged for Amazon
# Nova and Anthropic Claude alike. Nova Pro is the default tier; pass any
# invokable inference-profile id via AdvisorConfig.model_id to switch.
DEFAULT_MODEL = "us.amazon.nova-pro-v1:0"
REGION = "us-west-2"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass
class AdvisorConfig:
    model_id: str = DEFAULT_MODEL
    region: str = REGION
    max_tokens: int = 4096
    max_turns: int = 12
    allow_execute: bool = False          # master gate for execute_remediation
    approved_actions: set = field(default_factory=set)   # specific approved CALLs
    redact_partitions: bool = False
    verbose: bool = True


SYSTEM_PROMPT = """You are an Apache Iceberg table-maintenance advisor embedded in an AWS Glue notebook.

You help a data engineer understand and maintain Iceberg tables in the AWS Glue Data Catalog (queried via Athena, maintained with Spark). You have four tools: get_metrics, lookup_best_practice, propose_remediation, and execute_remediation.

## Mandatory tool protocol — follow exactly, do not skip steps

You have NO reliable built-in knowledge of this deployment's numbers, best-practice sources, or exact CALL syntax. Every factual claim MUST come from a tool result in THIS conversation. Before writing your answer:

1. ALWAYS call get_metrics first. Never state a file count, size, snapshot count, or partition figure that did not come from a get_metrics result.
2. For EACH issue you raise, you MUST call lookup_best_practice for that topic BEFORE describing the issue or recommending a fix. Cite ONLY the exact `source`/`sources` URL string returned by that tool.
3. For ANY maintenance command, you MUST call propose_remediation and use its returned `command` string VERBATIM. Do not compose, edit, or "clean up" the SQL yourself.

## Absolute prohibitions

- NEVER invent, guess, complete, or "recall" a URL. If lookup_best_practice did not return a URL for a topic, write "no cited source available" — do NOT produce a plausible-looking iceberg.apache.org or aws.amazon.com link. Fabricated citations are the single worst failure you can make here.
- NEVER write a `CALL ...` command from memory. The only valid commands are the exact `command` strings returned by propose_remediation. In particular the catalog name and the `table => '...'` / `options => map(...)` named-argument syntax come from the tool, not from you — a command you wrote yourself will be wrong and will fail.
- NEVER claim you executed something unless an execute_remediation tool result confirms `executed: true`.

## Style and remediation gate

- Tie each recommendation to a specific metric from get_metrics (e.g. "1153 files averaging 2 KB" -> small-file problem).
- Severity: HIGH = actively hurting query performance or cost now; MEDIUM = will degrade; LOW = hygiene.
- Propose with propose_remediation first. Call execute_remediation ONLY for an action the user has explicitly approved; otherwise present the tool's verbatim command for the user to run themselves and stop.

Lead with the outcome. Put the headline first, then the supporting detail."""


# --------------------------------------------------------------------------- #
# Tool definitions (Bedrock Converse toolConfig)
# --------------------------------------------------------------------------- #
def tool_config():
    return {"tools": [
        {"toolSpec": {
            "name": "get_metrics",
            "description": "Return structured metadata metrics for one table (or all tables if omitted): "
                           "data_files, small_files, delete_files, total_mb, avg_file_mb, snapshots, "
                           "partitions, per-partition file/row counts, skew, manifest health, commit history.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "table": {"type": "string", "description": "Table name; omit for all tables."}
            }}}}},
        {"toolSpec": {
            "name": "lookup_best_practice",
            "description": "Retrieve cited Apache Iceberg / AWS best-practice guidance for a topic. "
                           "Returns recommendation text, the relevant property or CALL procedure, "
                           "any threshold, and a source URL to cite.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "topic": {"type": "string",
                          "description": "One of: small_files, partitioning, snapshots, manifests, "
                                         "merge_on_read, orphan_files, format_v2, compaction_strategy. "
                                         "Free text is matched too."}
            }, "required": ["topic"]}}}},
        {"toolSpec": {
            "name": "propose_remediation",
            "description": "Return candidate maintenance CALL procedures (compaction, expire_snapshots, "
                           "rewrite_manifests, rewrite_position_deletes) for a table, based on its metrics. "
                           "Does NOT execute anything.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "table": {"type": "string"}
            }, "required": ["table"]}}}},
        {"toolSpec": {
            "name": "execute_remediation",
            "description": "Execute a single maintenance CALL procedure against a table. This MUTATES the table. "
                           "Only call this when the user has explicitly approved this specific action.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "table": {"type": "string"},
                "action": {"type": "string",
                           "description": "One of: compact, expire_snapshots, rewrite_manifests, rewrite_position_deletes"}
            }, "required": ["table", "action"]}}}},
    ]}


# --------------------------------------------------------------------------- #
# The advisor
# --------------------------------------------------------------------------- #
class IcebergAdvisor:
    def __init__(self,
                 metrics_provider: Callable[[Optional[str]], dict],
                 best_practices: dict,
                 remediation_proposer: Callable[[str], list],
                 remediation_executor: Optional[Callable[[str, str], dict]] = None,
                 config: AdvisorConfig = None):
        self.metrics_provider = metrics_provider
        self.best_practices = best_practices
        self.remediation_proposer = remediation_proposer
        self.remediation_executor = remediation_executor
        self.cfg = config or AdvisorConfig()
        self.client = boto3.client("bedrock-runtime", region_name=self.cfg.region)
        self._executed = []   # audit log of executed actions

    # ---- tool implementations --------------------------------------------- #
    def _t_get_metrics(self, table=None):
        m = self.metrics_provider(table)
        if self.cfg.redact_partitions:
            m = _redact(m)
        return m

    def _t_lookup_best_practice(self, topic):
        topic_l = (topic or "").lower().strip()
        bp = self.best_practices
        # exact key, else substring match against keys + tags
        if topic_l in bp:
            return bp[topic_l]
        for key, entry in bp.items():
            tags = " ".join(entry.get("tags", [])) + " " + key
            if topic_l in tags or any(w in tags for w in topic_l.split()):
                return entry
        return {"note": f"No best-practice entry for '{topic}'. Available topics: {sorted(bp.keys())}"}

    def _t_propose_remediation(self, table):
        return {"table": table, "proposals": self.remediation_proposer(table)}

    def _t_execute_remediation(self, table, action):
        key = f"{table}:{action}"
        if not self.cfg.allow_execute:
            return {"executed": False,
                    "reason": "Execution is disabled. Set allow_execute=True and approve the action "
                              "(APPROVE in the notebook) before the agent can run remediation."}
        if key not in self.cfg.approved_actions:
            return {"executed": False,
                    "reason": f"Action '{key}' was not approved. Approved: {sorted(self.cfg.approved_actions)}. "
                              "Present the command for the user to run instead."}
        if self.remediation_executor is None:
            return {"executed": False, "reason": "No executor wired in this environment."}
        result = self.remediation_executor(table, action)
        self._executed.append({"action": key, "result": result})
        return {"executed": True, "table": table, "action": action, "result": result}

    def _dispatch(self, name, inp):
        try:
            if name == "get_metrics":
                return self._t_get_metrics(inp.get("table"))
            if name == "lookup_best_practice":
                return self._t_lookup_best_practice(inp.get("topic", ""))
            if name == "propose_remediation":
                return self._t_propose_remediation(inp["table"])
            if name == "execute_remediation":
                return self._t_execute_remediation(inp["table"], inp["action"])
            return {"error": f"unknown tool {name}"}
        except Exception as e:
            return {"error": f"{type(e).__name__}: {e}"}

    # ---- the agent loop --------------------------------------------------- #
    def ask(self, user_request: str) -> str:
        messages = [{"role": "user", "content": [{"text": user_request}]}]
        tools = tool_config()
        final_text = []

        for turn in range(self.cfg.max_turns):
            resp = self.client.converse(
                modelId=self.cfg.model_id,
                system=[{"text": SYSTEM_PROMPT}],
                messages=messages,
                toolConfig=tools,
                inferenceConfig={"maxTokens": self.cfg.max_tokens},
            )
            out = resp["output"]["message"]
            messages.append(out)
            stop = resp.get("stopReason")

            # collect any assistant text
            for block in out["content"]:
                if "text" in block and block["text"].strip():
                    final_text.append(block["text"])
                    if self.cfg.verbose:
                        print(block["text"], flush=True)

            if stop != "tool_use":
                break

            # run every tool call this turn, return all results in one user msg
            tool_results = []
            for block in out["content"]:
                if "toolUse" not in block:
                    continue
                tu = block["toolUse"]
                if self.cfg.verbose:
                    print(f"\n  [tool] {tu['name']}({json.dumps(tu['input'])})", flush=True)
                result = _json_safe(self._dispatch(tu["name"], tu["input"]))
                tool_results.append({"toolResult": {
                    "toolUseId": tu["toolUseId"],
                    "content": [{"json": result}],
                }})
            messages.append({"role": "user", "content": tool_results})
        else:
            final_text.append("\n[advisor: hit max turns]")

        return "\n".join(final_text)

    # convenience wrappers for the three named actions
    def summarize(self, table=None):
        scope = f"table {table}" if table else "all tables"
        return self.ask(f"Summarize the current state of {scope}. Plain summary, no remediation.")

    def advise(self, table=None):
        scope = f"table {table}" if table else "all tables"
        return self.ask(f"Analyze {scope}. Highlight issues and give advisory recommendations, "
                        f"each tied to a metric and a cited best practice. Do not execute anything.")

    def remediate(self, table):
        return self.ask(f"Propose remediation for table {table}. For each proposal, explain why "
                        f"(metric + cited best practice) and give the exact command. Execute only the "
                        f"actions that have been approved; otherwise present the commands for me to run.")

    def executed_log(self):
        return list(self._executed)


def _json_safe(obj):
    """Coerce a tool result into types the Bedrock Converse serializer accepts.

    Spark SQL ROUND()/aggregates return decimal.Decimal, and metadata columns
    can yield date/datetime — botocore's document serializer rejects all of
    those. Convert Decimal->float and date/datetime->isoformat str, recursively.
    """
    import decimal
    import datetime
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    if isinstance(obj, (bytes, bytearray)):
        return obj.decode("utf-8", "replace")
    return obj


# Metric keys that carry actual partition VALUES (which may be sensitive, e.g.
# a customer_id or region). Counts/sizes/ratios are NOT sensitive and are kept.
# Keep this list in sync with the metrics_provider output schema: the notebook's
# _table_metrics emits partition values under "partition" (and inside the
# "top_partitions" list, each item's "partition"). Add any new value-bearing
# key here if the provider schema grows.
_PARTITION_VALUE_KEYS = frozenset({"partition", "partition_value", "partition_values"})

def _redact(m):
    """Mask partition VALUES (keep counts/sizes/ratios) for privacy-sensitive
    deployments. Recurses through dicts AND lists so nested structures like
    top_partitions[].partition are covered."""
    import copy
    m = copy.deepcopy(m)

    def scrub(d):
        if isinstance(d, dict):
            for k, v in list(d.items()):
                if k in _PARTITION_VALUE_KEYS:
                    d[k] = "<redacted>"
                else:
                    scrub(v)
        elif isinstance(d, list):
            for x in d:
                scrub(x)

    scrub(m)
    return m
