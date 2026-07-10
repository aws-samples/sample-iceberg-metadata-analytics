#!/usr/bin/env python3
"""
test_advisor_bedrock.py  --  end-to-end test of the agentic advisor against
LIVE Bedrock, using a stub metrics provider seeded with the REAL measured
numbers from the three demo tables. No Spark needed.

Verifies: tool dispatch, best-practice citation retrieval, propose_remediation,
and the execute_remediation gate (denied unless approved).
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from iceberg_advisor import IcebergAdvisor, AdvisorConfig   # noqa: E402

# Real metrics measured from the live demo tables (sections A-G of the notebook).
STUB_METRICS = {
    "web_events_large": {
        "table": "web_events_large", "partitioned": True, "data_files": 7,
        "small_files": 0, "delete_files": 0, "total_mb": 470.4, "avg_file_mb": 67.2,
        "snapshots": 1, "partitions": 7, "skew_cv": 0.0, "hot_partition_share": 0.143,
        "delete_ratio": 0.0, "n_manifests": 1, "avg_files_per_manifest": 7.0,
    },
    "clickstream_micro": {
        "table": "clickstream_micro", "partitioned": True, "data_files": 577,
        "small_files": 577, "delete_files": 13, "total_mb": 1.7, "avg_file_mb": 0.003,
        "snapshots": 50, "partitions": 12, "skew_cv": 1.527, "hot_partition_share": 0.427,
        "delete_ratio": 0.023, "n_manifests": 51, "avg_files_per_manifest": 11.3,
        "top_partitions": [
            {"partition": "US", "files": 49, "rows": 7849, "avg_file_kb": 6.3},
            {"partition": "NL", "files": 48, "rows": 77, "avg_file_kb": 2.2},
        ],
    },
    "customers_unpartitioned": {
        "table": "customers_unpartitioned", "partitioned": False, "data_files": 3,
        "small_files": 3, "delete_files": 0, "total_mb": 2.8, "avg_file_mb": 0.93,
        "snapshots": 2, "partitions": None, "delete_ratio": 0.0,
        "n_manifests": 2, "avg_files_per_manifest": 3.0,
    },
}


def metrics_provider(table=None):
    if table:
        return STUB_METRICS.get(table, {"error": f"no such table {table}"})
    return {"tables": list(STUB_METRICS.values())}


def remediation_proposer(table):
    m = STUB_METRICS.get(table, {})
    props = []
    if m.get("small_files", 0) > max(4, 0.3 * m.get("data_files", 0)):
        props.append({"priority": "HIGH", "action": "compact",
                      "command": f"CALL glue_catalog.system.rewrite_data_files(table => 'iceberg_meta_analytics.{table}', options => map('target-file-size-bytes','536870912','min-input-files','5'))"})
    if m.get("snapshots", 0) > 20:
        props.append({"priority": "MEDIUM", "action": "expire_snapshots",
                      "command": f"CALL glue_catalog.system.expire_snapshots(table => 'iceberg_meta_analytics.{table}', retain_last => 5)"})
    if m.get("delete_files", 0) > 0:
        props.append({"priority": "MEDIUM", "action": "rewrite_position_deletes",
                      "command": f"CALL glue_catalog.system.rewrite_position_delete_files(table => 'iceberg_meta_analytics.{table}')"})
    if m.get("n_manifests", 0) > 20:
        props.append({"priority": "LOW", "action": "rewrite_manifests",
                      "command": f"CALL glue_catalog.system.rewrite_manifests(table => 'iceberg_meta_analytics.{table}')"})
    return props


def remediation_executor(table, action):
    # Stub — would run spark.sql(CALL ...) in the notebook.
    return {"stub": True, "would_run": f"{action} on {table}"}


def main():
    bp = json.load(open(os.path.join(os.path.dirname(__file__), "best_practices.json")))

    print("=" * 70, "\nTEST 1: ADVISE clickstream_micro (should cite best practices)\n", "=" * 70)
    advisor = IcebergAdvisor(metrics_provider, bp, remediation_proposer, remediation_executor,
                             AdvisorConfig(allow_execute=False, verbose=True))
    advisor.advise("clickstream_micro")

    print("\n" + "=" * 70, "\nTEST 2: REMEDIATE with execution DENIED (no approval)\n", "=" * 70)
    advisor2 = IcebergAdvisor(metrics_provider, bp, remediation_proposer, remediation_executor,
                              AdvisorConfig(allow_execute=False, verbose=True))
    advisor2.remediate("clickstream_micro")
    print("\n  >>> executed log (should be EMPTY):", advisor2.executed_log())
    assert advisor2.executed_log() == [], "FAIL: something executed without approval!"
    print("  >>> PASS: nothing executed without approval")

    print("\n" + "=" * 70, "\nTEST 3: REMEDIATE with compact APPROVED (gated execution allowed)\n", "=" * 70)
    advisor3 = IcebergAdvisor(metrics_provider, bp, remediation_proposer, remediation_executor,
                              AdvisorConfig(allow_execute=True,
                                            approved_actions={"clickstream_micro:compact"},
                                            verbose=True))
    advisor3.ask("Execute the approved compaction on clickstream_micro now.")
    print("\n  >>> executed log:", advisor3.executed_log())

    print("\n" + "=" * 70, "\nTEST 4: SUMMARIZE all tables\n", "=" * 70)
    advisor4 = IcebergAdvisor(metrics_provider, bp, remediation_proposer, remediation_executor,
                              AdvisorConfig(verbose=True))
    advisor4.summarize()

    print("\nALL ADVISOR TESTS COMPLETED.")


if __name__ == "__main__":
    main()
