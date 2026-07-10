"""
render_artifacts.py  --  CloudFormation custom-resource Lambda.

Two jobs (selected by the resource's `Action` property):

  (default) render artifacts:
    * Read the TEMPLATED artifacts (notebook .ipynb/.py, utility, advisor module,
      best-practices pack, specs) from s3://<ArtifactBucket>/<ArtifactPrefix>/.
    * Substitute the customer's values for the {{SENTINEL}} tokens.
    * Write the rendered files back into the SEED bucket under a per-deployment
      prefix (<ArtifactPrefix>/rendered/<ResourcePrefix>/...), NOT into the
      customer's data (warehouse) bucket. The warehouse bucket holds Iceberg
      data + advisory reports only. Rendered layout the Glue jobs expect:
        notebooks/<NotebookJobName>.ipynb     scripts/<NotebookJobName>.py
        notebook/iceberg_nb_helpers.py
        utility/iceberg_utility.py            utility/specs/*.json
        agentic/iceberg_advisor.py            agentic/best_practices.json
        agentic/run_advisory.py
    * On DELETE, remove the rendered objects (best-effort).

  Action == "run_generator":
    * Start the generator Glue job and poll to completion (Full + CreateDemoData).

Sends a SUCCESS/FAILED signal back to CloudFormation via the pre-signed
ResponseURL (no cfnresponse dependency needed; we POST it ourselves).
"""
import json
import time
import urllib.request

import boto3

s3 = boto3.client("s3")
glue = boto3.client("glue")
lam = boto3.client("lambda")

# (artifact key under ArtifactPrefix)  ->  (target key, RELATIVE to RenderedPrefix
# in the seed bucket). {NB} is replaced with the notebook job name.
ARTIFACT_MAP = [
    ("notebook/iceberg_metadata_analytics.template.ipynb", "notebooks/{NB}.ipynb", True),
    ("notebook/iceberg_metadata_analytics.template.py",    "scripts/{NB}.py",      True),
    # Notebook helper module attached to the session via %extra_py_files. The
    # notebook's magic points at {{CODE_S3_BASE}}/notebook/iceberg_nb_helpers.py
    # (= <RenderedPrefix>/notebook/...), so the target key MUST be exactly that
    # tail. No {{SENTINEL}} tokens -> no substitution.
    ("notebook/iceberg_nb_helpers.py",                     "notebook/iceberg_nb_helpers.py", False),
    ("utility/iceberg_utility.py",                          "utility/iceberg_utility.py", False),
    ("utility/specs/demo.json",                             "utility/specs/demo.json", True),
    ("utility/specs/example_custom.json",                   "utility/specs/example_custom.json", False),
    ("agentic/iceberg_advisor.py",                          "agentic/iceberg_advisor.py", False),
    ("agentic/best_practices.json",                         "agentic/best_practices.json", False),
    # Headless advisory runner (user-driven, post-deploy). Reads all config from
    # Glue job args, so no substitution. Lives under agentic/ so it is skipped
    # when EnableAgentic=false (same rule as the other agentic/ assets).
    ("agentic/run_advisory.py",                             "agentic/run_advisory.py", False),
]


def _substitute(text, props):
    """Replace {{SENTINEL}} tokens with customer values.

    Note the notebook embeds TABLE_ALLOWLIST as a PYTHON literal, so we emit
    "None" or a Python list literal (e.g. ['a', 'b']) -- never JSON "null"."""
    allow = props.get("TableAllowList", "").strip()
    if allow:
        items = [t.strip() for t in allow.split(",") if t.strip()]
        allowlist_py = repr(items)            # e.g. ['orders', 'events']
    else:
        allowlist_py = "None"
    # Python bool literal so section H of the notebook no-ops cleanly when the
    # advisor assets weren't uploaded (EnableAgentic=false).
    agentic_py = "True" if props.get("EnableAgentic", "true") == "true" else "False"
    repl = {
        "{{WAREHOUSE_BUCKET}}": props["WarehouseBucket"],
        # Code base (seed bucket) the notebook uses for %extra_py_files + the
        # advisor pack. Distinct from WAREHOUSE_BUCKET, which is data-only now.
        "{{CODE_S3_BASE}}": props["CodeS3Base"],
        "{{GLUE_DB}}": props["GlueDatabase"],
        "{{REGION}}": props["Region"],
        "{{BEDROCK_MODEL}}": props.get("BedrockModelId", "us.amazon.nova-pro-v1:0"),
        "{{TABLE_ALLOWLIST_PY}}": allowlist_py,
        "{{AGENTIC_ENABLED_PY}}": agentic_py,
    }
    for k, v in repl.items():
        text = text.replace(k, v)
    return text


def _render(props):
    art_bucket = props["ArtifactBucket"]
    art_prefix = props["ArtifactPrefix"].rstrip("/")
    target = props["TargetBucket"]                    # seed bucket now
    rendered_prefix = props["RenderedPrefix"].rstrip("/")   # <art_prefix>/rendered/<res_prefix>
    nb = props["NotebookJobName"]
    want_agentic = props.get("EnableAgentic", "true") == "true"

    written = []
    for src_rel, dst_tmpl, do_subst in ARTIFACT_MAP:
        if src_rel.startswith("agentic/") and not want_agentic:
            continue
        src_key = f"{art_prefix}/{src_rel}"
        # Rendered code lands under the per-deployment prefix in the seed bucket.
        dst_key = f"{rendered_prefix}/{dst_tmpl.replace('{NB}', nb)}"
        body = s3.get_object(Bucket=art_bucket, Key=src_key)["Body"].read()
        if do_subst:
            body = _substitute(body.decode("utf-8"), props).encode("utf-8")
        s3.put_object(Bucket=target, Key=dst_key, Body=body)
        written.append(f"s3://{target}/{dst_key}")
    return written


def _cleanup(props):
    """Remove this deployment's rendered code + scratch from the seed bucket.

    Rendered code and glue-temp scratch both live under the per-deployment prefix
    in the seed bucket now, so a prefix sweep removes everything this stack wrote
    (the data/warehouse bucket is drained separately by _empty_bucket)."""
    target = props["TargetBucket"]
    rendered_prefix = props["RenderedPrefix"].rstrip("/")
    art_prefix = props["ArtifactPrefix"].rstrip("/")
    res_prefix = rendered_prefix.rsplit("/", 1)[-1]   # <ResourcePrefix>
    scratch_prefix = f"{art_prefix}/glue-temp/{res_prefix}"
    for prefix in (rendered_prefix, scratch_prefix):
        _delete_prefix(target, prefix)


def _delete_prefix(bucket, prefix):
    """Best-effort delete of every object under a prefix (paged)."""
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix + "/", "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        try:
            resp = s3.list_objects_v2(**kwargs)
        except Exception:
            return
        objs = [{"Key": o["Key"]} for o in resp.get("Contents", [])]
        if objs:
            try:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
            except Exception:
                pass
        if not resp.get("IsTruncated"):
            return
        token = resp.get("NextContinuationToken")


def _empty_bucket(bucket):
    """Delete all objects + versions + delete-markers so CFN can delete the
    (versioned) bucket on stack teardown.

    On a versioned bucket, delete_objects on a current object creates a new
    delete-marker, so a single pass can leave residue. Each iteration clears one
    full page (1000 objects); we page until a fresh listing returns nothing.
    Bound high enough for realistic artifact/demo buckets and surface a clear
    error if residue remains (the Delete handler logs it without blocking)."""
    MAX_ITERS = 1000
    for i in range(MAX_ITERS):
        try:
            resp = s3.list_object_versions(Bucket=bucket, MaxKeys=1000)
        except s3.exceptions.NoSuchBucket:
            return
        objs = [{"Key": v["Key"], "VersionId": v["VersionId"]}
                for v in resp.get("Versions", [])]
        objs += [{"Key": m["Key"], "VersionId": m["VersionId"]}
                 for m in resp.get("DeleteMarkers", [])]
        if not objs:
            return                      # truly empty
        s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
    raise RuntimeError(
        f"_empty_bucket: {bucket} still not empty after {MAX_ITERS} pages; "
        "manual cleanup may be required before the bucket can be deleted.")


# Sentinel returned when the generator is still running and this invocation is
# out of time -- tells the handler to re-invoke itself rather than signal CFN.
_STILL_RUNNING = object()

# Per-invocation poll budget. Well under the Lambda timeout so we always have
# room to fire the continuation invoke before being killed. The self-re-invoke
# chain (see handler) means total wait is NOT bounded by one Lambda's 15 min --
# it's bounded by CloudFormation's custom-resource signal timeout (~1 hour).
_POLL_BUDGET_SECONDS = 4 * 60


def _run_generator(props):
    """Start the generator job (first call) or resume polling an existing run.

    Returns a result dict on terminal SUCCESS, raises on terminal failure, or
    returns _STILL_RUNNING if the job is alive but this invocation ran out of
    poll budget (handler then re-invokes to continue polling)."""
    job = props["GeneratorJobName"]
    # RunId is carried across self-re-invokes so we poll ONE run, not start many.
    run_id = props.get("_GeneratorRunId") or glue.start_job_run(JobName=job)["JobRunId"]

    waited = 0
    while waited < _POLL_BUDGET_SECONDS:
        time.sleep(20)
        waited += 20
        state = glue.get_job_run(JobName=job, RunId=run_id)["JobRun"]["JobRunState"]
        if state == "SUCCEEDED":
            return {"jobRun": run_id, "state": state}
        if state in ("FAILED", "ERROR", "TIMEOUT", "STOPPED"):
            raise RuntimeError(f"Generator job {job} run {run_id} ended: {state}")
    # Out of budget but job still RUNNING/STARTING -> continue in a fresh invoke.
    props["_GeneratorRunId"] = run_id
    return _STILL_RUNNING


def _reinvoke(event, context):
    """Asynchronously invoke THIS function again with the same CFN event (which
    now carries _GeneratorRunId in ResourceProperties) so polling continues in a
    fresh 15-min window. We deliberately do NOT signal CloudFormation here -- the
    continuation invoke owns the signal. CFN waits up to ~1h for that signal."""
    lam.invoke(FunctionName=context.function_name,
               InvocationType="Event",       # async; returns immediately
               Payload=json.dumps(event).encode("utf-8"))


def _send(event, context, status, data, reason=""):
    body = json.dumps({
        "Status": status,
        "Reason": reason or f"See CloudWatch log stream: {context.log_stream_name}",
        "PhysicalResourceId": event.get("PhysicalResourceId") or context.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": data,
    }).encode("utf-8")
    req = urllib.request.Request(event["ResponseURL"], data=body, method="PUT",
                                 headers={"content-type": "", "content-length": str(len(body))})
    # Retry the CFN signal so a transient network blip on the response PUT can't
    # leave the stack hanging for an hour waiting for a signal that never comes.
    last = None
    for attempt in range(3):
        try:
            urllib.request.urlopen(req, timeout=20)
            return
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"WARN: CFN signal attempt {attempt + 1} failed: {e}", flush=True)
    print(f"ERROR: could not signal CloudFormation after retries: {last}", flush=True)


def handler(event, context):
    rtype = event["RequestType"]
    props = event.get("ResourceProperties", {})
    action = props.get("Action", "render")
    try:
        if rtype == "Delete":
            # empty_bucket: on stack delete, drain the created bucket so CFN can
            # delete it (versioned buckets can't be deleted while non-empty).
            if action == "empty_bucket":
                try:
                    _empty_bucket(props["TargetBucket"])
                except Exception:
                    pass   # never block teardown
            elif action == "render":
                _cleanup(props)
            _send(event, context, "SUCCESS", {"deleted": True})
            return

        # Create / Update
        if action == "run_generator":
            result = _run_generator(props)
            if result is _STILL_RUNNING:
                # Job alive, this invocation out of budget: continue in a fresh
                # invoke and DO NOT signal CFN yet (the continuation owns it).
                _reinvoke(event, context)
                print(f"generator still running; re-invoked to keep polling "
                      f"(run {props.get('_GeneratorRunId')})", flush=True)
                return
            data = result
        elif action == "empty_bucket":
            data = {"noop": "empty_bucket only acts on Delete"}
        else:
            written = _render(props)
            data = {"written": written, "count": len(written)}
        _send(event, context, "SUCCESS", data)
    except Exception as e:
        # Log full detail to CloudWatch; send only the exception TYPE + log pointer
        # to CloudFormation (avoid echoing raw exception text into stack events).
        print(f"ERROR: {type(e).__name__}: {e}", flush=True)
        _send(event, context, "FAILED", {},
              reason=f"{type(e).__name__} -- see CloudWatch log stream {context.log_stream_name}")
