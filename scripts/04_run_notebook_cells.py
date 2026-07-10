#!/usr/bin/env python3
"""
04_run_notebook_cells.py

Definitive notebook test: extracts every CODE cell from the generated
iceberg_metadata_analytics.ipynb and runs them IN ORDER in a real Glue
interactive session -- exactly what the user will execute. Magic-only cells
(%idle_timeout, %%configure, etc.) are skipped because the interactive backend
consumes those directives itself; we replicate their effect via session args.

Prints PASS/FAIL per cell and the tail of each cell's output. Always tears the
session down.
"""
import json
import os
import sys
import time

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import REGION, ACCOUNT, ROLE_ARN, GLUE_DB, BUCKET  # noqa: E402

SESSION_ID = "iceberg-meta-analytics-cells"
# The account-specific notebook is a local build artifact (real values baked in),
# NOT part of the shippable solution/. Build it on demand from
# solution/notebook/build_notebook.py into a local, gitignored dir.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_BUILD = os.path.join(_SCRIPT_DIR, "..", "solution", "notebook", "build_notebook.py")
_NB_DIR = os.path.join(_SCRIPT_DIR, ".local", "notebook")
NB = os.path.join(_NB_DIR, "iceberg_metadata_analytics.ipynb")
if not os.path.exists(NB):
    import subprocess
    os.makedirs(_NB_DIR, exist_ok=True)
    _env = {**os.environ,
            "NB_WAREHOUSE_BUCKET": BUCKET,
            "NB_GLUE_DB": GLUE_DB,
            "NB_REGION": REGION}
    subprocess.run(["python3", _BUILD, "--outdir", _NB_DIR], check=True, env=_env)

glue = boto3.client("glue", region_name=REGION)


def magic_only(src):
    """True if the cell is a notebook magic the interactive kernel consumes
    (not Python we should execute here).

    Two cases:
      * cell magic  -> first non-blank line starts with '%%' (e.g. %%configure,
        %%sql); the WHOLE cell belongs to the magic, body included.
      * line magics -> every non-blank line starts with a single '%'
        (e.g. %idle_timeout, %glue_version).
    """
    lines = [ln.lstrip() for ln in src.splitlines() if ln.strip()]
    if not lines:
        return False
    if lines[0].startswith("%%"):
        return True
    return all(ln.startswith("%") for ln in lines)


def load_code_cells():
    """Return (python_cells, configure_args, extra_py_files).

    configure_args is the JSON object from the notebook's %%configure cell, fed
    into the session's DefaultArguments so this harness exercises EXACTLY the
    same session config the Glue Studio console builds from %%configure (e.g.
    the spark.sql.extensions --conf needed for CALL procedures).

    extra_py_files is the list of S3 paths from any %extra_py_files line magic.
    The interactive backend consumes that magic itself; we replicate its effect
    via the --extra-py-files session arg so `import iceberg_nb_helpers` works."""
    nb = json.load(open(NB))
    cells = []
    configure_args = {}
    extra_py_files = []
    for c in nb["cells"]:
        if c["cell_type"] != "code":
            continue
        src = "".join(c["source"])
        if magic_only(src):
            first = src.strip().splitlines()[0]
            if first.lstrip().startswith("%%configure"):
                body = src.split("%%configure", 1)[1].strip()
                try:
                    configure_args = json.loads(body)
                    print(f"   (parsed %%configure: {len(configure_args)} args)")
                except json.JSONDecodeError as e:
                    print(f"   (WARNING: could not parse %%configure: {e})")
            else:
                # Capture %extra_py_files paths from a line-magic cell so we can
                # attach the helper module to the session the same way Glue does.
                for ln in src.splitlines():
                    s = ln.strip()
                    if s.startswith("%extra_py_files"):
                        arg = s[len("%extra_py_files"):].strip()
                        extra_py_files += [p.strip() for p in arg.split(",") if p.strip()]
                print(f"   (skip magic cell: {first!r})")
            continue
        cells.append(src)
    return cells, configure_args, extra_py_files


def wait_ready():
    for _ in range(60):
        st = glue.get_session(Id=SESSION_ID)["Session"]["Status"]
        print(f"    status={st}")
        if st == "READY":
            return
        if st in ("FAILED", "STOPPED", "TIMEOUT"):
            raise RuntimeError(f"session {st}")
        time.sleep(10)
    raise RuntimeError("never READY")


def run(code, idx):
    rid = glue.run_statement(SessionId=SESSION_ID, Code=code)["Id"]
    while True:
        stmt = glue.get_statement(SessionId=SESSION_ID, Id=rid)["Statement"]
        if stmt["State"] in ("AVAILABLE", "ERROR", "CANCELLED"):
            break
        time.sleep(3)
    out = stmt.get("Output", {})
    if out.get("Status") == "ok":
        text = out.get("Data", {}).get("TextPlain", "")
        tail = "\n".join(text.strip().splitlines()[-12:])
        print(f"  PASS  cell {idx}")
        for ln in tail.splitlines():
            print(f"        | {ln}")
        return True
    print(f"  FAIL  cell {idx}")
    print(f"        {out.get('ErrorName','')}: {out.get('ErrorValue','')}")
    tb = out.get("Traceback", [])
    for ln in tb[-6:]:
        print(f"        {ln.rstrip()}")
    return False


def main():
    cells, configure_args, extra_py_files = load_code_cells()
    print(f">>> {len(cells)} python code cells to run")

    # The notebook attaches its helper module via %extra_py_files pointing at
    # s3://<bucket>/notebook/iceberg_nb_helpers.py. The interactive kernel would
    # stage that for us; here we upload the local module to that path and pass
    # --extra-py-files so `import iceberg_nb_helpers` resolves in the session.
    if extra_py_files:
        s3 = boto3.client("s3", region_name=REGION)
        _helper_local = os.path.join(
            _SCRIPT_DIR, "..", "solution", "notebook", "iceberg_nb_helpers.py")
        for uri in extra_py_files:
            assert uri.startswith("s3://"), f"unexpected extra_py_files path: {uri}"
            bkt, key = uri[len("s3://"):].split("/", 1)
            if os.path.basename(key) == "iceberg_nb_helpers.py":
                print(f"   uploading helper module -> {uri}")
                s3.upload_file(os.path.abspath(_helper_local), bkt, key)

    # Build session args from the notebook's own %%configure, falling back to a
    # minimal default if the notebook didn't specify them.
    default_args = {"--datalake-formats": "iceberg",
                    "--enable-glue-datacatalog": "true"}
    default_args.update(configure_args)
    if extra_py_files:
        default_args["--extra-py-files"] = ",".join(extra_py_files)

    try:
        glue.delete_session(Id=SESSION_ID); time.sleep(3)
    except Exception:
        pass

    print(">>> creating session with args:", json.dumps(default_args))
    glue.create_session(
        Id=SESSION_ID, Role=ROLE_ARN,
        Command={"Name": "glueetl", "PythonVersion": "3"},
        GlueVersion="5.0", WorkerType="G.1X", NumberOfWorkers=2, IdleTimeout=20,
        DefaultArguments=default_args,
        Tags={"project": "iceberg-meta-analytics", "managed-by": "blog-artifact"},
    )
    ok = True
    try:
        wait_ready()
        for i, code in enumerate(cells):
            if not run(code, i):
                ok = False
                # keep going to surface all failures

        # Extra check: prove the Iceberg CALL extension parses (section G), using
        # a THROWAWAY table so the 3 demo tables are never mutated.
        print(">>> extra: verifying CALL procedure parses (section G prerequisite)")
        call_probe = (
            f"t = 'glue_catalog.{DB}._nb_call_probe'\n"
            "spark.sql(f'DROP TABLE IF EXISTS {t} PURGE')\n"
            "spark.sql(f'CREATE TABLE {t} (id bigint) USING iceberg')\n"
            "spark.sql(f'INSERT INTO {t} VALUES (1)')\n"
            "spark.sql(f'INSERT INTO {t} VALUES (2)')\n"
            "spark.sql(\"CALL glue_catalog.system.rewrite_data_files("
            f"table => '{DB}._nb_call_probe', "
            "options => map('min-input-files','2'))\").show(truncate=False)\n"
            "spark.sql(f'DROP TABLE IF EXISTS {t} PURGE')\n"
            "print('CALL_OK')\n"
        )
        if not run(call_probe, "CALL-check"):
            ok = False
    finally:
        print(">>> deleting session")
        try:
            glue.delete_session(Id=SESSION_ID)
        except Exception:
            pass
    print("\n>>> RESULT:", "ALL CELLS PASS" if ok else "SOME CELLS FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
