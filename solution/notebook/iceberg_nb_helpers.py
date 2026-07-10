"""
iceberg_nb_helpers.py  --  analytics engine for the Iceberg Metadata Analytics
notebook.
================================================================================

All of the notebook's heavy lifting lives here so the notebook cells stay thin
and show only the *action* (call a method, `.show()` the result). The notebook
attaches this file to the Glue interactive session with:

    %extra_py_files <code-base>/notebook/iceberg_nb_helpers.py

and then, once the Spark session exists:

    import iceberg_nb_helpers as nb
    analytics = nb.IcebergAnalytics(spark, db=DB, table_allowlist=TABLE_ALLOWLIST)

Everything is a method on `IcebergAnalytics` so the SQL is written once, takes
`spark` explicitly (no notebook globals), and the advisor tool callables
(`metrics_provider`, `remediation_proposer`, `remediation_executor`) are just
bound methods with exactly the signatures `iceberg_advisor.IcebergAdvisor`
expects.

The methods return Spark DataFrames / Rows (or plain lists) so the notebook can
`.show()` them; none of them print, so the notebook controls all display.
"""
import statistics

from pyspark.sql import Row

# Files below this are "small" for the small-file detector (matches section A/B).
SMALL_FILE_THRESHOLD_MB = 32


class IcebergAnalytics:
    """Metadata-layer analytics over the Iceberg tables in a Glue database.

    Reads only Iceberg *metadata* tables (`.files`, `.partitions`, `.snapshots`,
    `.manifests`, `.history`), never the row data, so it is cheap on huge tables.
    """

    def __init__(self, spark, db, catalog="glue_catalog", table_allowlist=None,
                 small_file_threshold_mb=SMALL_FILE_THRESHOLD_MB):
        self.spark = spark
        self.db = db
        self.catalog = catalog
        self.table_allowlist = table_allowlist
        self.small_file_threshold_mb = small_file_threshold_mb
        self._partitioned_cache = {}
        # Discover once at construction so `analytics.tables` is ready to use.
        self.tables = self.discover_tables()

    # ---- identifiers ------------------------------------------------------- #
    @staticmethod
    def _q(ident):
        """Back-quote a SQL identifier so names with hyphens are valid
        (e.g. a Glue database derived from a hyphenated resource prefix)."""
        return "`" + ident.replace("`", "``") + "`"

    def fqtn(self, t):
        """Fully-qualified table name for SQL (db + table back-quoted)."""
        return f"{self.catalog}.{self._q(self.db)}.{self._q(t)}"

    # ---- discovery --------------------------------------------------------- #
    def discover_tables(self):
        """Bring-your-own-Iceberg friendly: analyze the allow-list if given, else
        auto-discover every Iceberg table in the configured Glue database."""
        if self.table_allowlist:
            return list(self.table_allowlist)
        rows = self.spark.sql(
            f"SHOW TABLES IN {self.catalog}.{self._q(self.db)}").collect()
        found = []
        for r in rows:
            name = r["tableName"]
            try:
                # Only keep Iceberg tables (have a .snapshots metadata table).
                self.spark.sql(
                    f"SELECT 1 FROM {self.catalog}.{self._q(self.db)}."
                    f"{self._q(name)}.snapshots LIMIT 1")
                found.append(name)
            except Exception:
                pass
        return found

    def is_partitioned(self, t):
        """True if the table has partition columns.

        Iceberg's `.partitions` metadata table exposes a struct column named
        `partition` ONLY for partitioned tables; unpartitioned tables omit it
        (and return a single synthetic row), so its presence is a reliable test.
        Cached: a table's partitioning does not change within a session."""
        if t not in self._partitioned_cache:
            cols = [f.name for f in
                    self.spark.table(f"{self.fqtn(t)}.partitions").schema.fields]
            self._partitioned_cache[t] = "partition" in cols
        return self._partitioned_cache[t]

    # ---- A. inventory & layout -------------------------------------------- #
    def inventory(self):
        """One metadata sweep per table -> the table-level 'dashboard' row."""
        rows = []
        for t in self.tables:
            # IMPORTANT: in Glue 5.0 the `.files` metadata table contains BOTH
            # data files (content=0) AND delete files (content=1/2). We filter
            # content = 0 for true data-file metrics, else delete files inflate
            # them.
            files = self.spark.sql(f"""
                SELECT COUNT(*)               AS data_files,
                       COALESCE(SUM(file_size_in_bytes),0) AS total_bytes,
                       COALESCE(AVG(file_size_in_bytes),0) AS avg_bytes,
                       COALESCE(MIN(file_size_in_bytes),0) AS min_bytes,
                       COALESCE(MAX(file_size_in_bytes),0) AS max_bytes
                FROM {self.fqtn(t)}.files
                WHERE content = 0
            """).first()

            small = self.spark.sql(f"""
                SELECT COUNT(*) AS small_files
                FROM {self.fqtn(t)}.files
                WHERE content = 0
                  AND file_size_in_bytes < {self.small_file_threshold_mb} * 1024 * 1024
            """).first()["small_files"]

            delete_files = self.spark.sql(f"""
                SELECT COUNT(*) AS c FROM {self.fqtn(t)}.files WHERE content != 0
            """).first()["c"]

            snaps = self.spark.sql(
                f"SELECT COUNT(*) c FROM {self.fqtn(t)}.snapshots").first()["c"]

            nparts = self.spark.sql(
                f"SELECT COUNT(*) c FROM {self.fqtn(t)}.partitions").first()["c"] \
                if self.is_partitioned(t) else None

            rows.append(Row(
                table=t,
                data_files=int(files["data_files"]),
                small_files=int(small),
                delete_files=int(delete_files),
                total_mb=round(files["total_bytes"] / 1024 / 1024, 1),
                avg_file_mb=round(files["avg_bytes"] / 1024 / 1024, 3),
                min_file_kb=round(files["min_bytes"] / 1024, 1),
                max_file_mb=round(files["max_bytes"] / 1024 / 1024, 2),
                snapshots=int(snaps),
                partitions=nparts,
            ))
        return self.spark.createDataFrame(rows)

    # ---- B. file-size distribution ---------------------------------------- #
    def file_size_histogram(self, t):
        return self.spark.sql(f"""
            SELECT
              CASE
                WHEN file_size_in_bytes <        128*1024 THEN '1. < 128 KB'
                WHEN file_size_in_bytes <       1024*1024 THEN '2. 128 KB - 1 MB'
                WHEN file_size_in_bytes <    8*1024*1024  THEN '3. 1 - 8 MB'
                WHEN file_size_in_bytes <   32*1024*1024  THEN '4. 8 - 32 MB'
                WHEN file_size_in_bytes <  128*1024*1024  THEN '5. 32 - 128 MB'
                ELSE '6. > 128 MB'
              END                                              AS size_bucket,
              COUNT(*)                                         AS n_files,
              ROUND(SUM(file_size_in_bytes)/1024.0/1024.0, 1)  AS total_mb,
              ROUND(SUM(record_count))                         AS total_rows
            FROM {self.fqtn(t)}.files
            WHERE content = 0          -- data files only (exclude delete files)
            GROUP BY 1 ORDER BY 1
        """)

    # ---- C. partition-level analytics ------------------------------------- #
    def partition_profile(self, t):
        """Per-partition file/row/byte profile straight from .partitions."""
        return self.spark.sql(f"""
            SELECT
              partition,
              record_count                                      AS rows,
              file_count                                        AS files,
              ROUND(total_data_file_size_in_bytes/1024.0/1024.0, 2) AS total_mb,
              ROUND( (total_data_file_size_in_bytes/1024.0)
                     / GREATEST(file_count,1), 1)               AS avg_file_kb
            FROM {self.fqtn(t)}.partitions
            ORDER BY files DESC
        """)

    def partition_skew(self, t):
        """Coefficient of variation of rows-per-partition (None if <2 parts)."""
        vals = [r["rows"] for r in
                self.spark.sql(
                    f"SELECT record_count AS rows FROM {self.fqtn(t)}.partitions"
                ).collect()]
        if len(vals) < 2:
            return None
        mean = statistics.mean(vals)
        cv = (statistics.pstdev(vals) / mean) if mean else 0.0
        return Row(table=t, n_partitions=len(vals),
                   min_rows=min(vals), max_rows=max(vals),
                   mean_rows=round(mean, 1),
                   skew_cv=round(cv, 3),
                   hot_partition_share=round(max(vals) / sum(vals), 3))

    def partition_skew_summary(self):
        """DataFrame of partition_skew for every partitioned table (or None)."""
        skew_rows = [self.partition_skew(t) for t in self.tables
                     if self.is_partitioned(t)]
        skew_rows = [r for r in skew_rows if r]
        return self.spark.createDataFrame(skew_rows) if skew_rows else None

    def small_file_partitions(self, t, avg_kb_threshold=1024, min_files=8):
        return self.spark.sql(f"""
            SELECT
              partition,
              file_count                                              AS files,
              ROUND((total_data_file_size_in_bytes/1024.0)/GREATEST(file_count,1),1) AS avg_file_kb,
              record_count                                            AS rows
            FROM {self.fqtn(t)}.partitions
            WHERE file_count >= {min_files}
              AND (total_data_file_size_in_bytes/1024.0)/GREATEST(file_count,1) < {avg_kb_threshold}
            ORDER BY files DESC
        """)

    # ---- D. snapshot & commit history ------------------------------------- #
    def snapshot_activity(self, t):
        return self.spark.sql(f"""
            SELECT
              h.made_current_at                       AS commit_time,
              s.operation,
              CAST(s.summary['added-data-files']  AS INT) AS added_files,
              CAST(s.summary['deleted-data-files']AS INT) AS deleted_files,
              CAST(s.summary['added-records']     AS LONG) AS added_records,
              CAST(s.summary['added-files-size']  AS LONG) AS added_bytes
            FROM {self.fqtn(t)}.snapshots s
            JOIN {self.fqtn(t)}.history  h ON s.snapshot_id = h.snapshot_id
            ORDER BY commit_time
        """)

    def ingest_fingerprint(self, t):
        """Avg files & bytes added per append commit (trickle-ingest signal)."""
        r = self.spark.sql(f"""
            SELECT
              COUNT(*)                                              AS append_commits,
              ROUND(AVG(CAST(summary['added-data-files'] AS INT)),1)        AS avg_files_per_commit,
              ROUND(AVG(CAST(summary['added-files-size'] AS LONG))/1024.0,1) AS avg_kb_per_commit,
              ROUND(AVG( CAST(summary['added-files-size'] AS DOUBLE)
                       / GREATEST(CAST(summary['added-data-files'] AS DOUBLE),1))/1024.0,1) AS avg_kb_per_file
            FROM {self.fqtn(t)}.snapshots
            WHERE operation = 'append'
        """).first()
        return Row(table=t, **r.asDict())

    def ingest_fingerprint_summary(self):
        return self.spark.createDataFrame(
            [self.ingest_fingerprint(t) for t in self.tables])

    # ---- E. delete-file (merge-on-read) pressure -------------------------- #
    def delete_pressure(self, t):
        r = self.spark.sql(f"""
            SELECT
              SUM(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_files,
              SUM(CASE WHEN content = 1 THEN 1 ELSE 0 END) AS pos_delete_files,
              SUM(CASE WHEN content = 2 THEN 1 ELSE 0 END) AS eq_delete_files
            FROM {self.fqtn(t)}.files
        """).first()
        data = r["data_files"] or 0
        dels = (r["pos_delete_files"] or 0) + (r["eq_delete_files"] or 0)
        return Row(table=t, data_files=int(data),
                   pos_delete_files=int(r["pos_delete_files"] or 0),
                   eq_delete_files=int(r["eq_delete_files"] or 0),
                   delete_ratio=round(dels / data, 3) if data else 0.0)

    def delete_pressure_summary(self):
        return self.spark.createDataFrame(
            [self.delete_pressure(t) for t in self.tables])

    # ---- F. manifest health ----------------------------------------------- #
    def manifest_health(self, t):
        return self.spark.sql(f"""
            SELECT
              COUNT(*)                                  AS n_manifests,
              SUM(added_data_files_count
                  + existing_data_files_count
                  + deleted_data_files_count)           AS files_tracked,
              ROUND(AVG(length))                        AS avg_manifest_bytes,
              ROUND( SUM(added_data_files_count
                       + existing_data_files_count
                       + deleted_data_files_count)
                     / GREATEST(COUNT(*),1), 1)         AS avg_files_per_manifest
            FROM {self.fqtn(t)}.manifests
        """)

    # ---- G. remediation playbook ------------------------------------------ #
    def remediation_playbook(self):
        """Prioritized, copy-pasteable CALL procedures derived from inventory.

        Distinct from the agentic proposer below: this is the deterministic
        section-G playbook (its own thresholds and target file size)."""
        recs = []
        for inv in self.inventory().collect():
            t = inv["table"]
            # 1. compaction for small files
            if inv["small_files"] > max(4, 0.3 * inv["data_files"]):
                recs.append((t, "HIGH", "Compact small files",
                             f"CALL {self.catalog}.system.rewrite_data_files("
                             f"table => '{self.db}.{t}', "
                             f"options => map('target-file-size-bytes','134217728','min-input-files','5'))"))
            # 2. expire snapshots if churny
            if inv["snapshots"] > 20:
                recs.append((t, "MEDIUM", f"Expire old snapshots ({inv['snapshots']} present)",
                             f"CALL {self.catalog}.system.expire_snapshots("
                             f"table => '{self.db}.{t}', older_than => TIMESTAMP '2099-01-01 00:00:00', retain_last => 5)"))
            # 3. delete-file pressure
            if inv["delete_files"] > 0:
                recs.append((t, "MEDIUM", f"Rewrite to clear {inv['delete_files']} delete files",
                             f"CALL {self.catalog}.system.rewrite_data_files("
                             f"table => '{self.db}.{t}', options => map('delete-file-threshold','1'))"))
            # 4. manifest rewrite if many files
            if inv["data_files"] > 200:
                recs.append((t, "LOW", "Rewrite manifests for faster planning",
                             f"CALL {self.catalog}.system.rewrite_manifests('{self.db}.{t}')"))
        if recs:
            return self.spark.createDataFrame(
                recs, ["table", "priority", "action", "command"])
        return None

    # ---- H. agentic advisor tool callables -------------------------------- #
    # These three bound methods have exactly the signatures IcebergAdvisor
    # expects for metrics_provider / remediation_proposer / remediation_executor.
    def table_metrics(self, t):
        """Rich per-table metric dict fed to the agent's get_metrics tool."""
        files = self.spark.sql(f"""
            SELECT COUNT(*) df, COALESCE(SUM(file_size_in_bytes),0) tb,
                   COALESCE(AVG(file_size_in_bytes),0) ab, COALESCE(MIN(file_size_in_bytes),0) mn
            FROM {self.fqtn(t)}.files WHERE content = 0""").first()
        small = self.spark.sql(f"""SELECT COUNT(*) c FROM {self.fqtn(t)}.files
            WHERE content = 0 AND file_size_in_bytes < {self.small_file_threshold_mb}*1024*1024""").first()["c"]
        dels = self.spark.sql(
            f"SELECT COUNT(*) c FROM {self.fqtn(t)}.files WHERE content != 0").first()["c"]
        snaps = self.spark.sql(
            f"SELECT COUNT(*) c FROM {self.fqtn(t)}.snapshots").first()["c"]
        man = self.spark.sql(f"""SELECT COUNT(*) n,
            ROUND(SUM(added_data_files_count+existing_data_files_count+deleted_data_files_count)
                  / GREATEST(COUNT(*),1),1) afpm FROM {self.fqtn(t)}.manifests""").first()
        m = {"table": t, "partitioned": self.is_partitioned(t),
             "data_files": int(files["df"]), "small_files": int(small), "delete_files": int(dels),
             "total_mb": round(files["tb"]/1024/1024, 2), "avg_file_mb": round(files["ab"]/1024/1024, 3),
             "min_file_kb": round(files["mn"]/1024, 1), "snapshots": int(snaps),
             "delete_ratio": round(dels/files["df"], 3) if files["df"] else 0.0,
             "n_manifests": int(man["n"]), "avg_files_per_manifest": man["afpm"]}
        if self.is_partitioned(t):
            parts = self.spark.sql(f"""SELECT partition, record_count rc, file_count fc,
                ROUND(total_data_file_size_in_bytes/1024.0/file_count,1) avg_kb
                FROM {self.fqtn(t)}.partitions ORDER BY fc DESC""").collect()
            m["partitions"] = len(parts)
            m["top_partitions"] = [{"partition": str(r["partition"]), "files": r["fc"],
                                    "rows": r["rc"], "avg_file_kb": r["avg_kb"]} for r in parts[:8]]
            rows = [r["rc"] for r in parts]
            if len(rows) > 1 and statistics.mean(rows):
                m["skew_cv"] = round(statistics.pstdev(rows)/statistics.mean(rows), 3)
                m["hot_partition_share"] = round(max(rows)/sum(rows), 3)
        else:
            m["partitions"] = None
        return m

    def metrics_provider(self, table=None):
        if table:
            return self.table_metrics(table)
        return {"tables": [self.table_metrics(t) for t in self.tables]}

    def remediation_proposer(self, table):
        m = self.table_metrics(table)
        props = []
        if m["small_files"] > max(4, 0.3*m["data_files"]):
            props.append({"priority": "HIGH", "action": "compact",
                "command": f"CALL {self.catalog}.system.rewrite_data_files(table => '{self.db}.{table}', options => map('target-file-size-bytes','536870912','min-input-files','5'))"})
        if m["snapshots"] > 20:
            props.append({"priority": "MEDIUM", "action": "expire_snapshots",
                "command": f"CALL {self.catalog}.system.expire_snapshots(table => '{self.db}.{table}', retain_last => 5)"})
        if m["delete_files"] > 0:
            props.append({"priority": "MEDIUM", "action": "rewrite_position_deletes",
                "command": f"CALL {self.catalog}.system.rewrite_position_delete_files(table => '{self.db}.{table}')"})
        if m["n_manifests"] > 20:
            props.append({"priority": "LOW", "action": "rewrite_manifests",
                "command": f"CALL {self.catalog}.system.rewrite_manifests(table => '{self.db}.{table}')"})
        return props

    def _action_sql(self, action, t):
        """Maps an approved action name -> the actual CALL run via Spark."""
        sql = {
            "compact": f"CALL {self.catalog}.system.rewrite_data_files(table => '{self.db}.{t}', options => map('target-file-size-bytes','536870912','min-input-files','5'))",
            "expire_snapshots": f"CALL {self.catalog}.system.expire_snapshots(table => '{self.db}.{t}', retain_last => 5)",
            "rewrite_manifests": f"CALL {self.catalog}.system.rewrite_manifests(table => '{self.db}.{t}')",
            "rewrite_position_deletes": f"CALL {self.catalog}.system.rewrite_position_delete_files(table => '{self.db}.{t}')",
        }
        return sql[action]

    def remediation_executor(self, table, action):
        sql = self._action_sql(action, table)
        print(f"   [EXECUTING] {sql}")
        return self.spark.sql(sql).toJSON().collect()
