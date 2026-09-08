"""Audit logging.

A task writes a RUNNING row when it starts and updates that row on completion, so a
task killed mid-flight leaves a RUNNING row behind rather than no evidence at all -
that is the state an operator most needs to see.

The `task` context manager wraps this: it records the start, converts an exception
into a FAILED row with the message and stack trace, and re-raises so the workflow task
still fails.
"""

from __future__ import annotations

import traceback
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..config import FrameworkConfig
from ..logging_utils import FrameworkLogger
from ..spark_utils import job_context, new_uuid

_MAX_STACKTRACE_CHARS = 8000


@dataclass
class TaskMetrics:
    """Counters a layer runner reports back for one task."""

    records_read: Optional[int] = None
    records_inserted: Optional[int] = None
    records_updated: Optional[int] = None
    records_deleted: Optional[int] = None
    records_rejected: Optional[int] = None
    files_processed: Optional[int] = None
    bytes_processed: Optional[int] = None
    target_row_count: Optional[int] = None

    def merge(self, other: "TaskMetrics") -> "TaskMetrics":
        """Sum two metric sets, treating None as absent rather than zero.

        Streaming runners accumulate one TaskMetrics per micro-batch, so this is what
        turns per-batch counters into a per-task total.
        """
        merged = TaskMetrics()
        for key in asdict(self):
            left, right = getattr(self, key), getattr(other, key)
            if left is None and right is None:
                merged_value = None
            else:
                merged_value = (left or 0) + (right or 0)
            setattr(merged, key, merged_value)
        return merged


@dataclass
class DQRunMetrics:
    """Summary of one table's DQ evaluation, written to dq_run_audit."""

    dq_check_outcome: str = "PASSED"
    pipeline_status: str = "SUCCEEDED"
    rules_evaluated: int = 0
    rules_failed: int = 0
    src_rec_count: int = 0
    quarantine_count: int = 0
    warning_count: int = 0
    target_rec_count: int = 0
    error_message: Optional[str] = None
    rule_details: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def quarantine_pct(self) -> float:
        if not self.src_rec_count:
            return 0.0
        return round(100.0 * self.quarantine_count / self.src_rec_count, 4)


_JOB_RUN_AUDIT_SCHEMA = StructType(
    [
        StructField("audit_id", StringType()),
        StructField("batch_id", StringType()),
        StructField("job_id", StringType()),
        StructField("job_name", StringType()),
        StructField("job_run_id", StringType()),
        StructField("task_name", StringType()),
        StructField("task_run_id", StringType()),
        StructField("layer", StringType()),
        StructField("catalog_name", StringType()),
        StructField("schema_name", StringType()),
        StructField("table_name", StringType()),
        StructField("source_object", StringType()),
        StructField("job_status", StringType()),
        StructField("records_read", LongType()),
        StructField("records_inserted", LongType()),
        StructField("records_updated", LongType()),
        StructField("records_deleted", LongType()),
        StructField("records_rejected", LongType()),
        StructField("files_processed", LongType()),
        StructField("bytes_processed", LongType()),
        StructField("target_row_count", LongType()),
        StructField("error_message", StringType()),
        StructField("error_stacktrace", StringType()),
        StructField("retry_count", IntegerType()),
        StructField("task_start_timestamp", TimestampType()),
        StructField("task_end_timestamp", TimestampType()),
        StructField("duration_seconds", DoubleType()),
        StructField("environment", StringType()),
        StructField("framework_version", StringType()),
        StructField("run_by", StringType()),
        StructField("cluster_id", StringType()),
        StructField("control_row_id", LongType()),
        StructField("audit_insert_ts", TimestampType()),
    ]
)

_DQ_RUN_AUDIT_SCHEMA = StructType(
    [
        StructField("audit_id", StringType()),
        StructField("batch_id", StringType()),
        StructField("dq_task_run_id", StringType()),
        StructField("source_catalog_name", StringType()),
        StructField("source_schema_name", StringType()),
        StructField("table_name", StringType()),
        StructField("target_catalog_name", StringType()),
        StructField("target_schema_name", StringType()),
        StructField("target_table_name", StringType()),
        StructField("quarantine_table_name", StringType()),
        StructField("pipeline_status", StringType()),
        StructField("dq_check_outcome", StringType()),
        StructField("rules_evaluated", IntegerType()),
        StructField("rules_failed", IntegerType()),
        StructField("src_rec_count", LongType()),
        StructField("quarantine_count", LongType()),
        StructField("warning_count", LongType()),
        StructField("target_rec_count", LongType()),
        StructField("quarantine_pct", DoubleType()),
        StructField("error_message", StringType()),
        StructField("dq_task_start_timestamp", TimestampType()),
        StructField("dq_task_end_timestamp", TimestampType()),
        StructField("environment", StringType()),
        StructField("audit_insert_ts", TimestampType()),
    ]
)

_DQ_DETAIL_SCHEMA = StructType(
    [
        StructField("audit_id", StringType()),
        StructField("batch_id", StringType()),
        StructField("dq_task_run_id", StringType()),
        StructField("catalog_name", StringType()),
        StructField("schema_name", StringType()),
        StructField("table_name", StringType()),
        StructField("column_name", StringType()),
        StructField("rule_id", StringType()),
        StructField("rule_type", StringType()),
        StructField("rule_expression", StringType()),
        StructField("dq_dimension", StringType()),
        StructField("severity", StringType()),
        StructField("rows_evaluated", LongType()),
        StructField("rows_failed", LongType()),
        StructField("pass_pct", DoubleType()),
        StructField("rule_status", StringType()),
        StructField("error_message", StringType()),
        StructField("evaluated_at", TimestampType()),
        StructField("environment", StringType()),
        StructField("audit_insert_ts", TimestampType()),
    ]
)

_FILE_AUDIT_SCHEMA = StructType(
    [
        StructField("batch_id", StringType()),
        StructField("stream_batch_id", LongType()),
        StructField("catalog_name", StringType()),
        StructField("schema_name", StringType()),
        StructField("table_name", StringType()),
        StructField("source_file_path", StringType()),
        StructField("source_file_name", StringType()),
        StructField("source_file_size", LongType()),
        StructField("source_file_mod_time", TimestampType()),
        StructField("record_count", LongType()),
        StructField("rescued_record_count", LongType()),
        StructField("ingested_at", TimestampType()),
        StructField("environment", StringType()),
    ]
)


class AuditLogger:
    def __init__(
        self,
        spark: SparkSession,
        cfg: FrameworkConfig,
        batch_id: Optional[str] = None,
        logger: Optional[FrameworkLogger] = None,
    ):
        self.spark = spark
        self.cfg = cfg
        self.ctx = job_context(spark)
        # Outside a workflow there is no parent run id, so a uuid keeps ad hoc runs
        # auditable and distinguishable from scheduled ones.
        self.batch_id = batch_id or self.ctx.get("job_run_id") or f"manual-{new_uuid()}"
        self.log = logger or FrameworkLogger({"batch_id": self.batch_id}, cfg.log_level)

    # -----------------------------------------------------------------------------
    # job_run_audit
    # -----------------------------------------------------------------------------
    def start_task(
        self,
        layer: str,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        source_object: Optional[str] = None,
        control_row_id: Optional[int] = None,
    ) -> str:
        """Write the RUNNING row and return its audit_id."""
        audit_id = new_uuid()
        now = _utcnow()
        row = {
            "audit_id": audit_id,
            "batch_id": self.batch_id,
            "job_id": self.ctx.get("job_id"),
            "job_name": self.ctx.get("job_name"),
            "job_run_id": self.ctx.get("job_run_id"),
            "task_name": self.ctx.get("task_name"),
            "task_run_id": self.ctx.get("task_run_id"),
            "layer": layer,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "source_object": source_object,
            "job_status": "RUNNING",
            "records_read": None,
            "records_inserted": None,
            "records_updated": None,
            "records_deleted": None,
            "records_rejected": None,
            "files_processed": None,
            "bytes_processed": None,
            "target_row_count": None,
            "error_message": None,
            "error_stacktrace": None,
            "retry_count": 0,
            "task_start_timestamp": now,
            "task_end_timestamp": None,
            "duration_seconds": None,
            "environment": self.cfg.environment,
            "framework_version": self.cfg.framework_version,
            "run_by": self.ctx.get("run_by"),
            "cluster_id": self.ctx.get("cluster_id"),
            "control_row_id": control_row_id,
            "audit_insert_ts": now,
        }
        self._append("job_run_audit", [row], _JOB_RUN_AUDIT_SCHEMA)
        return audit_id

    def complete_task(
        self,
        audit_id: str,
        status: str,
        metrics: Optional[TaskMetrics] = None,
        error: Optional[BaseException] = None,
    ) -> None:
        """Update the RUNNING row to its terminal state."""
        metrics = metrics or TaskMetrics()
        error_message = f"{type(error).__name__}: {error}" if error else None
        stacktrace = (
            "".join(traceback.format_exception(type(error), error, error.__traceback__))[
                :_MAX_STACKTRACE_CHARS
            ]
            if error
            else None
        )
        set_clause = """
                 t.job_status         = :status,
                 t.records_read       = :records_read,
                 t.records_inserted   = :records_inserted,
                 t.records_updated    = :records_updated,
                 t.records_deleted    = :records_deleted,
                 t.records_rejected   = :records_rejected,
                 t.files_processed    = :files_processed,
                 t.bytes_processed    = :bytes_processed,
                 t.target_row_count   = :target_row_count,
                 t.error_message      = :error_message,
                 t.error_stacktrace   = :error_stacktrace,
                 t.task_end_timestamp = current_timestamp(),
                 t.duration_seconds   = unix_timestamp(current_timestamp())
                                        - unix_timestamp(t.task_start_timestamp)
        """
        self.spark.sql(
            f"""
            UPDATE {self.cfg.audit_table('job_run_audit')} t
               SET {set_clause}
             WHERE t.audit_id = :audit_id
            """,
            args={
                "audit_id": audit_id,
                "status": status,
                "records_read": metrics.records_read,
                "records_inserted": metrics.records_inserted,
                "records_updated": metrics.records_updated,
                "records_deleted": metrics.records_deleted,
                "records_rejected": metrics.records_rejected,
                "files_processed": metrics.files_processed,
                "bytes_processed": metrics.bytes_processed,
                "target_row_count": metrics.target_row_count,
                "error_message": error_message,
                "error_stacktrace": stacktrace,
            },
        )

    @contextmanager
    def task(
        self,
        layer: str,
        catalog_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        source_object: Optional[str] = None,
        control_row_id: Optional[int] = None,
    ) -> Iterator[TaskMetrics]:
        """Audit a task: RUNNING on entry, SUCCEEDED/FAILED on exit.

        Yields a mutable TaskMetrics the body fills in; whatever it holds at exit is
        what gets audited, so a failure still records the rows it managed to write.
        """
        audit_id = self.start_task(layer, catalog_name, schema_name, table_name, source_object, control_row_id)
        metrics = TaskMetrics()
        self.log.info(
            "task started",
            layer=layer,
            target=_join_name(catalog_name, schema_name, table_name),
            audit_id=audit_id,
        )
        try:
            yield metrics
        except BaseException as exc:  # noqa: BLE001 - audited, then re-raised
            self.complete_task(audit_id, "FAILED", metrics, exc)
            self.log.error(
                "task failed",
                exc_info=True,
                layer=layer,
                target=_join_name(catalog_name, schema_name, table_name),
                audit_id=audit_id,
            )
            raise
        else:
            self.complete_task(audit_id, "SUCCEEDED", metrics)
            self.log.info(
                "task succeeded",
                layer=layer,
                target=_join_name(catalog_name, schema_name, table_name),
                audit_id=audit_id,
                **{k: v for k, v in asdict(metrics).items() if v is not None},
            )

    # -----------------------------------------------------------------------------
    # dq_run_audit / dq_result_detail
    # -----------------------------------------------------------------------------
    def log_dq_run(
        self,
        metrics: DQRunMetrics,
        source_catalog: str,
        source_schema: str,
        table_name: str,
        target_catalog: str,
        target_schema: str,
        target_table: str,
        quarantine_table: Optional[str],
        started_at: datetime,
    ) -> str:
        audit_id = new_uuid()
        now = _utcnow()
        summary = {
            "audit_id": audit_id,
            "batch_id": self.batch_id,
            "dq_task_run_id": self.ctx.get("task_run_id") or audit_id,
            "source_catalog_name": source_catalog,
            "source_schema_name": source_schema,
            "table_name": table_name,
            "target_catalog_name": target_catalog,
            "target_schema_name": target_schema,
            "target_table_name": target_table,
            "quarantine_table_name": quarantine_table,
            "pipeline_status": metrics.pipeline_status,
            "dq_check_outcome": metrics.dq_check_outcome,
            "rules_evaluated": metrics.rules_evaluated,
            "rules_failed": metrics.rules_failed,
            "src_rec_count": metrics.src_rec_count,
            "quarantine_count": metrics.quarantine_count,
            "warning_count": metrics.warning_count,
            "target_rec_count": metrics.target_rec_count,
            "quarantine_pct": metrics.quarantine_pct,
            "error_message": metrics.error_message,
            "dq_task_start_timestamp": started_at,
            "dq_task_end_timestamp": now,
            "environment": self.cfg.environment,
            "audit_insert_ts": now,
        }
        self._append("dq_run_audit", [summary], _DQ_RUN_AUDIT_SCHEMA)

        if metrics.rule_details:
            details = [
                {
                    "audit_id": new_uuid(),
                    "batch_id": self.batch_id,
                    "dq_task_run_id": summary["dq_task_run_id"],
                    "catalog_name": source_catalog,
                    "schema_name": source_schema,
                    "table_name": table_name,
                    "column_name": d.get("column_name"),
                    "rule_id": d.get("rule_id"),
                    "rule_type": d.get("rule_type"),
                    "rule_expression": d.get("rule_expression"),
                    "dq_dimension": d.get("dq_dimension"),
                    "severity": d.get("severity"),
                    "rows_evaluated": d.get("rows_evaluated"),
                    "rows_failed": d.get("rows_failed"),
                    "pass_pct": d.get("pass_pct"),
                    "rule_status": d.get("rule_status"),
                    "error_message": d.get("error_message"),
                    "evaluated_at": now,
                    "environment": self.cfg.environment,
                    "audit_insert_ts": now,
                }
                for d in metrics.rule_details
            ]
            self._append("dq_result_detail", details, _DQ_DETAIL_SCHEMA)
        return audit_id

    # -----------------------------------------------------------------------------
    # bronze_file_audit
    # -----------------------------------------------------------------------------
    def log_files(self, file_stats: DataFrame) -> None:
        """Append pre-shaped per-file statistics produced by the ingestion module."""
        if file_stats is None:
            return
        target = self.cfg.audit_table("bronze_file_audit")
        expected = [f.name for f in _FILE_AUDIT_SCHEMA.fields]
        file_stats.select(*expected).write.format("delta").mode("append").saveAsTable(target)

    # -----------------------------------------------------------------------------
    def _append(self, table: str, rows: List[Dict[str, Any]], schema: StructType) -> None:
        if not rows:
            return
        ordered = [tuple(row.get(f.name) for f in schema.fields) for row in rows]
        df = self.spark.createDataFrame(ordered, schema=schema)
        df.write.format("delta").mode("append").saveAsTable(self.cfg.audit_table(table))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _join_name(*parts: Optional[str]) -> Optional[str]:
    present = [p for p in parts if p]
    return ".".join(present) if present else None
