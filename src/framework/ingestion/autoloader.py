"""Auto Loader ingestion: landing files -> bronze Delta tables.

One BronzeConfig row produces one structured streaming query. Both `batch` and
`stream` feeds run through the same reader; they differ only in the trigger, which is
why the design can treat them as one code path:

    batch  -> trigger(availableNow=True), the task ends when the backlog is drained
    stream -> trigger(processingTime=...) or trigger(continuous=...), long running

Every write goes through foreachBatch rather than a direct .toTable(), because a
micro-batch is where the framework can:

  * create the target with the right partitioning/clustering on first arrival,
  * de-duplicate and MERGE when write_mode = merge,
  * make the write idempotent with txnAppId/txnVersion, so a retried micro-batch
    cannot double-insert,
  * capture per-file lineage and per-batch counters for the audit tables.

foreachBatch executes on the driver, so accumulating counters in a plain dict is safe.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.window import Window

from ..audit import AuditLogger, TaskMetrics
from ..config import FrameworkConfig
from ..exceptions import IngestionError
from ..logging_utils import FrameworkLogger
from ..models import BronzeConfig
from ..spark_utils import (
    apply_liquid_clustering,
    apply_table_properties,
    ensure_schema,
    fq,
    normalise_columns,
    row_count,
    table_exists,
)

# Auto Loader formats where inferring types is worth the extra listing cost; parquet,
# avro and orc carry their own schema, and binaryFile/text have a fixed one.
_TYPE_INFERRING_FORMATS = {"csv", "json", "xml"}


@dataclass
class _StreamAccumulator:
    """Driver-side counters accumulated across micro-batches."""

    records_read: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    files_processed: int = 0
    bytes_processed: int = 0
    batches: int = 0
    file_rows: List[Dict[str, Any]] = field(default_factory=list)

    def as_task_metrics(self) -> TaskMetrics:
        return TaskMetrics(
            records_read=self.records_read,
            records_inserted=self.records_inserted,
            records_updated=self.records_updated or None,
            files_processed=self.files_processed,
            bytes_processed=self.bytes_processed,
        )


class AutoLoaderIngestor:
    def __init__(
        self,
        spark: SparkSession,
        cfg: FrameworkConfig,
        audit: AuditLogger,
        logger: Optional[FrameworkLogger] = None,
    ):
        self.spark = spark
        self.cfg = cfg
        self.audit = audit
        self.log = logger or FrameworkLogger({"component": "autoloader"}, cfg.log_level)

    # =============================================================================
    # public API
    # =============================================================================
    def ingest(self, bronze: BronzeConfig, await_timeout_seconds: Optional[int] = None) -> TaskMetrics:
        """Ingest one bronze feed and return its metrics.

        For a batch feed this returns once the available files are processed. For a
        streaming feed it blocks until `await_timeout_seconds` elapses, or forever
        when that is None - which is the right shape for a continuous workflow task.
        """
        self._validate(bronze)
        ensure_schema(self.spark, bronze.catalog, bronze.schema)

        log = self.log.child(target=bronze.full_name, source=bronze.file_location, load_type=bronze.load_type)
        log.info(
            "starting Auto Loader ingestion",
            file_type=bronze.source_file_type,
            trigger_mode=bronze.trigger_mode,
            write_mode=bronze.write_mode,
            schema_location=bronze.schema_location,
            checkpoint_location=bronze.checkpoint_location,
        )

        source = self._build_reader(bronze)
        source = self._add_metadata_columns(source, bronze)
        if bronze.normalise_column_names:
            source = normalise_columns(source)

        accumulator = _StreamAccumulator()
        query = self._start_query(bronze, source, accumulator, log)

        try:
            if await_timeout_seconds is not None:
                query.awaitTermination(await_timeout_seconds)
                if query.isActive:
                    log.info("await timeout reached, stopping the query gracefully")
                    query.stop()
                    query.awaitTermination()
            else:
                query.awaitTermination()
        except Exception as exc:
            raise IngestionError(f"Auto Loader ingestion failed for {bronze.full_name}: {exc}") from exc

        if query.exception() is not None:  # a terminated-with-error query
            raise IngestionError(
                f"Auto Loader stream for {bronze.full_name} terminated with an error: {query.exception()}"
            )

        self._flush_file_audit(accumulator, log)

        metrics = accumulator.as_task_metrics()
        metrics.target_row_count = row_count(self.spark, bronze.catalog, bronze.schema, bronze.table)
        log.info(
            "ingestion complete",
            micro_batches=accumulator.batches,
            records_read=metrics.records_read,
            records_inserted=metrics.records_inserted,
            files_processed=metrics.files_processed,
        )
        return metrics

    # =============================================================================
    # reader
    # =============================================================================
    def _build_reader(self, bronze: BronzeConfig) -> DataFrame:
        options = self._reader_options(bronze)
        self.log.debug("cloudFiles options resolved", target=bronze.full_name, options=options)
        reader = self.spark.readStream.format("cloudFiles").options(**options)
        return reader.load(bronze.file_location)

    def _reader_options(self, bronze: BronzeConfig) -> Dict[str, str]:
        """Assemble the cloudFiles option map.

        Precedence, lowest to highest: framework defaults, format-specific defaults,
        derived locations, control table columns, reader_options, cloud_files_options.
        The last one is the deliberate escape hatch - a new Auto Loader option can be
        used from metadata without a framework release.
        """
        options: Dict[str, str] = {
            "cloudFiles.format": bronze.source_file_type,
            "cloudFiles.schemaLocation": bronze.schema_location,
            "cloudFiles.schemaEvolutionMode": bronze.schema_evolution_mode,
            "rescuedDataColumn": bronze.rescued_data_column,
        }

        # Without inferColumnTypes, Auto Loader reads every CSV/JSON column as a
        # string, which pushes the cast burden onto every silver mapping.
        if bronze.source_file_type in _TYPE_INFERRING_FORMATS:
            options["cloudFiles.inferColumnTypes"] = str(
                self.cfg.default_for("infer_column_types", True)
            ).lower()

        if bronze.source_file_type == "csv":
            options.setdefault("header", "true")

        if bronze.schema_hints:
            options["cloudFiles.schemaHints"] = bronze.schema_hints
        if bronze.source_file_pattern:
            options["pathGlobFilter"] = bronze.source_file_pattern
        if bronze.max_files_per_trigger:
            options["cloudFiles.maxFilesPerTrigger"] = str(bronze.max_files_per_trigger)
        if bronze.max_bytes_per_trigger:
            options["cloudFiles.maxBytesPerTrigger"] = str(bronze.max_bytes_per_trigger)

        # File notification mode scales past the ~millions-of-files point where
        # directory listing stops being viable; it is opt-in per feed.
        default_notifications = self.cfg.default_for("use_notifications")
        if default_notifications is not None:
            options["cloudFiles.useNotifications"] = str(default_notifications).lower()

        options.update(bronze.reader_options)
        options.update(bronze.cloud_files_options)
        return {k: str(v) for k, v in options.items()}

    def _add_metadata_columns(self, df: DataFrame, bronze: BronzeConfig) -> DataFrame:
        """Attach ingestion lineage columns from the file `_metadata` struct.

        _source_file is what makes "which file did this bad row come from" answerable
        months later, and it is the join key for bronze_file_audit.
        """
        if not bronze.add_ingestion_metadata:
            return df
        return (
            df.withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_source_file_size", F.col("_metadata.file_size"))
            .withColumn("_source_file_mod_time", F.col("_metadata.file_modification_time"))
            .withColumn("_batch_id", F.lit(self.audit.batch_id))
        )

    # =============================================================================
    # writer
    # =============================================================================
    def _start_query(
        self,
        bronze: BronzeConfig,
        source: DataFrame,
        accumulator: _StreamAccumulator,
        log: FrameworkLogger,
    ) -> StreamingQuery:
        writer = (
            source.writeStream.option("checkpointLocation", bronze.checkpoint_location)
            .queryName(f"bronze::{bronze.full_name}")
            .foreachBatch(self._make_batch_handler(bronze, accumulator, log))
        )
        return self._apply_trigger(writer, bronze).start()

    def _apply_trigger(self, writer, bronze: BronzeConfig):
        mode = bronze.trigger_mode
        if mode == "available_now":
            return writer.trigger(availableNow=True)
        if mode == "processing_time":
            return writer.trigger(processingTime=bronze.trigger_interval or "1 minute")
        if mode == "continuous":
            return writer.trigger(continuous=bronze.trigger_interval or "1 minute")
        raise IngestionError(f"Unsupported trigger_mode {mode!r} for {bronze.full_name}")

    def _make_batch_handler(
        self, bronze: BronzeConfig, accumulator: _StreamAccumulator, log: FrameworkLogger
    ):
        """Build the foreachBatch callback for this feed."""

        def handle(batch_df: DataFrame, batch_id: int) -> None:
            # The micro-batch is read several times below (count, file stats, write),
            # and re-reading it means re-listing and re-parsing the source files.
            batch_df = batch_df.drop("_metadata").persist()
            try:
                read_count = batch_df.count()
                if read_count == 0:
                    log.debug("empty micro-batch skipped", stream_batch_id=batch_id)
                    return

                file_rows = self._collect_file_stats(bronze, batch_df, batch_id)
                prepared = self._prepare_for_write(bronze, batch_df)

                self._ensure_target(bronze, prepared)
                written = self._write_batch(bronze, prepared, batch_id)

                accumulator.batches += 1
                accumulator.records_read += read_count
                accumulator.records_inserted += written.get("inserted", 0)
                accumulator.records_updated += written.get("updated", 0)
                accumulator.files_processed += len(file_rows)
                accumulator.bytes_processed += sum(r["source_file_size"] or 0 for r in file_rows)
                accumulator.file_rows.extend(file_rows)

                log.info(
                    "micro-batch written",
                    stream_batch_id=batch_id,
                    records_read=read_count,
                    records_inserted=written.get("inserted", 0),
                    records_updated=written.get("updated", 0),
                    files=len(file_rows),
                )
            finally:
                batch_df.unpersist()

        return handle

    def _prepare_for_write(self, bronze: BronzeConfig, df: DataFrame) -> DataFrame:
        """De-duplicate the micro-batch when the feed declares primary keys.

        Landing zones re-deliver rows more often than anyone expects (re-sent files,
        overlapping extracts). Collapsing to the latest row per key here keeps a MERGE
        from failing with "multiple source rows matched", and keeps append-mode bronze
        from accumulating exact duplicates.
        """
        if not bronze.primary_keys:
            return df

        missing = [k for k in bronze.primary_keys if k not in df.columns]
        if missing:
            raise IngestionError(
                f"{bronze.full_name}: primary_keys {missing} are not present in the source "
                f"(available: {sorted(df.columns)})"
            )

        order_column = bronze.sequence_by or ("_source_file_mod_time" if bronze.add_ingestion_metadata else None)
        if order_column and order_column in df.columns:
            window = F.row_number().over(
                Window.partitionBy(*[F.col(k) for k in bronze.primary_keys]).orderBy(
                    F.col(order_column).desc_nulls_last()
                )
            )
            return df.withColumn("_etl_rn", window).filter(F.col("_etl_rn") == 1).drop("_etl_rn")

        if bronze.write_mode == "merge":
            raise IngestionError(
                f"{bronze.full_name}: write_mode='merge' needs an ordering column - set sequence_by, "
                f"or leave add_ingestion_metadata on so _source_file_mod_time can be used"
            )
        return df.dropDuplicates(bronze.primary_keys)

    def _ensure_target(self, bronze: BronzeConfig, df: DataFrame) -> None:
        """Create the bronze table on first arrival, with its physical layout applied.

        The schema is only knowable once a micro-batch has been read, so creation
        happens here rather than in the DDL scripts.
        """
        if table_exists(self.spark, bronze.catalog, bronze.schema, bronze.table):
            return

        empty = df.limit(0)
        writer = empty.write.format("delta")
        if bronze.partition_columns:
            writer = writer.partitionBy(*bronze.partition_columns)
        writer.mode("append").saveAsTable(f"{bronze.catalog}.{bronze.schema}.{bronze.table}")

        apply_liquid_clustering(self.spark, bronze.catalog, bronze.schema, bronze.table, bronze.cluster_by)
        apply_table_properties(
            self.spark, bronze.catalog, bronze.schema, bronze.table, bronze.table_properties
        )
        self.spark.sql(
            f"COMMENT ON TABLE {fq(bronze.catalog, bronze.schema, bronze.table)} IS "
            f"'Bronze table ingested by the metadata driven framework from {bronze.file_location} "
            f"(source system: {bronze.source_system})'"
        )
        self.log.info("bronze target created", target=bronze.full_name, columns=len(df.columns))

    def _write_batch(self, bronze: BronzeConfig, df: DataFrame, batch_id: int) -> Dict[str, int]:
        """Write one micro-batch according to write_mode."""
        target = f"{bronze.catalog}.{bronze.schema}.{bronze.table}"

        if bronze.write_mode == "append":
            # Counted before the write so the row count comes off the cached batch
            # rather than re-reading the target's new files.
            count = df.count()
            # txnAppId + txnVersion make the append idempotent: if the same
            # micro-batch is retried after a driver failure, Delta discards the
            # duplicate commit instead of inserting the rows twice.
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .option("txnAppId", f"etl_bronze_{target}")
                .option("txnVersion", str(batch_id))
                .saveAsTable(target)
            )
            return {"inserted": count, "updated": 0}

        if bronze.write_mode == "overwrite":
            count = df.count()
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(target)
            )
            return {"inserted": count, "updated": 0}

        if bronze.write_mode == "merge":
            return self._merge_batch(bronze, df, target)

        raise IngestionError(f"Unsupported write_mode {bronze.write_mode!r} for {bronze.full_name}")

    def _merge_batch(self, bronze: BronzeConfig, df: DataFrame, target: str) -> Dict[str, int]:
        """Upsert the micro-batch on primary_keys.

        Used for feeds that land full or overlapping extracts of the same keys, where
        appending would grow the table without bound.
        """
        view = f"_etl_bronze_stage_{bronze.table}"
        df.createOrReplaceTempView(view)

        # WITH SCHEMA EVOLUTION lets a new source column be added by the merge itself,
        # so a schema change in the landing file does not fail the ingestion.
        on_clause = " AND ".join(f"t.`{k}` = s.`{k}`" for k in bronze.primary_keys)
        result = self.spark.sql(
            f"""
            MERGE WITH SCHEMA EVOLUTION INTO {target} t
            USING {view} s
              ON {on_clause}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        self.spark.catalog.dropTempView(view)
        metrics = _merge_counts(result)
        return {"inserted": metrics.get("num_inserted_rows", 0), "updated": metrics.get("num_updated_rows", 0)}

    # =============================================================================
    # file lineage
    # =============================================================================
    def _collect_file_stats(
        self, bronze: BronzeConfig, df: DataFrame, batch_id: int
    ) -> List[Dict[str, Any]]:
        """Per-file row counts for bronze_file_audit.

        Collected to the driver deliberately: a micro-batch spans tens of files, not
        millions, and having the rows in hand lets one insert cover the whole batch.
        """
        if not bronze.add_ingestion_metadata or "_source_file" not in df.columns:
            return []

        rescued = bronze.rescued_data_column
        rescued_expr = (
            F.sum(F.when(F.col(rescued).isNotNull(), F.lit(1)).otherwise(F.lit(0)))
            if rescued in df.columns
            else F.lit(None).cast("long")
        )

        grouped = (
            df.groupBy("_source_file", "_source_file_size", "_source_file_mod_time")
            .agg(F.count(F.lit(1)).alias("record_count"), rescued_expr.alias("rescued_record_count"))
            .collect()
        )
        return [
            {
                "batch_id": self.audit.batch_id,
                "stream_batch_id": int(batch_id),
                "catalog_name": bronze.catalog,
                "schema_name": bronze.schema,
                "table_name": bronze.table,
                "source_file_path": row["_source_file"],
                "source_file_name": (row["_source_file"] or "").rsplit("/", 1)[-1],
                "source_file_size": row["_source_file_size"],
                "source_file_mod_time": row["_source_file_mod_time"],
                "record_count": row["record_count"],
                "rescued_record_count": row["rescued_record_count"],
                "ingested_at": None,  # stamped at flush time
                "environment": self.cfg.environment,
            }
            for row in grouped
        ]

    def _flush_file_audit(self, accumulator: _StreamAccumulator, log: FrameworkLogger) -> None:
        if not accumulator.file_rows:
            return
        from ..audit.audit_logger import _FILE_AUDIT_SCHEMA  # local import avoids a cycle

        rows = [tuple(r.get(f.name) for f in _FILE_AUDIT_SCHEMA.fields) for r in accumulator.file_rows]
        df = self.spark.createDataFrame(rows, schema=_FILE_AUDIT_SCHEMA).withColumn(
            "ingested_at", F.current_timestamp()
        )
        self.audit.log_files(df)
        log.debug("file audit written", file_count=len(accumulator.file_rows))

    # =============================================================================
    # validation
    # =============================================================================
    def _validate(self, bronze: BronzeConfig) -> None:
        if bronze.partition_columns and bronze.cluster_by:
            raise IngestionError(
                f"{bronze.full_name}: partition_columns and cluster_by are mutually exclusive - "
                f"a Delta table is either partitioned or liquid clustered"
            )
        if bronze.trigger_mode == "continuous" and bronze.write_mode == "merge":
            raise IngestionError(
                f"{bronze.full_name}: continuous trigger cannot be combined with write_mode='merge'"
            )
        if bronze.load_type == "batch" and bronze.trigger_mode != "available_now":
            self.log.warning(
                "batch feed configured with a non-availableNow trigger - the task will not terminate",
                target=bronze.full_name,
                trigger_mode=bronze.trigger_mode,
            )


def _merge_counts(result: DataFrame) -> Dict[str, int]:
    try:
        row = result.collect()[0].asDict()
        return {k: int(v or 0) for k, v in row.items() if k.startswith("num_")}
    except Exception:  # pragma: no cover
        return {}
