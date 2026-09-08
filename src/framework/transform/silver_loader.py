"""Bronze -> silver curation: DQ checks plus SCD1/SCD2 loading.

One SilverConfig row produces one silver table. The reading strategy is metadata
driven, because the right choice differs per table:

    batch  - full or watermark-bounded read of the bronze table. Simplest, and the
             only option when the transformation needs to see the whole table.
    stream - readStream over the bronze Delta table with a checkpoint; each
             micro-batch is DQ checked and merged. Incremental without a watermark
             column, at the cost of a checkpoint to manage.
    cdf    - Change Data Feed read, so bronze updates and deletes propagate rather
             than only appends. Requires delta.enableChangeDataFeed on bronze.

All three converge on _process_batch, so DQ behaviour and load semantics cannot drift
between them.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..audit import AuditLogger, DQRunMetrics, TaskMetrics
from ..config import FrameworkConfig
from ..control import ControlRepository
from ..dq import DQEngine
from ..dq.rule_engine import DQ_COLUMNS
from ..logging_utils import FrameworkLogger
from ..models import SilverConfig
from ..spark_utils import ensure_schema, fq, row_count, table_exists
from .scd import DeltaWriter

# Change Data Feed adds these; they describe the change, not the entity.
_CDF_COLUMNS = ("_change_type", "_commit_version", "_commit_timestamp")


class SilverLoader:
    def __init__(
        self,
        spark: SparkSession,
        cfg: FrameworkConfig,
        repo: ControlRepository,
        audit: AuditLogger,
        logger: Optional[FrameworkLogger] = None,
    ):
        self.spark = spark
        self.cfg = cfg
        self.repo = repo
        self.audit = audit
        self.log = logger or FrameworkLogger({"component": "silver_loader"}, cfg.log_level)
        self.writer = DeltaWriter(spark, self.log)

    # =============================================================================
    # public API
    # =============================================================================
    def load(self, silver: SilverConfig, await_timeout_seconds: Optional[int] = None) -> TaskMetrics:
        """Load one silver table and return its metrics."""
        ensure_schema(self.spark, silver.target_catalog, silver.target_schema)
        log = self.log.child(target=silver.target_full_name, source=silver.source_full_name)

        if not table_exists(self.spark, silver.source_catalog, silver.source_schema, silver.source_table):
            raise ValueError(
                f"{silver.target_full_name}: source table {silver.source_full_name} does not exist - "
                f"run the bronze task for it first"
            )

        rules = self.repo.get_dq_rules(
            silver.source_catalog, silver.source_schema, silver.source_table
        ) if silver.dq_enabled else []

        log.info(
            "starting silver load",
            load_type=silver.load_type,
            read_mode=silver.read_mode,
            dq_rule_count=len(rules),
            business_keys=silver.business_keys,
        )

        if silver.read_mode == "batch":
            return self._load_batch(silver, rules, log)
        return self._load_streaming(silver, rules, log, await_timeout_seconds)

    # =============================================================================
    # batch path
    # =============================================================================
    def _load_batch(self, silver: SilverConfig, rules: List, log: FrameworkLogger) -> TaskMetrics:
        source = self.spark.table(
            f"{silver.source_catalog}.{silver.source_schema}.{silver.source_table}"
        )
        source = self._apply_watermark(source, silver, log)
        return self._process_batch(silver, source, rules, log)

    def _apply_watermark(self, df: DataFrame, silver: SilverConfig, log: FrameworkLogger) -> DataFrame:
        """Restrict a batch read to rows newer than what the target already holds.

        This keeps a batch-mode silver load incremental without a checkpoint. The
        high-water mark is read from the target rather than stored separately, so it
        cannot drift out of step with the data.
        """
        if not silver.watermark_column:
            return df
        if not table_exists(self.spark, silver.target_catalog, silver.target_schema, silver.target_table):
            return df
        if silver.watermark_column not in df.columns:
            log.warning(
                "watermark column absent from the source - reading in full",
                watermark_column=silver.watermark_column,
            )
            return df

        target = fq(silver.target_catalog, silver.target_schema, silver.target_table)
        target_columns = {
            f.name for f in self.spark.table(
                f"{silver.target_catalog}.{silver.target_schema}.{silver.target_table}"
            ).schema.fields
        }
        if silver.watermark_column not in target_columns:
            log.warning(
                "watermark column absent from the target - reading in full",
                watermark_column=silver.watermark_column,
            )
            return df

        high_water = self.spark.sql(
            f"SELECT MAX(`{silver.watermark_column}`) AS hwm FROM {target}"
        ).collect()[0]["hwm"]
        if high_water is None:
            return df

        log.info("applying watermark", watermark_column=silver.watermark_column, high_water_mark=str(high_water))
        return df.filter(F.col(silver.watermark_column) > F.lit(high_water))

    # =============================================================================
    # streaming path
    # =============================================================================
    def _load_streaming(
        self,
        silver: SilverConfig,
        rules: List,
        log: FrameworkLogger,
        await_timeout_seconds: Optional[int],
    ) -> TaskMetrics:
        """Incremental read of bronze, DQ checked and merged per micro-batch."""
        reader = self.spark.readStream.format("delta")
        if silver.read_mode == "cdf":
            reader = reader.option("readChangeFeed", "true")
        stream = reader.table(f"{silver.source_catalog}.{silver.source_schema}.{silver.source_table}")

        accumulated = TaskMetrics()

        def handle(batch_df: DataFrame, batch_id: int) -> None:
            nonlocal accumulated
            if silver.read_mode == "cdf":
                batch_df = self._prepare_cdf(batch_df)
            if batch_df.limit(1).count() == 0:
                log.debug("empty micro-batch skipped", stream_batch_id=batch_id)
                return
            batch_metrics = self._process_batch(
                silver, batch_df, rules, log.child(stream_batch_id=batch_id), refresh_target_count=False
            )
            accumulated = accumulated.merge(batch_metrics)

        query = (
            stream.writeStream.option("checkpointLocation", silver.checkpoint_location)
            .queryName(f"silver::{silver.target_full_name}")
            .foreachBatch(handle)
            .trigger(availableNow=True)
            .start()
        )
        if await_timeout_seconds:
            query.awaitTermination(await_timeout_seconds)
            if query.isActive:
                log.info("await timeout reached, stopping the silver stream gracefully")
                query.stop()
                query.awaitTermination()
        else:
            query.awaitTermination()

        if query.exception() is not None:
            raise RuntimeError(
                f"silver stream for {silver.target_full_name} terminated with an error: {query.exception()}"
            )

        accumulated.target_row_count = row_count(
            self.spark, silver.target_catalog, silver.target_schema, silver.target_table
        )
        return accumulated

    def _prepare_cdf(self, df: DataFrame) -> DataFrame:
        """Keep the post-image of inserts and updates, and drop the CDF metadata.

        Deletes are deliberately not propagated automatically: whether a bronze delete
        should remove a silver row is a modelling decision, so it belongs in metadata
        (load_type = delete_insert) rather than in an implicit default here.
        """
        return df.filter(F.col("_change_type").isin("insert", "update_postimage")).drop(*_CDF_COLUMNS)

    # =============================================================================
    # shared processing
    # =============================================================================
    def _process_batch(
        self,
        silver: SilverConfig,
        source: DataFrame,
        rules: List,
        log: FrameworkLogger,
        refresh_target_count: bool = True,
    ) -> TaskMetrics:
        """Shape, DQ check, quarantine and load one batch of rows."""
        started_at = datetime.now(timezone.utc)
        shaped = self._shape(source, silver)

        engine = DQEngine(self.audit.batch_id, log)
        result = engine.apply(
            shaped,
            rules,
            table_label=silver.target_full_name,
            failure_threshold_pct=silver.dq_failure_threshold_pct,
        )

        quarantined_count = 0
        if result.quarantined is not None and silver.quarantine_enabled:
            quarantined_count = self._write_quarantine(silver, result.quarantined, log)
        elif result.quarantined is not None:
            quarantined_count = result.metrics.quarantine_count
            log.warning(
                "rows failed DQ but quarantine is disabled - they are dropped without a copy",
                quarantine_count=quarantined_count,
            )

        to_load = result.valid.drop(*[c for c in DQ_COLUMNS if c in result.valid.columns])
        write_result = self.writer.write(
            to_load,
            catalog=silver.target_catalog,
            schema=silver.target_schema,
            table=silver.target_table,
            load_type=silver.load_type,
            business_keys=silver.business_keys,
            sequence_by=silver.sequence_by,
            hash_column_list=silver.scd2_hash_columns,
            partition_columns=silver.partition_columns,
            cluster_by=silver.cluster_by,
            table_properties=silver.table_properties,
            deduplicate=silver.deduplicate,
        )

        target_count = (
            row_count(self.spark, silver.target_catalog, silver.target_schema, silver.target_table)
            if refresh_target_count
            else None
        )
        result.metrics.target_rec_count = target_count or 0
        self._audit_dq(silver, result.metrics, started_at)

        log.info(
            "silver batch loaded",
            src_rec_count=result.metrics.src_rec_count,
            inserted=write_result.inserted,
            updated=write_result.updated,
            quarantined=quarantined_count,
        )
        return TaskMetrics(
            records_read=result.metrics.src_rec_count,
            records_inserted=write_result.inserted,
            records_updated=write_result.updated,
            records_deleted=write_result.deleted,
            records_rejected=quarantined_count,
            target_row_count=target_count,
        )

    def _shape(self, df: DataFrame, silver: SilverConfig) -> DataFrame:
        """Apply the metadata-driven projection, filter and column removal.

        This is deliberately limited to declarative reshaping. Anything needing real
        logic belongs in a gold transformation module, so that silver stays a
        predictable, cleansed mirror of bronze.
        """
        if silver.filter_condition:
            df = df.filter(F.expr(silver.filter_condition))
        if silver.select_expressions:
            df = df.selectExpr(*silver.select_expressions)
        if silver.drop_columns:
            present = [c for c in silver.drop_columns if c in df.columns]
            if present:
                df = df.drop(*present)
        return df

    def _write_quarantine(self, silver: SilverConfig, quarantined: DataFrame, log: FrameworkLogger) -> int:
        """Append failing rows, with their verdict, to the quarantine table.

        The quarantine table is append-only and carries _dq_failed_rules, so a
        data steward can see which rule rejected each row without re-running anything.
        """
        target = f"{silver.target_catalog}.{silver.target_schema}.{silver.quarantine_table}"
        count = quarantined.count()
        if count == 0:
            return 0
        (
            quarantined.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(target)
        )
        log.info("rows quarantined", quarantine_table=target, quarantine_count=count)
        return count

    def _audit_dq(self, silver: SilverConfig, metrics: DQRunMetrics, started_at: datetime) -> None:
        self.audit.log_dq_run(
            metrics=metrics,
            source_catalog=silver.source_catalog,
            source_schema=silver.source_schema,
            table_name=silver.source_table,
            target_catalog=silver.target_catalog,
            target_schema=silver.target_schema,
            target_table=silver.target_table,
            quarantine_table=(
                f"{silver.target_catalog}.{silver.target_schema}.{silver.quarantine_table}"
                if silver.quarantine_enabled
                else None
            ),
            started_at=started_at,
        )
