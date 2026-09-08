"""Gold layer runner: dispatches a transformation, then loads its output.

The design has the gold control table naming a transformation notebook. This adds two
further options, because a notebook is the hardest of the three to test:

    module   - an importable python module exposing transform(spark, ctx) -> DataFrame.
               Runs in-process, so it can be unit tested with a local session and
               composed by other modules. The recommended default.
    sql      - a .sql file holding a single SELECT. The right choice when the
               transformation genuinely is one query, and it stays readable to
               analysts who do not write python.
    notebook - dbutils.notebook.run, kept for the original design and for
               transformations that must stay in notebook form.

Whichever route produces the DataFrame, the WRITE is always the framework's job via
DeltaWriter, so load semantics (SCD2, overwrite, merge) stay consistent and every
gold table gets the same audit treatment. A notebook is the exception: it writes its
own target, so the framework only records what it produced.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from ..audit import AuditLogger, TaskMetrics
from ..config import FrameworkConfig
from ..exceptions import TransformationError
from ..logging_utils import FrameworkLogger
from ..models import GoldConfig
from ..spark_utils import ensure_schema, row_count, table_exists
from ..sql_utils import render_placeholders, split_sql_statements
from ..transform.scd import DeltaWriter


@dataclass
class TransformContext:
    """Everything a transformation is allowed to depend on.

    Passing a context object rather than letting transformations read the control
    tables themselves keeps them pure functions of their inputs - which is what makes
    them unit testable.
    """

    spark: SparkSession
    cfg: FrameworkConfig
    batch_id: str
    target_catalog: str
    target_schema: str
    target_table: str
    parameters: Dict[str, str] = field(default_factory=dict)
    logger: Optional[FrameworkLogger] = None

    # ---- convenience accessors used by transformation modules --------------------
    def silver(self, table: str, schema: Optional[str] = None) -> DataFrame:
        """Read a silver table by name, using the environment's silver catalog."""
        catalog = self.cfg.resolve_catalog("silver")
        schema = schema or self.cfg.default_for("silver_schema", "silver")
        return self.spark.table(f"{catalog}.{schema}.{table}")

    def gold(self, table: str, schema: Optional[str] = None) -> DataFrame:
        """Read an already-built gold table, e.g. a dimension a fact must join to."""
        return self.spark.table(f"{self.target_catalog}.{schema or self.target_schema}.{table}")

    def param(self, key: str, default: Any = None) -> Any:
        return self.parameters.get(key, default)

    def int_param(self, key: str, default: int) -> int:
        value = self.parameters.get(key)
        return int(value) if value is not None else default

    @property
    def target_exists(self) -> bool:
        return table_exists(self.spark, self.target_catalog, self.target_schema, self.target_table)


class GoldRunner:
    def __init__(
        self,
        spark: SparkSession,
        cfg: FrameworkConfig,
        audit: AuditLogger,
        logger: Optional[FrameworkLogger] = None,
        transformation_root: Optional[str] = None,
    ):
        self.spark = spark
        self.cfg = cfg
        self.audit = audit
        self.log = logger or FrameworkLogger({"component": "gold_runner"}, cfg.log_level)
        self.writer = DeltaWriter(spark, self.log)
        self.transformation_root = (
            Path(transformation_root)
            if transformation_root
            else Path(__file__).resolve().parents[3] / "notebooks" / "transformations" / "gold"
        )

    # =============================================================================
    # public API
    # =============================================================================
    def run(self, gold: GoldConfig) -> TaskMetrics:
        """Build and load one gold table."""
        ensure_schema(self.spark, gold.target_catalog, gold.target_schema)
        log = self.log.child(target=gold.full_name, object_type=gold.object_type or "unknown")

        self._check_dependencies(gold, log)

        ctx = TransformContext(
            spark=self.spark,
            cfg=self.cfg,
            batch_id=self.audit.batch_id,
            target_catalog=gold.target_catalog,
            target_schema=gold.target_schema,
            target_table=gold.table,
            parameters=gold.parameters,
            logger=log,
        )

        log.info(
            "running gold transformation",
            transformation_type=gold.transformation_type,
            load_type=gold.load_type,
            artefact=gold.module_name or gold.sql_file_name or gold.notebook_name,
        )

        if gold.transformation_type == "notebook":
            return self._run_notebook(gold, log)

        df = (
            self._run_module(gold, ctx)
            if gold.transformation_type == "module"
            else self._run_sql(gold, ctx)
        )
        if df is None:
            raise TransformationError(
                f"{gold.full_name}: the transformation returned None - it must return a DataFrame"
            )

        records_read = df.count()
        write_result = self.writer.write(
            df,
            catalog=gold.target_catalog,
            schema=gold.target_schema,
            table=gold.table,
            load_type=gold.load_type,
            business_keys=gold.business_keys,
            sequence_by=gold.sequence_by,
            hash_column_list=gold.scd2_hash_columns,
            partition_columns=gold.partition_columns,
            cluster_by=gold.cluster_by,
            table_properties=gold.table_properties,
        )

        target_count = row_count(self.spark, gold.target_catalog, gold.target_schema, gold.table)
        log.info(
            "gold table loaded",
            records_read=records_read,
            inserted=write_result.inserted,
            updated=write_result.updated,
            target_row_count=target_count,
        )
        return TaskMetrics(
            records_read=records_read,
            records_inserted=write_result.inserted,
            records_updated=write_result.updated,
            records_deleted=write_result.deleted,
            target_row_count=target_count,
        )

    # =============================================================================
    # dispatch
    # =============================================================================
    def _run_module(self, gold: GoldConfig, ctx: TransformContext) -> DataFrame:
        """Import the module and call its transform(spark, ctx)."""
        module_name = gold.module_name or ""
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            raise TransformationError(
                f"{gold.full_name}: cannot import module {module_name!r}. "
                f"Confirm it is on sys.path (add the repo's notebooks/ directory) and the name is correct."
            ) from exc

        # Reloaded so an edit during interactive development takes effect without
        # detaching the notebook.
        module = importlib.reload(module)

        transform = getattr(module, "transform", None)
        if not callable(transform):
            raise TransformationError(
                f"{gold.full_name}: module {module_name!r} must define transform(spark, ctx) -> DataFrame"
            )
        return transform(self.spark, ctx)

    def _run_sql(self, gold: GoldConfig, ctx: TransformContext) -> DataFrame:
        """Read the .sql file, substitute parameters, and run it.

        Placeholders available to the SQL: ${silver_catalog}, ${gold_catalog},
        ${target_schema}, ${target_table}, ${batch_id}, plus every key in the control
        row's `parameters` map.
        """
        path = self.transformation_root / (gold.sql_file_name or "")
        if not path.exists():
            raise TransformationError(
                f"{gold.full_name}: SQL file not found at {path}. "
                f"Place it under {self.transformation_root}."
            )
        substitutions = {
            "silver_catalog": self.cfg.resolve_catalog("silver"),
            "gold_catalog": gold.target_catalog,
            "target_catalog": gold.target_catalog,
            "target_schema": gold.target_schema,
            "target_table": gold.table,
            "batch_id": ctx.batch_id,
            **gold.parameters,
        }
        try:
            script = render_placeholders(path.read_text(encoding="utf-8"), substitutions)
        except ValueError as exc:
            raise TransformationError(
                f"{gold.full_name}: {exc} in {path.name}. Add it to the control row's parameters map."
            ) from exc

        # A transformation must produce exactly one result set for the framework to
        # write; several statements would leave it ambiguous which one is the target.
        statements = split_sql_statements(script)
        if len(statements) != 1:
            raise TransformationError(
                f"{gold.full_name}: {path.name} must hold exactly one SELECT statement, "
                f"found {len(statements)}. Move multi-step logic into a transformation module."
            )
        return self.spark.sql(statements[0])

    def _run_notebook(self, gold: GoldConfig, log: FrameworkLogger) -> TaskMetrics:
        """Run a transformation notebook via dbutils.

        The notebook owns its own write, so the framework can only report the target's
        resulting row count - which is why module/sql are preferred.
        """
        from ..spark_utils import get_dbutils

        dbutils = get_dbutils(self.spark)
        if dbutils is None:
            raise TransformationError(
                f"{gold.full_name}: transformation_type='notebook' needs dbutils, which is not available "
                f"in this context. Use transformation_type='module' for a testable alternative."
            )
        timeout = int(gold.parameters.get("notebook_timeout_seconds", 3600))
        arguments = {
            "target_catalog": gold.target_catalog,
            "target_schema": gold.target_schema,
            "target_table": gold.table,
            "batch_id": self.audit.batch_id,
            "load_type": gold.load_type,
            **gold.parameters,
        }
        before = row_count(self.spark, gold.target_catalog, gold.target_schema, gold.table)
        output = dbutils.notebook.run(gold.notebook_name, timeout, arguments)
        after = row_count(self.spark, gold.target_catalog, gold.target_schema, gold.table)
        log.info("transformation notebook finished", notebook=gold.notebook_name, notebook_output=output)
        return TaskMetrics(records_inserted=max(after - before, 0), target_row_count=after)

    # =============================================================================
    # dependencies
    # =============================================================================
    def _check_dependencies(self, gold: GoldConfig, log: FrameworkLogger) -> None:
        """Verify declared upstream tables exist before running.

        Workflow task dependencies should already guarantee this; the check turns a
        confusing mid-transformation "table not found" into a clear message naming
        the missing dependency.
        """
        missing: List[str] = []
        for dependency in gold.depends_on:
            parts = dependency.split(".")
            if len(parts) == 3:
                catalog, schema, table = parts
                catalog = self.cfg.resolve_catalog(catalog)
            elif len(parts) == 2:
                catalog, (schema, table) = gold.target_catalog, parts
            else:
                catalog, schema, table = gold.target_catalog, gold.target_schema, parts[0]
            if not table_exists(self.spark, catalog, schema, table):
                missing.append(f"{catalog}.{schema}.{table}")
        if missing:
            raise TransformationError(
                f"{gold.full_name}: declared dependencies do not exist yet: {missing}. "
                f"Check the workflow task ordering."
            )
        if gold.depends_on:
            log.debug("dependencies satisfied", depends_on=gold.depends_on)
