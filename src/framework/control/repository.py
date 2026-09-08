"""Read side of the control tables.

Every lookup filters record_is_active = true AND is_enabled, so a runner can only ever
see the current, switched-on version of a configuration. The DQ lookup performs the
dq_rules_assignment -> dq_rules join in one query rather than two, because it runs
once per silver table and per-table latency adds up across a wide batch.
"""

from __future__ import annotations

from typing import List, Optional

from pyspark.sql import Row, SparkSession

from ..config import FrameworkConfig
from ..exceptions import ControlTableError
from ..models import BronzeConfig, DQRule, GoldConfig, SilverConfig


class ControlRepository:
    def __init__(self, spark: SparkSession, cfg: FrameworkConfig):
        self.spark = spark
        self.cfg = cfg

    # -----------------------------------------------------------------------------
    # Bronze
    # -----------------------------------------------------------------------------
    def get_bronze_config(self, catalog_name: str, schema_name: str, table_name: str) -> BronzeConfig:
        rows = self.spark.sql(
            f"""
            SELECT * FROM {self.cfg.control_table('bronze_control_table')}
            WHERE record_is_active = true
              AND COALESCE(is_enabled, true) = true
              AND lower(target_catalog_name) = lower(:catalog_name)
              AND lower(bronze_schema_name)  = lower(:schema_name)
              AND lower(bronze_table_name)   = lower(:table_name)
            """,
            args={"catalog_name": catalog_name, "schema_name": schema_name, "table_name": table_name},
        ).collect()
        row = self._exactly_one(rows, "bronze_control_table", f"{catalog_name}.{schema_name}.{table_name}")
        return BronzeConfig.from_row(row, self.cfg)

    def list_bronze_configs(
        self, source_system: Optional[str] = None, catalog_name: Optional[str] = None
    ) -> List[BronzeConfig]:
        """All enabled bronze feeds, optionally narrowed - used to generate job tasks."""
        rows = self.spark.sql(
            f"""
            SELECT * FROM {self.cfg.control_table('bronze_control_table')}
            WHERE record_is_active = true
              AND COALESCE(is_enabled, true) = true
              AND (:source_system IS NULL OR lower(source_system) = lower(:source_system))
              AND (:catalog_name  IS NULL OR lower(target_catalog_name) = lower(:catalog_name))
            ORDER BY source_system, bronze_schema_name, bronze_table_name
            """,
            args={"source_system": source_system, "catalog_name": catalog_name},
        ).collect()
        return [BronzeConfig.from_row(r, self.cfg) for r in rows]

    # -----------------------------------------------------------------------------
    # Silver
    # -----------------------------------------------------------------------------
    def get_silver_config(self, catalog_name: str, schema_name: str, table_name: str) -> SilverConfig:
        """Look the row up by its TARGET identity (catalog + silver schema + table)."""
        rows = self.spark.sql(
            f"""
            SELECT * FROM {self.cfg.control_table('silver_control_table')}
            WHERE record_is_active = true
              AND COALESCE(is_enabled, true) = true
              AND lower(target_catalog_name) = lower(:catalog_name)
              AND lower(silver_schema_name)  = lower(:schema_name)
              AND lower(COALESCE(silver_table_name, source_table_name)) = lower(:table_name)
            """,
            args={"catalog_name": catalog_name, "schema_name": schema_name, "table_name": table_name},
        ).collect()
        row = self._exactly_one(rows, "silver_control_table", f"{catalog_name}.{schema_name}.{table_name}")
        return SilverConfig.from_row(row, self.cfg)

    def list_silver_configs(self, catalog_name: Optional[str] = None) -> List[SilverConfig]:
        rows = self.spark.sql(
            f"""
            SELECT * FROM {self.cfg.control_table('silver_control_table')}
            WHERE record_is_active = true
              AND COALESCE(is_enabled, true) = true
              AND (:catalog_name IS NULL OR lower(target_catalog_name) = lower(:catalog_name))
            ORDER BY silver_schema_name, silver_table_name
            """,
            args={"catalog_name": catalog_name},
        ).collect()
        return [SilverConfig.from_row(r, self.cfg) for r in rows]

    # -----------------------------------------------------------------------------
    # Gold
    # -----------------------------------------------------------------------------
    def get_gold_config(self, catalog_name: str, schema_name: str, table_name: str) -> GoldConfig:
        rows = self.spark.sql(
            f"""
            SELECT * FROM {self.cfg.control_table('gold_control_table')}
            WHERE record_is_active = true
              AND COALESCE(is_enabled, true) = true
              AND lower(target_catalog_name) = lower(:catalog_name)
              AND lower(target_schema_name)  = lower(:schema_name)
              AND lower(table_name)          = lower(:table_name)
            """,
            args={"catalog_name": catalog_name, "schema_name": schema_name, "table_name": table_name},
        ).collect()
        row = self._exactly_one(rows, "gold_control_table", f"{catalog_name}.{schema_name}.{table_name}")
        return GoldConfig.from_row(row, self.cfg)

    def list_gold_configs(self, catalog_name: Optional[str] = None) -> List[GoldConfig]:
        """Enabled gold objects, dimensions before facts.

        Ordering dimensions first gives a workable default even when depends_on is not
        populated, because facts join to dimensions and never the reverse.
        """
        rows = self.spark.sql(
            f"""
            SELECT * FROM {self.cfg.control_table('gold_control_table')}
            WHERE record_is_active = true
              AND COALESCE(is_enabled, true) = true
              AND (:catalog_name IS NULL OR lower(target_catalog_name) = lower(:catalog_name))
            ORDER BY CASE lower(COALESCE(object_type, 'zz'))
                       WHEN 'dimension' THEN 1
                       WHEN 'bridge'    THEN 2
                       WHEN 'fact'      THEN 3
                       WHEN 'aggregate' THEN 4
                       ELSE 5 END,
                     target_schema_name, table_name
            """,
            args={"catalog_name": catalog_name},
        ).collect()
        return [GoldConfig.from_row(r, self.cfg) for r in rows]

    # -----------------------------------------------------------------------------
    # Data quality
    # -----------------------------------------------------------------------------
    def get_dq_rules(self, catalog_name: str, schema_name: str, table_name: str) -> List[DQRule]:
        """Active rule assignments for one table, already joined to the rule registry.

        Table level rules (column_name = '__table__') sort last so that column checks,
        which are cheaper and catch the common problems, are reported first.
        """
        rows = self.spark.sql(
            f"""
            SELECT
                a.column_name,
                a.severity,
                a.rule_parameters,
                a.filter_condition,
                r.rule_id,
                r.rule_name,
                r.rule_type,
                r.rule,
                r.rule_parameters AS rule_parameters_default,
                r.dq_dimension,
                r.default_severity,
                r.description
            FROM {self.cfg.control_table('dq_rules_assignment')} a
            JOIN {self.cfg.control_table('dq_rules')} r
              ON r.rule_id = a.rule_id
             AND r.record_is_active = true
             AND COALESCE(r.is_enabled, true) = true
            WHERE a.record_is_active = true
              AND COALESCE(a.is_enabled, true) = true
              AND lower(a.catalog_name) = lower(:catalog_name)
              AND lower(a.schema_name)  = lower(:schema_name)
              AND lower(a.table_name)   = lower(:table_name)
            ORDER BY CASE WHEN a.column_name = '__table__' THEN 2 ELSE 1 END,
                     a.column_name, r.rule_id
            """,
            args={"catalog_name": catalog_name, "schema_name": schema_name, "table_name": table_name},
        ).collect()
        return [DQRule.from_row(r) for r in rows]

    # -----------------------------------------------------------------------------
    def _exactly_one(self, rows: List[Row], table: str, key: str) -> Row:
        if not rows:
            raise ControlTableError(
                f"No active, enabled row in {table} for {key}. "
                f"Load the metadata YAML first (notebooks/01_load_control_tables.py) "
                f"and confirm is_enabled is not false."
            )
        if len(rows) > 1:
            ids = [r["id"] for r in rows]
            raise ControlTableError(
                f"{len(rows)} active rows in {table} for {key} (ids={ids}). "
                f"The SCD2 close step did not run - investigate before ingesting."
            )
        return rows[0]
