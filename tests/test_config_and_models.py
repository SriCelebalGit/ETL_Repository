"""Tests for configuration resolution and control row validation.

These are the checks worth having in CI: a bad control row should fail loudly at
construction, because the alternative is a confusing failure deep inside a streaming
foreachBatch at 2am.
"""

from __future__ import annotations

import pytest

from framework.config import FrameworkConfig
from framework.exceptions import ConfigurationError, ControlTableError
from framework.models import BronzeConfig, DQRule, GoldConfig, SilverConfig
from framework.spark_utils import normalise_column_name, sha256_of_payload


# =====================================================================================
# FrameworkConfig
# =====================================================================================
def test_env_token_is_expanded():
    cfg = FrameworkConfig.from_dict(
        {
            "framework_catalog": "fw_${env}",
            "catalogs": {"bronze": "b_${env}", "silver": "s_${env}", "gold": "g_${env}"},
            "checkpoint_root": "/Volumes/fw_${env}/etl_volumes/checkpoints",
        },
        environment="tst",
    )
    assert cfg.framework_catalog == "fw_tst"
    assert cfg.catalogs["bronze"] == "b_tst"
    assert cfg.checkpoint_root == "/Volumes/fw_tst/etl_volumes/checkpoints"


def test_external_checkpoint_root_is_rejected():
    """Free Edition has no storage credential, so abfss:// can only fail later, mid-run."""
    with pytest.raises(ConfigurationError, match="external path"):
        FrameworkConfig.from_dict(
            {
                "framework_catalog": "fw",
                "catalogs": {"bronze": "b", "silver": "s", "gold": "g"},
                "checkpoint_root": "abfss://c@a.dfs.core.windows.net/dev",
            },
            environment="dev",
        )


def test_volume_checkpoint_root_is_accepted():
    cfg = FrameworkConfig.from_dict(
        {
            "framework_catalog": "workspace",
            "catalogs": {"bronze": "workspace", "silver": "workspace", "gold": "workspace"},
            "checkpoint_root": "/Volumes/workspace/etl_volumes/checkpoints",
        },
        environment="free",
    )
    # All three layer tokens legitimately resolve to one catalog on Free Edition.
    assert cfg.resolve_catalog("bronze") == cfg.resolve_catalog("gold") == "workspace"


def test_free_edition_block_is_surfaced():
    """notebooks/00_setup_framework.py reads this to create the Volumes and schemas."""
    cfg = FrameworkConfig.from_dict(
        {
            "framework_catalog": "workspace",
            "catalogs": {"bronze": "workspace", "silver": "workspace", "gold": "workspace"},
            "checkpoint_root": "/Volumes/workspace/etl_volumes/checkpoints",
            "free_edition": {"volumes_schema": "etl_volumes", "layer_schemas": ["bronze_crm"]},
        },
        environment="free",
    )
    assert cfg.free_edition["volumes_schema"] == "etl_volumes"
    assert cfg.free_edition["layer_schemas"] == ["bronze_crm"]


def test_missing_checkpoint_root_is_rejected():
    with pytest.raises(ConfigurationError, match="checkpoint_root"):
        FrameworkConfig.from_dict(
            {"framework_catalog": "fw", "catalogs": {"bronze": "b", "silver": "s", "gold": "g"}},
            environment="dev",
        )


def test_missing_layer_catalog_is_rejected():
    with pytest.raises(ConfigurationError, match="catalogs.gold"):
        FrameworkConfig.from_dict(
            {
                "framework_catalog": "fw",
                "catalogs": {"bronze": "b", "silver": "s"},
                "checkpoint_root": "/tmp/c",
            },
            environment="dev",
        )


def test_logical_catalog_resolves_and_physical_passes_through(framework_config):
    assert framework_config.resolve_catalog("bronze") == "bronze_test"
    assert framework_config.resolve_catalog("BRONZE") == "bronze_test"
    # A row naming a physical catalog directly is honoured rather than rewritten.
    assert framework_config.resolve_catalog("some_other_catalog") == "some_other_catalog"


def test_checkpoint_paths_are_deterministic_and_distinct(framework_config):
    schema_path = framework_config.checkpoint_path("bronze", "c", "s", "t", "schema")
    checkpoint_path = framework_config.checkpoint_path("bronze", "c", "s", "t", "checkpoint")
    assert schema_path != checkpoint_path
    assert schema_path == framework_config.checkpoint_path("bronze", "c", "s", "t", "schema")
    assert "/bronze/c/s/t/" in schema_path


# =====================================================================================
# BronzeConfig
# =====================================================================================
def _bronze_row(**overrides):
    row = {
        "id": 1,
        "source_system": "crm",
        "source_file_type": "csv",
        "file_location": "/Volumes/workspace/etl_volumes/landing/crm/customer/",
        "target_catalog_name": "bronze",
        "bronze_schema_name": "crm",
        "bronze_table_name": "customer",
        "load_type": "batch",
        "config_file_name": "crm.yml",
    }
    row.update(overrides)
    return row


def test_bronze_defaults_are_applied(framework_config):
    bronze = BronzeConfig.from_row(_bronze_row(), framework_config)
    assert bronze.catalog == "bronze_test"
    assert bronze.full_name == "bronze_test.crm.customer"
    # batch feeds must terminate, so availableNow is the only sensible default
    assert bronze.trigger_mode == "available_now"
    assert bronze.write_mode == "append"
    assert bronze.schema_evolution_mode == "addNewColumns"
    assert bronze.rescued_data_column == "_rescued_data"
    assert bronze.add_ingestion_metadata is True
    assert bronze.schema_location.endswith("_schema")
    assert bronze.checkpoint_location.endswith("_checkpoint")


def test_stream_feed_defaults_to_processing_time(framework_config):
    bronze = BronzeConfig.from_row(_bronze_row(load_type="stream"), framework_config)
    assert bronze.trigger_mode == "processing_time"


def test_missing_catalog_is_rejected(framework_config):
    row = _bronze_row()
    row["target_catalog_name"] = None
    with pytest.raises(ControlTableError, match="target_catalog_name"):
        BronzeConfig.from_row(row, framework_config)


def test_merge_without_primary_keys_is_rejected(framework_config):
    with pytest.raises(ControlTableError, match="primary_keys"):
        BronzeConfig.from_row(_bronze_row(write_mode="merge"), framework_config)


def test_invalid_load_type_is_rejected(framework_config):
    with pytest.raises(ControlTableError, match="load_type"):
        BronzeConfig.from_row(_bronze_row(load_type="micro_batch"), framework_config)


def test_comma_separated_lists_are_accepted(framework_config):
    bronze = BronzeConfig.from_row(
        _bronze_row(write_mode="merge", primary_keys="order_id, line_id"), framework_config
    )
    assert bronze.primary_keys == ["order_id", "line_id"]


# =====================================================================================
# SilverConfig
# =====================================================================================
def _silver_row(**overrides):
    row = {
        "id": 7,
        "source_catalog_name": "bronze",
        "source_schema_name": "crm",
        "source_table_name": "customer",
        "target_catalog_name": "silver",
        "silver_schema_name": "crm",
        "load_type": "scd2",
        "business_keys": ["customer_id"],
        "config_file_name": "crm.yml",
    }
    row.update(overrides)
    return row


def test_silver_table_name_defaults_to_source(framework_config):
    silver = SilverConfig.from_row(_silver_row(), framework_config)
    assert silver.target_table == "customer"
    assert silver.quarantine_table == "customer_quarantine"
    assert silver.source_full_name == "bronze_test.crm.customer"
    assert silver.target_full_name == "silver_test.crm.customer"


def test_scd2_without_business_keys_is_rejected(framework_config):
    with pytest.raises(ControlTableError, match="business_keys"):
        SilverConfig.from_row(_silver_row(business_keys=None), framework_config)


def test_append_without_business_keys_is_allowed(framework_config):
    silver = SilverConfig.from_row(
        _silver_row(load_type="append", business_keys=[]), framework_config
    )
    assert silver.business_keys == []


def test_unknown_read_mode_is_rejected(framework_config):
    with pytest.raises(ControlTableError, match="read_mode"):
        SilverConfig.from_row(_silver_row(read_mode="trickle"), framework_config)


# =====================================================================================
# GoldConfig
# =====================================================================================
def _gold_row(**overrides):
    row = {
        "id": 3,
        "target_catalog_name": "gold",
        "target_schema_name": "sales",
        "table_name": "dim_customer",
        "object_type": "dimension",
        "transformation_type": "module",
        "module_name": "transformations.gold.dim_customer",
        "load_type": "scd2",
        "business_keys": ["customer_id"],
        "config_file_name": "sales_mart.yml",
    }
    row.update(overrides)
    return row


def test_gold_module_config_resolves(framework_config):
    gold = GoldConfig.from_row(_gold_row(), framework_config)
    assert gold.full_name == "gold_test.sales.dim_customer"
    assert gold.module_name == "transformations.gold.dim_customer"


def test_gold_transformation_type_requires_its_artefact(framework_config):
    row = _gold_row(transformation_type="sql", module_name=None, sql_file_name=None)
    with pytest.raises(ControlTableError, match="sql"):
        GoldConfig.from_row(row, framework_config)


# =====================================================================================
# DQRule
# =====================================================================================
def test_assignment_parameters_override_registry_defaults():
    rule = DQRule.from_row(
        {
            "rule_id": "DQ_RANGE",
            "rule_type": "sql",
            "rule": "{column} BETWEEN {min} AND {max}",
            "column_name": "credit_limit",
            "severity": "drop",
            "rule_parameters_default": {"min": "0", "max": "100"},
            "rule_parameters": {"max": "5000"},
        }
    )
    assert rule.parameters == {"min": "0", "max": "5000"}


def test_severity_falls_back_to_the_registry_default():
    rule = DQRule.from_row(
        {
            "rule_id": "DQ_NOT_NULL",
            "rule_type": "sql",
            "rule": "{column} IS NOT NULL",
            "column_name": "email",
            "severity": None,
            "default_severity": "warning",
        }
    )
    assert rule.severity == "warning"


def test_unknown_severity_is_rejected():
    with pytest.raises(ControlTableError, match="severity"):
        DQRule.from_row(
            {
                "rule_id": "DQ_NOT_NULL",
                "rule_type": "sql",
                "rule": "{column} IS NOT NULL",
                "column_name": "email",
                "severity": "block",
            }
        )


def test_table_level_rules_are_flagged():
    rule = DQRule.from_row(
        {
            "rule_id": "DQ_ROW",
            "rule_type": "sql",
            "rule": "a > b",
            "column_name": "__table__",
            "severity": "drop",
        }
    )
    assert rule.is_table_level is True


# =====================================================================================
# helpers
# =====================================================================================
@pytest.mark.parametrize(
    ("original", "expected"),
    [
        ("CustomerID", "customer_id"),
        ("customer name", "customer_name"),
        ("Order.Amount", "order_amount"),
        ("  spaced  out  ", "spaced_out"),
        ("weird%%chars!!", "weird_chars"),
        ("already_snake", "already_snake"),
        ("A-B/C", "a_b_c"),
    ],
)
def test_column_name_normalisation(original, expected):
    assert normalise_column_name(original) == expected


def test_payload_hash_ignores_key_order():
    # Without this, reordering a YAML file would close and reopen every control row.
    assert sha256_of_payload({"a": 1, "b": 2}) == sha256_of_payload({"b": 2, "a": 1})
    assert sha256_of_payload({"a": 1}) != sha256_of_payload({"a": 2})
