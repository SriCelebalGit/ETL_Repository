"""Tests for YAML metadata parsing and validation.

The point of validating here is that CI catches a typo, rather than the ingestion run
at 2am discovering it. These tests need no Spark session - only the parsing and
validation paths are exercised.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from framework.control.metadata_loader import (
    BRONZE_SPEC,
    DQ_ASSIGNMENT_SPEC,
    GOLD_SPEC,
    SILVER_SPEC,
    MetadataLoader,
    _coerce,
)
from framework.exceptions import MetadataValidationError
from pyspark.sql.types import ArrayType, BooleanType, IntegerType, MapType, StringType

REPO_ROOT = Path(__file__).resolve().parents[1]


class _LoaderUnderTest(MetadataLoader):
    """MetadataLoader with the Spark dependencies stubbed out.

    Only _read_yaml_dir and _validate are exercised, so a real session is unnecessary
    and would triple the test runtime.
    """

    def __init__(self, metadata_dir):
        self.spark = None
        self.cfg = None
        self.metadata_dir = Path(metadata_dir)

        class _Log:
            def info(self, *a, **k):
                pass

            def warning(self, *a, **k):
                pass

            def debug(self, *a, **k):
                pass

        self.log = _Log()

    def _validate_cross_references(self, spec, records):
        # Cross-table checks need the control tables; covered separately.
        return []


def _write(tmp_path, subdir, name, payload):
    directory = tmp_path / subdir
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return directory


# =====================================================================================
# parsing
# =====================================================================================
def test_defaults_block_is_merged_under_each_entry(tmp_path):
    _write(
        tmp_path,
        "bronze_control",
        "crm.yml",
        {
            "defaults": {"source_system": "crm", "target_catalog_name": "bronze", "load_type": "batch"},
            "bronze_control": [
                {
                    "bronze_schema_name": "crm",
                    "bronze_table_name": "customer",
                    "source_file_type": "csv",
                    "file_location": "/landing/crm/customer/",
                },
                {
                    "bronze_schema_name": "crm",
                    "bronze_table_name": "orders",
                    "source_file_type": "json",
                    "file_location": "/landing/crm/orders/",
                    "load_type": "stream",  # entry-level override wins
                },
            ],
        },
    )
    loader = _LoaderUnderTest(tmp_path)
    records, files = loader._read_yaml_dir(BRONZE_SPEC)

    assert files == ["crm.yml"]
    assert len(records) == 2
    assert all(r["source_system"] == "crm" for r in records)
    assert records[0]["load_type"] == "batch"
    assert records[1]["load_type"] == "stream"
    # config_file_name is stamped automatically for traceability back to git.
    assert all(r["config_file_name"] == "crm.yml" for r in records)


def test_bare_list_file_is_accepted(tmp_path):
    directory = tmp_path / "dq_rules"
    directory.mkdir(parents=True)
    (directory / "rules.yml").write_text(
        yaml.safe_dump([{"rule_id": "R1", "rule_type": "sql", "rule": "1=1"}]), encoding="utf-8"
    )
    from framework.control.metadata_loader import DQ_RULES_SPEC

    records, _ = _LoaderUnderTest(tmp_path)._read_yaml_dir(DQ_RULES_SPEC)
    assert records[0]["rule_id"] == "R1"


def test_wrong_root_key_is_reported(tmp_path):
    _write(tmp_path, "bronze_control", "crm.yml", {"bronze_tables": []})
    with pytest.raises(MetadataValidationError, match="bronze_control"):
        _LoaderUnderTest(tmp_path)._read_yaml_dir(BRONZE_SPEC)


def test_missing_directory_is_not_fatal(tmp_path):
    records, files = _LoaderUnderTest(tmp_path)._read_yaml_dir(GOLD_SPEC)
    assert records == [] and files == []


# =====================================================================================
# validation
# =====================================================================================
def test_unknown_column_is_rejected(tmp_path):
    records = [
        {
            "source_system": "crm",
            "source_file_type": "csv",
            "file_location": "/landing/",
            "target_catalog_name": "bronze",
            "bronze_schema_name": "crm",
            "bronze_table_name": "customer",
            "load_type": "batch",
            "config_file_name": "crm.yml",
            "bronz_table_nmae": "typo",  # misspelled
        }
    ]
    with pytest.raises(MetadataValidationError, match="unknown column"):
        _LoaderUnderTest(tmp_path)._validate(BRONZE_SPEC, records)


def test_missing_required_column_is_rejected(tmp_path):
    records = [
        {
            "source_system": "crm",
            "source_file_type": "csv",
            "file_location": "/landing/",
            "bronze_schema_name": "crm",
            "bronze_table_name": "customer",
            "load_type": "batch",
            "config_file_name": "crm.yml",
            # target_catalog_name is absent
        }
    ]
    with pytest.raises(MetadataValidationError, match="target_catalog_name"):
        _LoaderUnderTest(tmp_path)._validate(BRONZE_SPEC, records)


def test_invalid_enum_value_is_rejected(tmp_path):
    records = [
        {
            "source_system": "crm",
            "source_file_type": "xlsx",  # Auto Loader has no xlsx reader
            "file_location": "/landing/",
            "target_catalog_name": "bronze",
            "bronze_schema_name": "crm",
            "bronze_table_name": "customer",
            "load_type": "batch",
            "config_file_name": "crm.yml",
        }
    ]
    with pytest.raises(MetadataValidationError, match="source_file_type"):
        _LoaderUnderTest(tmp_path)._validate(BRONZE_SPEC, records)


def test_duplicate_business_key_is_rejected(tmp_path):
    base = {
        "source_system": "crm",
        "source_file_type": "csv",
        "file_location": "/landing/",
        "target_catalog_name": "bronze",
        "bronze_schema_name": "crm",
        "bronze_table_name": "customer",
        "load_type": "batch",
        "config_file_name": "crm.yml",
    }
    # Two active rows for one business key would make the control lookup ambiguous.
    with pytest.raises(MetadataValidationError, match="duplicate business key"):
        _LoaderUnderTest(tmp_path)._validate(BRONZE_SPEC, [dict(base), dict(base)])


def test_silver_business_key_falls_back_to_source_table_name(tmp_path):
    records = [
        {
            "source_catalog_name": "bronze",
            "source_schema_name": "crm",
            "source_table_name": "customer",
            "target_catalog_name": "silver",
            "silver_schema_name": "crm",
            "load_type": "scd2",
            "business_keys": ["customer_id"],
            "config_file_name": "crm.yml",
        }
    ]
    key = MetadataLoader._business_key(SILVER_SPEC, records[0])
    assert key == ("silver", "crm", "customer")
    # And with silver_table_name absent the row still validates.
    _LoaderUnderTest(tmp_path)._validate(SILVER_SPEC, records)


def test_dq_assignment_requires_a_severity(tmp_path):
    records = [
        {
            "catalog_name": "bronze",
            "schema_name": "crm",
            "table_name": "customer",
            "column_name": "email",
            "rule_id": "DQ_NOT_NULL",
            "config_file_name": "crm.yml",
        }
    ]
    with pytest.raises(MetadataValidationError, match="severity"):
        _LoaderUnderTest(tmp_path)._validate(DQ_ASSIGNMENT_SPEC, records)


# =====================================================================================
# type coercion
# =====================================================================================
def test_scalar_is_lifted_into_an_array():
    assert _coerce("order_id", ArrayType(StringType())) == ["order_id"]
    assert _coerce("a, b", ArrayType(StringType())) == ["a", "b"]
    assert _coerce(["a", "b"], ArrayType(StringType())) == ["a", "b"]
    assert _coerce(None, ArrayType(StringType())) is None


def test_map_values_are_stringified_the_way_spark_options_expect():
    # YAML's `header: true` becomes python True; Spark options need the string "true".
    assert _coerce({"header": True, "maxFiles": 10}, MapType(StringType(), StringType())) == {
        "header": "true",
        "maxFiles": "10",
    }


def test_non_mapping_for_a_map_column_is_rejected():
    with pytest.raises(MetadataValidationError, match="expected a mapping"):
        _coerce("header=true", MapType(StringType(), StringType()))


def test_boolean_and_integer_coercion():
    assert _coerce("yes", BooleanType()) is True
    assert _coerce("false", BooleanType()) is False
    assert _coerce("200", IntegerType()) == 200


# =====================================================================================
# the repo's own metadata must be valid
# =====================================================================================
@pytest.mark.parametrize("spec", [BRONZE_SPEC, SILVER_SPEC, DQ_ASSIGNMENT_SPEC, GOLD_SPEC])
def test_shipped_sample_metadata_validates(spec):
    """Guards the sample YAML in conf/metadata against drift from the table specs."""
    loader = _LoaderUnderTest(REPO_ROOT / "conf" / "metadata")
    records, _ = loader._read_yaml_dir(spec)
    if records:
        loader._validate(spec, records)
