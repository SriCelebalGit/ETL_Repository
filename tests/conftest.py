"""Shared pytest fixtures.

The Spark fixture is session scoped and local: starting a JVM per test would dominate
the run time. Tests that need Spark are skipped rather than failed when pyspark or a
JVM is unavailable, so `pytest` still gives useful output on a machine without one.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
for candidate in (REPO_ROOT / "src", REPO_ROOT / "notebooks"):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark", reason="pyspark is not installed")
    from pyspark.sql import SparkSession

    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("etl_framework_tests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except Exception as exc:  # no JVM available
        pytest.skip(f"cannot start a local Spark session: {exc}")
    yield session
    session.stop()


@pytest.fixture()
def framework_config():
    """A FrameworkConfig built in memory, so tests do not depend on conf/ contents."""
    from framework.config import FrameworkConfig

    return FrameworkConfig.from_dict(
        {
            "framework_catalog": "fw_test",
            "control_schema": "control",
            "audit_schema": "audit",
            "catalogs": {"bronze": "bronze_test", "silver": "silver_test", "gold": "gold_test"},
            "checkpoint_root": "/Volumes/fw_test/etl_volumes/checkpoints",
            "defaults": {
                "schema_evolution_mode": "addNewColumns",
                "rescued_data_column": "_rescued_data",
                "silver_schema": "silver_crm",
            },
        },
        environment="test",
    )
