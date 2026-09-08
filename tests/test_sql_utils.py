"""Tests for SQL script splitting.

The framework's own DDL is the motivating case: its `COMMENT` literals and header
comments contain semicolons, so a naive `text.split(";")` produced fragments that were
not valid SQL. These tests pin the behaviour that fixed it, and assert the shipped DDL
still splits into runnable statements.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from framework.sql_utils import render_placeholders, split_sql_statements, strip_sql_comments

REPO_ROOT = Path(__file__).resolve().parents[1]
_DDL_VERBS = {"CREATE", "ALTER", "COMMENT", "INSERT", "DROP", "USE", "SELECT", "MERGE", "UPDATE"}


# =====================================================================================
# splitting
# =====================================================================================
def test_simple_statements_are_split():
    assert split_sql_statements("SELECT 1; SELECT 2;") == ["SELECT 1", "SELECT 2"]


def test_trailing_semicolon_does_not_produce_an_empty_statement():
    assert split_sql_statements("SELECT 1;\n\n") == ["SELECT 1"]


def test_semicolon_inside_a_string_literal_is_not_a_separator():
    script = "CREATE TABLE t (c STRING COMMENT 'stream | batch; both use Auto Loader');"
    statements = split_sql_statements(script)
    assert len(statements) == 1
    assert "both use Auto Loader" in statements[0]


def test_escaped_quote_inside_a_literal_is_handled():
    script = "SELECT 'it''s here; really' AS a; SELECT 2;"
    assert split_sql_statements(script) == ["SELECT 'it''s here; really' AS a", "SELECT 2"]


def test_semicolon_inside_a_line_comment_is_not_a_separator():
    script = "-- business key: (a, b); versions are closed later\nCREATE TABLE t (a INT);"
    statements = split_sql_statements(script)
    assert len(statements) == 1
    assert statements[0].strip().endswith("CREATE TABLE t (a INT)")


def test_semicolon_inside_a_block_comment_is_not_a_separator():
    script = "/* one; two; three */ CREATE TABLE t (a INT);"
    assert len(split_sql_statements(script)) == 1


def test_comment_only_chunks_are_dropped():
    script = "-- just a header\n\n/* and a block */\n"
    assert split_sql_statements(script) == []


def test_statement_preceded_by_a_comment_is_kept():
    # The original bug: a chunk starting with "--" was discarded, taking the SQL that
    # followed the comment with it.
    script = "-- header comment\nCREATE SCHEMA IF NOT EXISTS a.b;"
    statements = split_sql_statements(script)
    assert len(statements) == 1
    assert "CREATE SCHEMA" in statements[0]


def test_backquoted_identifier_containing_a_semicolon():
    script = "SELECT `odd;name` FROM t; SELECT 2;"
    assert split_sql_statements(script) == ["SELECT `odd;name` FROM t", "SELECT 2"]


def test_statement_without_a_trailing_semicolon_is_returned():
    assert split_sql_statements("SELECT 1") == ["SELECT 1"]


# =====================================================================================
# comment stripping
# =====================================================================================
def test_strip_comments_leaves_literals_intact():
    assert strip_sql_comments("SELECT 'a -- not a comment' -- but this is\n").strip() == (
        "SELECT 'a -- not a comment'"
    )


def test_strip_block_comments():
    assert strip_sql_comments("SELECT /* skip */ 1").strip() == "SELECT  1".strip()


# =====================================================================================
# placeholder rendering
# =====================================================================================
def test_placeholders_are_rendered():
    assert render_placeholders("USE ${cat}.${sch}", {"cat": "c", "sch": "s"}) == "USE c.s"


def test_unresolved_placeholder_is_reported():
    with pytest.raises(ValueError, match="unresolved placeholder"):
        render_placeholders("USE ${cat}.${sch}", {"cat": "c"})


# =====================================================================================
# the shipped DDL must split into runnable statements
# =====================================================================================
@pytest.mark.parametrize("ddl_file", ["01_control_tables.sql", "02_audit_tables.sql"])
def test_shipped_ddl_splits_into_valid_statements(ddl_file):
    script = render_placeholders(
        (REPO_ROOT / "ddl" / ddl_file).read_text(encoding="utf-8"),
        {"fw_catalog": "fw", "fw_schema": "control", "fw_audit_schema": "audit"},
    )
    statements = split_sql_statements(script)
    assert statements, f"{ddl_file} produced no statements"
    for statement in statements:
        first_word = strip_sql_comments(statement).strip().split()[0].upper()
        assert first_word in _DDL_VERBS, f"{ddl_file}: statement starts with {first_word!r}"


def test_control_ddl_creates_every_control_table():
    script = render_placeholders(
        (REPO_ROOT / "ddl" / "01_control_tables.sql").read_text(encoding="utf-8"),
        {"fw_catalog": "fw", "fw_schema": "control", "fw_audit_schema": "audit"},
    )
    statements = split_sql_statements(script)
    created = " ".join(statements).lower()
    for table in (
        "bronze_control_table",
        "silver_control_table",
        "dq_rules",
        "dq_rules_assignment",
        "gold_control_table",
    ):
        assert f"fw.control.{table} (" in created, f"{table} is not created by the DDL"


def test_audit_ddl_creates_every_audit_table_and_view():
    script = render_placeholders(
        (REPO_ROOT / "ddl" / "02_audit_tables.sql").read_text(encoding="utf-8"),
        {"fw_catalog": "fw", "fw_schema": "control", "fw_audit_schema": "audit"},
    )
    created = " ".join(split_sql_statements(script)).lower()
    for obj in (
        "job_run_audit",
        "dq_run_audit",
        "dq_result_detail",
        "bronze_file_audit",
        "vw_latest_batch_status",
        "vw_dq_rule_trend",
    ):
        assert f"fw.audit.{obj}" in created, f"{obj} is not created by the DDL"


def test_ddl_column_lists_match_the_loader_table_specs():
    """The DDL and the loader's TableSpec must agree, or a MERGE fails at run time."""
    from framework.control.metadata_loader import TABLE_SPECS

    script = (REPO_ROOT / "ddl" / "01_control_tables.sql").read_text(encoding="utf-8").lower()
    for table_name, spec in TABLE_SPECS.items():
        # Locate this table's CREATE block, then confirm every spec column appears in it.
        marker = f"{table_name} ("
        assert marker in script, f"{table_name} missing from the DDL"
        start = script.index(marker)
        end = script.index("using delta", start)
        block = script[start:end]
        for column, _ in spec.columns:
            assert column.lower() in block, f"{table_name}.{column} is in TableSpec but not in the DDL"
