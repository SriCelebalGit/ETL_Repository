"""Tests for the DQ rule engine.

The behaviour that matters most is the severity contract, because getting it wrong
either loses good data (over-dropping) or publishes bad data (under-dropping):

    drop     -> quarantined AND excluded
    warning  -> quarantined AND still loaded
    fail     -> nothing loads
"""

from __future__ import annotations

import pytest

from framework.dq import DQEngine
from framework.dq.rule_engine import DQ_FAILED_RULES, DQ_STATUS
from framework.exceptions import DataQualityError
from framework.models import DQRule


def _rule(rule_id, rule, column, severity, rule_type="sql", **params):
    return DQRule(
        rule_id=rule_id,
        rule_type=rule_type,
        rule=rule,
        column_name=column,
        severity=severity,
        parameters=params,
    )


@pytest.fixture()
def customers(spark):
    return spark.createDataFrame(
        [
            (1, "Alice", "alice@example.com", 5000),
            (2, None, "bob@example.com", 7000),          # customer_name is null
            (3, "Carol", "not-an-email", 9000),          # email is malformed
            (4, "Dave", "dave@example.com", -100),       # credit_limit is negative
            (5, "Erin", "erin@example.com", 1000),
        ],
        "customer_id int, customer_name string, email string, credit_limit int",
    )


# =====================================================================================
# severity contract
# =====================================================================================
def test_drop_severity_excludes_and_quarantines(spark, customers):
    engine = DQEngine("batch-1")
    result = engine.apply(
        customers,
        [_rule("R_NAME", "{column} IS NOT NULL", "customer_name", "drop")],
        table_label="test.customer",
    )

    assert result.metrics.src_rec_count == 5
    assert result.metrics.quarantine_count == 1
    assert result.valid.count() == 4
    assert 2 not in [r["customer_id"] for r in result.valid.collect()]
    assert [r["customer_id"] for r in result.quarantined.collect()] == [2]


def test_warning_severity_quarantines_but_still_loads(spark, customers):
    engine = DQEngine("batch-1")
    result = engine.apply(
        customers,
        [_rule("R_EMAIL", "{column} RLIKE '^[^@]+@[^@]+\\\\.[^@]+$'", "email", "warning")],
        table_label="test.customer",
    )

    # The row is reported without being withheld - the whole point of `warning`.
    assert result.valid.count() == 5
    assert result.quarantined.count() == 1
    assert result.metrics.warning_count == 1
    assert result.metrics.quarantine_count == 0
    assert result.quarantined.collect()[0][DQ_STATUS] == "WARNING"


def test_fail_severity_aborts_before_anything_loads(spark, customers):
    engine = DQEngine("batch-1")
    with pytest.raises(DataQualityError, match="fail-severity"):
        engine.apply(
            customers,
            [_rule("R_NAME", "{column} IS NOT NULL", "customer_name", "fail")],
            table_label="test.customer",
        )


def test_fail_severity_passes_when_no_row_violates(spark, customers):
    engine = DQEngine("batch-1")
    result = engine.apply(
        customers,
        [_rule("R_ID", "{column} IS NOT NULL", "customer_id", "fail")],
        table_label="test.customer",
    )
    assert result.valid.count() == 5
    assert result.metrics.dq_check_outcome == "PASSED"


# =====================================================================================
# rule rendering
# =====================================================================================
def test_parameters_are_substituted_into_the_expression(spark, customers):
    engine = DQEngine("batch-1")
    result = engine.apply(
        customers,
        [_rule("R_RANGE", "{column} BETWEEN {min} AND {max}", "credit_limit", "drop", min=0, max=8000)],
        table_label="test.customer",
    )
    # 9000 is above max and -100 is below min.
    assert result.metrics.quarantine_count == 2
    detail = result.metrics.rule_details[0]
    assert detail["rule_expression"] == "credit_limit BETWEEN 0 AND 8000"


def test_missing_placeholder_value_is_reported(spark, customers):
    engine = DQEngine("batch-1")
    with pytest.raises(DataQualityError, match="placeholder"):
        engine.apply(
            customers,
            [_rule("R_RANGE", "{column} BETWEEN {min} AND {max}", "credit_limit", "drop", min=0)],
            table_label="test.customer",
        )


def test_filter_condition_scopes_a_rule(spark):
    df = spark.createDataFrame(
        [(1, "GB", "SW1A 1AA"), (2, "GB", None), (3, "US", None)],
        "id int, country string, postcode string",
    )
    engine = DQEngine("batch-1")
    rule = DQRule(
        rule_id="R_POSTCODE",
        rule_type="sql",
        rule="{column} IS NOT NULL",
        column_name="postcode",
        severity="drop",
        filter_condition="country = 'GB'",
    )
    result = engine.apply(df, [rule], table_label="test.address")
    # Only the GB row with a null postcode fails; the US row is out of scope.
    assert result.metrics.quarantine_count == 1
    assert sorted(r["id"] for r in result.valid.collect()) == [1, 3]


# =====================================================================================
# function rules
# =====================================================================================
def test_is_unique_function_rule(spark):
    df = spark.createDataFrame([(1,), (2,), (2,), (3,)], "customer_id int")
    engine = DQEngine("batch-1")
    result = engine.apply(
        df,
        [_rule("R_UNIQUE", "is_unique", "customer_id", "drop", rule_type="function")],
        table_label="test.customer",
    )
    # Both copies of the duplicate are quarantined - the engine cannot know which is right.
    assert result.metrics.quarantine_count == 2
    assert sorted(r["customer_id"] for r in result.valid.collect()) == [1, 3]


def test_unregistered_function_is_reported(spark, customers):
    engine = DQEngine("batch-1")
    with pytest.raises(KeyError, match="not registered"):
        engine.apply(
            customers,
            [_rule("R_X", "no_such_function", "customer_id", "drop", rule_type="function")],
            table_label="test.customer",
        )


# =====================================================================================
# multiple rules and reporting
# =====================================================================================
def test_multiple_rules_are_reported_per_row(spark, customers):
    engine = DQEngine("batch-1")
    result = engine.apply(
        customers,
        [
            _rule("R_NAME", "{column} IS NOT NULL", "customer_name", "drop"),
            _rule("R_CREDIT", "{column} >= 0", "credit_limit", "drop"),
            _rule("R_EMAIL", "{column} LIKE '%@%'", "email", "warning"),
        ],
        table_label="test.customer",
    )

    assert result.metrics.rules_evaluated == 3
    assert result.metrics.rules_failed == 3
    quarantined = {r["customer_id"]: r for r in result.quarantined.collect()}
    assert set(quarantined) == {2, 3, 4}
    assert [f["rule_id"] for f in quarantined[2][DQ_FAILED_RULES]] == ["R_NAME"]
    assert quarantined[3][DQ_STATUS] == "WARNING"
    assert quarantined[4][DQ_STATUS] == "FAILED"


def test_missing_column_skips_the_rule_rather_than_failing(spark, customers):
    engine = DQEngine("batch-1")
    result = engine.apply(
        customers,
        [
            _rule("R_GONE", "{column} IS NOT NULL", "column_that_does_not_exist", "drop"),
            _rule("R_ID", "{column} IS NOT NULL", "customer_id", "drop"),
        ],
        table_label="test.customer",
    )
    # A stale assignment must not stop a table loading; it is reported instead.
    assert result.skipped_rules == ["R_GONE"]
    assert result.metrics.rules_evaluated == 1
    assert result.valid.count() == 5


def test_no_rules_is_a_pass_through(spark, customers):
    result = DQEngine("batch-1").apply(customers, [], table_label="test.customer")
    assert result.metrics.dq_check_outcome == "NO_RULES"
    assert result.quarantined is None
    assert result.valid.count() == 5


def test_threshold_breach_aborts_the_batch(spark, customers):
    engine = DQEngine("batch-1")
    with pytest.raises(DataQualityError, match="threshold"):
        engine.apply(
            customers,
            [_rule("R_CREDIT", "{column} > 6000", "credit_limit", "drop")],
            table_label="test.customer",
            failure_threshold_pct=10.0,  # 3 of 5 rows fail, far above 10%
        )


def test_rule_detail_counts_are_recorded(spark, customers):
    result = DQEngine("batch-1").apply(
        customers,
        [_rule("R_CREDIT", "{column} >= 0", "credit_limit", "drop")],
        table_label="test.customer",
    )
    detail = result.metrics.rule_details[0]
    assert detail["rows_evaluated"] == 5
    assert detail["rows_failed"] == 1
    assert detail["pass_pct"] == 80.0
    assert detail["rule_status"] == "FAILED"
