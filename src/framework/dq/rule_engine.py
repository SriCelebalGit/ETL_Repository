"""Data quality rule engine.

Evaluates every rule assigned to a table in ONE pass over the data, then splits the
result according to severity:

    fail     -> any failing row aborts the task (nothing is loaded)
    drop     -> failing rows are quarantined and excluded from the silver load
    warning  -> failing rows are quarantined AND still loaded

The single-pass design matters: a table with 30 assigned rules evaluated one rule at a
time would scan the batch 30 times. Instead each rule becomes a boolean column, the
failures are collected into an array of rule ids, and one filter separates the sets.

Rules are rendered before evaluation, so a single registry rule can serve many
columns: `{column}` is replaced with the assigned column and `{param}` with the
matching entry in rule_parameters.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..audit import DQRunMetrics
from ..exceptions import DataQualityError
from ..logging_utils import FrameworkLogger
from ..models import DQRule
from .functions import get_function

_PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z0-9_]+)\}")

# Column names the engine adds to the data. Kept together so the silver writer can
# strip them before the target write.
DQ_FAILED_RULES = "_dq_failed_rules"
DQ_STATUS = "_dq_status"
DQ_BATCH_ID = "_dq_batch_id"
DQ_QUARANTINED_AT = "_dq_quarantined_at"
DQ_COLUMNS = (DQ_FAILED_RULES, DQ_STATUS, DQ_BATCH_ID, DQ_QUARANTINED_AT)


@dataclass
class DQResult:
    """Outcome of evaluating a table's rules against one batch."""

    valid: DataFrame
    quarantined: Optional[DataFrame]
    metrics: DQRunMetrics
    evaluated_rule_count: int = 0
    skipped_rules: List[str] = field(default_factory=list)


class DQEngine:
    def __init__(self, batch_id: str, logger: Optional[FrameworkLogger] = None):
        self.batch_id = batch_id
        self.log = logger or FrameworkLogger({"component": "dq_engine"})

    # =============================================================================
    # public API
    # =============================================================================
    def apply(
        self,
        df: DataFrame,
        rules: List[DQRule],
        table_label: str,
        failure_threshold_pct: Optional[float] = None,
        collect_rule_details: bool = True,
    ) -> DQResult:
        """Evaluate `rules` against `df` and split it into valid and quarantined rows."""
        metrics = DQRunMetrics()

        if not rules:
            metrics.dq_check_outcome = "NO_RULES"
            self.log.info("no DQ rules assigned", table=table_label)
            return DQResult(valid=df, quarantined=None, metrics=metrics)

        applicable, skipped = self._filter_applicable(df, rules, table_label)
        if not applicable:
            metrics.dq_check_outcome = "NO_RULES"
            return DQResult(valid=df, quarantined=None, metrics=metrics, skipped_rules=skipped)

        # One boolean column per rule, all evaluated in the same projection.
        flag_columns: Dict[str, Tuple[DQRule, str]] = {}
        annotated = df
        for index, (rule, expression) in enumerate(applicable):
            flag = f"_dq_flag_{index}"
            annotated = annotated.withColumn(flag, self._pass_column(annotated, rule, expression))
            flag_columns[flag] = (rule, expression)

        annotated = self._add_verdict_columns(annotated, flag_columns)
        annotated = annotated.persist()

        try:
            metrics.src_rec_count = annotated.count()
            metrics.rules_evaluated = len(applicable)

            if collect_rule_details:
                metrics.rule_details = self._rule_details(annotated, flag_columns, metrics.src_rec_count)
                metrics.rules_failed = sum(1 for d in metrics.rule_details if d["rows_failed"])

            fail_rules = [r.rule_id for r, _ in applicable if r.severity == "fail"]
            if fail_rules:
                self._enforce_fail_severity(annotated, flag_columns, table_label)

            valid, quarantined, counts = self._split(annotated, flag_columns)
            metrics.quarantine_count = counts["quarantined"]
            metrics.warning_count = counts["warning_only"]
            metrics.dq_check_outcome = "QUARANTINED" if metrics.quarantine_count else "PASSED"

            self._enforce_threshold(metrics, failure_threshold_pct, table_label)

            self.log.info(
                "DQ evaluation complete",
                table=table_label,
                rules_evaluated=metrics.rules_evaluated,
                rules_failed=metrics.rules_failed,
                src_rec_count=metrics.src_rec_count,
                quarantine_count=metrics.quarantine_count,
                warning_count=metrics.warning_count,
            )
            # The caller writes both frames; unpersisting here would force a recompute.
            return DQResult(
                valid=valid,
                quarantined=quarantined,
                metrics=metrics,
                evaluated_rule_count=len(applicable),
                skipped_rules=skipped,
            )
        except Exception:
            annotated.unpersist()
            raise

    # =============================================================================
    # rule rendering
    # =============================================================================
    def _filter_applicable(
        self, df: DataFrame, rules: List[DQRule], table_label: str
    ) -> Tuple[List[Tuple[DQRule, str]], List[str]]:
        """Drop rules whose target column is absent, and render the rest.

        A missing column is skipped with a warning rather than failing the task: bronze
        schemas evolve, and one stale assignment should not stop a table loading. The
        skip is logged and lands in dq_result_detail as ERRORED so it stays visible.
        """
        applicable: List[Tuple[DQRule, str]] = []
        skipped: List[str] = []
        columns = set(df.columns)

        for rule in rules:
            if not rule.is_table_level and rule.column_name not in columns:
                skipped.append(rule.rule_id)
                self.log.warning(
                    "DQ rule skipped - column not present",
                    table=table_label,
                    rule_id=rule.rule_id,
                    column=rule.column_name,
                )
                continue
            applicable.append((rule, self._render(rule)))
        return applicable, skipped

    def _render(self, rule: DQRule) -> str:
        """Substitute {column} and {param} placeholders in the rule text."""
        scope = dict(rule.parameters)
        scope["column"] = rule.column_name

        def replace(match: re.Match) -> str:
            key = match.group(1)
            if key not in scope:
                raise DataQualityError(
                    f"rule {rule.rule_id}: placeholder {{{key}}} has no value. "
                    f"Supply it in rule_parameters (available: {sorted(scope)})"
                )
            return str(scope[key])

        return _PLACEHOLDER_RE.sub(replace, rule.rule)

    def _pass_column(self, df: DataFrame, rule: DQRule, expression: str) -> Column:
        """Boolean column that is TRUE when the row passes `rule`.

        `filter_condition` narrows a rule to a subset of rows - rows outside the
        subset are treated as passing, which is what makes conditional checks such as
        "postcode must be valid, but only for GB addresses" expressible.
        """
        if rule.rule_type == "sql":
            passed = F.expr(expression)
        elif rule.rule_type == "function":
            column_target = "" if rule.is_table_level else rule.column_name
            passed = get_function(expression)(df, column_target, rule.parameters)
        else:  # guarded by DQRule.from_row, kept for clarity
            raise DataQualityError(f"rule {rule.rule_id}: unsupported rule_type {rule.rule_type!r}")

        # A NULL verdict (e.g. a comparison against NULL) is treated as a pass so that
        # NOT NULL checks stay the explicit way to reject missing data.
        passed = F.coalesce(passed, F.lit(True))

        if rule.filter_condition:
            passed = F.when(F.expr(rule.filter_condition), passed).otherwise(F.lit(True))
        return passed

    # =============================================================================
    # verdict / split
    # =============================================================================
    def _add_verdict_columns(
        self, df: DataFrame, flag_columns: Dict[str, Tuple[DQRule, str]]
    ) -> DataFrame:
        """Collapse the per-rule flags into an array of failed rule ids and a status."""
        failed_entries = [
            F.when(
                ~F.col(flag),
                F.struct(
                    F.lit(rule.rule_id).alias("rule_id"),
                    F.lit(rule.column_name).alias("column_name"),
                    F.lit(rule.severity).alias("severity"),
                ),
            )
            for flag, (rule, _) in flag_columns.items()
        ]
        drop_or_fail = [
            ~F.col(flag) for flag, (rule, _) in flag_columns.items() if rule.severity in {"drop", "fail"}
        ]
        warning_only = [~F.col(flag) for flag, (rule, _) in flag_columns.items() if rule.severity == "warning"]

        has_blocking = _any_true(drop_or_fail)
        has_warning = _any_true(warning_only)

        return (
            df.withColumn(DQ_FAILED_RULES, F.array_compact(F.array(*failed_entries)))
            .withColumn(
                DQ_STATUS,
                F.when(has_blocking, F.lit("FAILED"))
                .when(has_warning, F.lit("WARNING"))
                .otherwise(F.lit("PASSED")),
            )
            .withColumn(DQ_BATCH_ID, F.lit(self.batch_id))
            .withColumn(DQ_QUARANTINED_AT, F.current_timestamp())
        )

    def _split(
        self, annotated: DataFrame, flag_columns: Dict[str, Tuple[DQRule, str]]
    ) -> Tuple[DataFrame, Optional[DataFrame], Dict[str, int]]:
        """Separate the batch into what loads and what is quarantined.

        Warning rows appear in BOTH frames by design: the row is good enough to load,
        and the operator still needs to see the finding.
        """
        internal_flags = list(flag_columns)

        quarantine_candidates = annotated.filter(F.col(DQ_STATUS) != "PASSED")
        quarantined = quarantine_candidates.drop(*internal_flags)

        valid = annotated.filter(F.col(DQ_STATUS) != "FAILED").drop(*internal_flags, *DQ_COLUMNS)

        counts = {
            "quarantined": quarantine_candidates.filter(F.col(DQ_STATUS) == "FAILED").count(),
            "warning_only": quarantine_candidates.filter(F.col(DQ_STATUS) == "WARNING").count(),
        }
        if counts["quarantined"] == 0 and counts["warning_only"] == 0:
            return valid, None, counts
        return valid, quarantined, counts

    # =============================================================================
    # reporting / enforcement
    # =============================================================================
    def _rule_details(
        self,
        annotated: DataFrame,
        flag_columns: Dict[str, Tuple[DQRule, str]],
        total_rows: int,
    ) -> List[Dict[str, object]]:
        """Per-rule failure counts, aggregated in one pass over the annotated batch."""
        if not flag_columns:
            return []
        aggregates = [
            F.sum(F.when(~F.col(flag), F.lit(1)).otherwise(F.lit(0))).alias(flag)
            for flag in flag_columns
        ]
        row = annotated.agg(*aggregates).collect()[0]

        details: List[Dict[str, object]] = []
        for flag, (rule, expression) in flag_columns.items():
            failed = int(row[flag] or 0)
            details.append(
                {
                    "column_name": rule.column_name,
                    "rule_id": rule.rule_id,
                    "rule_type": rule.rule_type,
                    "rule_expression": expression,
                    "dq_dimension": rule.dq_dimension,
                    "severity": rule.severity,
                    "rows_evaluated": total_rows,
                    "rows_failed": failed,
                    "pass_pct": round(100.0 * (total_rows - failed) / total_rows, 4) if total_rows else None,
                    "rule_status": "FAILED" if failed else "PASSED",
                    "error_message": None,
                }
            )
        return details

    def _enforce_fail_severity(
        self, annotated: DataFrame, flag_columns: Dict[str, Tuple[DQRule, str]], table_label: str
    ) -> None:
        """Abort when a fail-severity rule has any failing row.

        Reserved for invariants where loading partial data is worse than loading none -
        a missing primary key, or a currency code the downstream model cannot handle.
        """
        offenders: List[str] = []
        for flag, (rule, expression) in flag_columns.items():
            if rule.severity != "fail":
                continue
            if annotated.filter(~F.col(flag)).limit(1).count() > 0:
                offenders.append(f"{rule.rule_id} on {rule.column_name} ({expression})")
        if offenders:
            raise DataQualityError(
                f"{table_label}: {len(offenders)} fail-severity DQ rule(s) fired, nothing was loaded:\n  - "
                + "\n  - ".join(offenders)
            )

    def _enforce_threshold(
        self, metrics: DQRunMetrics, failure_threshold_pct: Optional[float], table_label: str
    ) -> None:
        """Abort when too much of the batch is being quarantined.

        A batch that is 60% quarantined is usually a source-side change, not 60% bad
        rows - loading the remainder would quietly publish an incomplete table.
        """
        if failure_threshold_pct is None or not metrics.src_rec_count:
            return
        if metrics.quarantine_pct > failure_threshold_pct:
            metrics.dq_check_outcome = "THRESHOLD_BREACHED"
            metrics.pipeline_status = "FAILED"
            metrics.error_message = (
                f"{metrics.quarantine_pct}% of the batch was quarantined, "
                f"threshold is {failure_threshold_pct}%"
            )
            raise DataQualityError(f"{table_label}: {metrics.error_message}")


def _any_true(conditions: List[Column]) -> Column:
    """OR-reduce a list of boolean columns; an empty list is FALSE."""
    if not conditions:
        return F.lit(False)
    combined = conditions[0]
    for condition in conditions[1:]:
        combined = combined | condition
    return combined
