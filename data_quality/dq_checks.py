"""
data_quality/dq_checks.py — Reusable PySpark Data Quality Engine
================================================================
Defines structured classes and rules for validating schemas,
handling nulls, ranges, uniqueness, and temporal freshness.
"""

from datetime import datetime, timezone
from pyspark.sql import DataFrame, functions as F

class DQResult:
    """Holds the outcome of a single DQ rule validation."""
    def __init__(self, rule: str, passed: bool, fail_count: int, total: int, details: str = ""):
        self.rule = rule
        self.passed = passed
        self.fail_count = fail_count
        self.total = total
        self.details = details

    def to_dict(self):
        """Convert result metadata to serializable dict."""
        return {
            "rule": self.rule,
            "passed": self.passed,
            "fail_count": self.fail_count,
            "total": self.total,
            "fail_pct": round(self.fail_count / self.total * 100, 2) if self.total else 0,
            "details": self.details,
        }


def dq_not_null(df: DataFrame, columns: list) -> DQResult:
    """Rule: Specified critical columns must contain no NULL/NaN values."""
    condition = F.lit(False)
    for col in columns:
        condition = condition | F.col(col).isNull()

    fail_count = df.filter(condition).count()
    return DQResult(
        rule=f"NOT_NULL({', '.join(columns)})",
        passed=(fail_count == 0),
        fail_count=fail_count,
        total=df.count(),
        details=f"Null entries found in key columns: {columns}",
    )


def dq_non_negative(df: DataFrame, columns: list) -> DQResult:
    """Rule: Numeric fields must be greater than or equal to zero."""
    condition = F.lit(False)
    for col in columns:
        if col in df.columns:
            condition = condition | (F.col(col) < 0)

    fail_count = df.filter(condition).count()
    total = df.count()
    return DQResult(
        rule=f"NON_NEGATIVE({', '.join(columns)})",
        passed=(fail_count == 0),
        fail_count=fail_count,
        total=total,
        details=f"Negative values detected in: {columns}",
    )


def dq_no_duplicates(df: DataFrame, key_cols: list) -> DQResult:
    """Rule: Uniqueness verification on composite partition/primary key columns."""
    total = df.count()
    distinct = df.select(key_cols).distinct().count()
    fail_count = total - distinct
    return DQResult(
        rule=f"UNIQUE({', '.join(key_cols)})",
        passed=(fail_count == 0),
        fail_count=fail_count,
        total=total,
        details=f"Composite duplicate keys found on: {key_cols}",
    )


def dq_row_count(df: DataFrame, min_rows: int = 1) -> DQResult:
    """Rule: Assert that the incoming ingestion batch is not empty or too small."""
    total = df.count()
    passed = total >= min_rows
    return DQResult(
        rule=f"MIN_ROWS({min_rows})",
        passed=passed,
        fail_count=0 if passed else 1,
        total=total,
        details=f"File batch size check failed: expected >= {min_rows}, got {total}",
    )


def dq_freshness(df: DataFrame, timestamp_col: str, max_hours: int = 48) -> DQResult:
    """Rule: Assert temporal freshness (records must have been ingested recently)."""
    threshold = F.expr(f"current_timestamp() - INTERVAL {max_hours} HOURS")
    fail_count = df.filter(F.col(timestamp_col) < threshold).count()
    total = df.count()
    return DQResult(
        rule=f"FRESHNESS({timestamp_col}, {max_hours}h)",
        passed=(fail_count == 0),
        fail_count=fail_count,
        total=total,
        details=f"Stale data detected: {fail_count} rows ingested older than {max_hours} hours",
    )
