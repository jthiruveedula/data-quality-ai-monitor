"""BigQuery table statistical profiler for data quality monitoring."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from google.cloud import bigquery

logger = logging.getLogger(__name__)


@dataclass
class ColumnProfile:
    column_name: str
    data_type: str
    null_count: int
    null_rate: float
    distinct_count: int
    min_value: Any
    max_value: Any
    avg_value: float | None
    p25: float | None
    p50: float | None
    p75: float | None


@dataclass
class TableProfile:
    project_id: str
    dataset: str
    table: str
    row_count: int
    column_count: int
    profile_timestamp: datetime
    columns: list[ColumnProfile]
    schema_hash: str  # MD5 of schema fingerprint


class BigQueryProfiler:
    """Profiles BigQuery tables for data quality monitoring."""

    def __init__(self, project_id: str) -> None:
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id

    def profile_table(
        self,
        dataset: str,
        table: str,
        sample_rate: float = 1.0,
    ) -> TableProfile:
        """Generate a full statistical profile of a BigQuery table."""
        full_table = f"`{self.project_id}.{dataset}.{table}`"
        
        # Get row count and schema
        table_ref = self.client.get_table(f"{self.project_id}.{dataset}.{table}")
        row_count = table_ref.num_rows
        schema = table_ref.schema
        schema_hash = self._compute_schema_hash(schema)

        logger.info("Profiling %s.%s (%d rows)", dataset, table, row_count)

        # Build dynamic profiling query
        sample_clause = f"TABLESAMPLE SYSTEM ({int(sample_rate * 100)} PERCENT)" if sample_rate < 1.0 else ""
        
        numeric_cols = [f.name for f in schema if f.field_type in ("INTEGER", "FLOAT", "NUMERIC", "BIGNUMERIC")]
        all_cols = [f.name for f in schema]

        select_parts = []
        for col in all_cols:
            select_parts.extend([
                f"COUNTIF({col} IS NULL) AS {col}__nulls",
                f"COUNT(DISTINCT {col}) AS {col}__distinct",
            ])
        for col in numeric_cols:
            select_parts.extend([
                f"MIN({col}) AS {col}__min",
                f"MAX({col}) AS {col}__max",
                f"AVG({col}) AS {col}__avg",
                f"APPROX_QUANTILES({col}, 4)[OFFSET(1)] AS {col}__p25",
                f"APPROX_QUANTILES({col}, 4)[OFFSET(2)] AS {col}__p50",
                f"APPROX_QUANTILES({col}, 4)[OFFSET(3)] AS {col}__p75",
            ])

        query = f"""
            SELECT
                COUNT(*) AS __total_rows,
                {', '.join(select_parts)}
            FROM {full_table} {sample_clause}
        """

        result = list(self.client.query(query).result())[0]
        total_rows = result["__total_rows"]

        columns = []
        for field in schema:
            col = field.name
            null_count = result.get(f"{col}__nulls", 0) or 0
            distinct = result.get(f"{col}__distinct", 0) or 0
            columns.append(ColumnProfile(
                column_name=col,
                data_type=field.field_type,
                null_count=null_count,
                null_rate=null_count / total_rows if total_rows > 0 else 0.0,
                distinct_count=distinct,
                min_value=result.get(f"{col}__min"),
                max_value=result.get(f"{col}__max"),
                avg_value=result.get(f"{col}__avg"),
                p25=result.get(f"{col}__p25"),
                p50=result.get(f"{col}__p50"),
                p75=result.get(f"{col}__p75"),
            ))

        return TableProfile(
            project_id=self.project_id,
            dataset=dataset,
            table=table,
            row_count=total_rows,
            column_count=len(schema),
            profile_timestamp=datetime.utcnow(),
            columns=columns,
            schema_hash=schema_hash,
        )

    def _compute_schema_hash(self, schema: list) -> str:
        import hashlib
        schema_str = "|".join(f"{f.name}:{f.field_type}:{f.mode}" for f in schema)
        return hashlib.md5(schema_str.encode()).hexdigest()
