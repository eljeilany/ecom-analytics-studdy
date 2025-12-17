from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None

from pydantic import ValidationError

from shared import EXPECTED_HEADERS, RawEvent, iter_csv_rows, project_root


def _init_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_events (
          client_id VARCHAR NOT NULL,
          timestamp TIMESTAMP NOT NULL,
          event_name VARCHAR NOT NULL,
          event_data JSON NOT NULL,
          page_url VARCHAR NOT NULL,
          referrer VARCHAR,
          user_agent VARCHAR NOT NULL
        );
        """
    )

    conn.execute("CREATE SEQUENCE IF NOT EXISTS pipeline_logs_log_id_seq;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_logs (
          log_id BIGINT PRIMARY KEY DEFAULT nextval('pipeline_logs_log_id_seq'),
          filename VARCHAR NOT NULL,
          run_timestamp TIMESTAMP NOT NULL,
          rows_read BIGINT NOT NULL,
          rows_inserted BIGINT NOT NULL,
          rows_quarantined BIGINT NOT NULL,
          status VARCHAR NOT NULL
        );
        """
    )


def main() -> int:
    root = project_root()
    raw_dir = root / "data" / "raw"
    processed_dir = root / "data" / "processed"
    quarantine_dir = root / "data" / "quarantine"
    processed_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    db_path = processed_dir / "puffy_main.db"
    conn = duckdb.connect(str(db_path))
    _init_db(conn)

    csv_files = sorted(raw_dir.glob("*.csv"))

    for csv_path in csv_files:
        filename = csv_path.name
        rows, header_report = iter_csv_rows(csv_path)

        extra_columns = header_report.get("extra_columns", [])
        if extra_columns:
            print(f"WARNING: File {filename} has extra columns: {extra_columns}")

        missing_core = header_report.get("missing_core", [])
        if missing_core:
            print(f"CRITICAL WARNING: File {filename} is missing core columns: {missing_core}")

        rows_read = len(rows)
        valid_rows: list[dict[str, Any]] = []
        quarantine_rows: list[dict[str, Any]] = []

        for row in rows:
            try:
                model = RawEvent.model_validate(row)
                record = model.model_dump()
                record["event_name"] = model.event_name.value
                record["event_data"] = json.dumps(record["event_data"], separators=(",", ":"))
                valid_rows.append(record)
            except ValidationError as exc:
                failure_row: dict[str, Any] = {k: row.get(k) for k in EXPECTED_HEADERS}
                summarized_errors: list[str] = []
                for err in exc.errors(include_url=False):
                    loc = ".".join(str(p) for p in err.get("loc", ()))
                    msg = str(err.get("msg", "validation error"))
                    summary = f"{loc}: {msg}" if loc else msg
                    summarized_errors.append(summary)
                failure_row["error_reason"] = " | ".join(summarized_errors)
                quarantine_rows.append(failure_row)

        rows_inserted = 0
        if valid_rows:
            if pd is None:
                raise RuntimeError("pandas is required for ingestion but is not installed")
            df = pd.DataFrame(valid_rows, columns=EXPECTED_HEADERS)
            conn.register("df_valid_rows", df)
            conn.execute(
                """
                INSERT INTO raw_events
                SELECT
                  client_id,
                  timestamp,
                  event_name,
                  CAST(event_data AS JSON) AS event_data,
                  page_url,
                  referrer,
                  user_agent
                FROM df_valid_rows;
                """
            )
            rows_inserted = len(valid_rows)

        rows_quarantined = len(quarantine_rows)
        if quarantine_rows:
            out_path = quarantine_dir / f"{Path(filename).stem}_errors.csv"
            with out_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=[*EXPECTED_HEADERS, "error_reason"])
                writer.writeheader()
                writer.writerows(quarantine_rows)

        status = "COMPLETED"
        if rows_quarantined and rows_inserted:
            status = "partial_failure"
        elif rows_quarantined and not rows_inserted:
            status = "FAILED"

        conn.execute(
            """
            INSERT INTO pipeline_logs
              (filename, run_timestamp, rows_read, rows_inserted, rows_quarantined, status)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            [
                filename,
                datetime.utcnow(),
                rows_read,
                rows_inserted,
                rows_quarantined,
                status,
            ],
        )

    print("Ingestion Complete. Data stored in `puffy_main.db`. Check `pipeline_logs` table for details.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
