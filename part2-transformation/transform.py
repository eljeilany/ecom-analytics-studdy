from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Sequence

import duckdb


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _declared_objects(sql_text: str) -> list[str]:
    pattern = re.compile(
        r"(?im)^[ \t]*create[ \t]+or[ \t]+replace[ \t]+(?:temp[ \t]+view|view|table)[ \t]+([a-zA-Z_][\\w]*)"
    )
    seen: set[str] = set()
    out: list[str] = []
    for name in pattern.findall(sql_text):
        if name not in seen:
            seen.add(name)
            out.append(name)
    return out


def _execute_sql_script(conn: duckdb.DuckDBPyConnection, sql_text: str) -> None:
    try:
        conn.execute(sql_text)
        return
    except Exception:
        pass

    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    for statement in statements:
        conn.execute(statement)


CheckType = Literal["zero_rows", "summary", "event_coverage", "diagnostic_dupes"]


@dataclass(frozen=True)
class SqlCheck:
    name: str
    sql: str
    pass_condition: CheckType


def _fetch_table(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    *,
    limit: int | None = None,
) -> tuple[list[str], list[tuple[Any, ...]]]:
    limited_sql = sql
    if limit is not None:
        limited_sql = f"SELECT * FROM ({sql}) t LIMIT {int(limit)}"
    cursor = conn.execute(limited_sql)
    cols = [d[0] for d in cursor.description] if cursor.description else []
    rows = cursor.fetchall()
    return cols, rows


def _format_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    if not headers:
        return "(no columns)"
    display_rows = [[("" if v is None else str(v)) for v in row] for row in rows]
    widths = [len(str(h)) for h in headers]
    for row in display_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(cells: Sequence[str]) -> str:
        return " | ".join(cells[i].ljust(widths[i]) for i in range(len(widths)))

    sep = "-+-".join("-" * w for w in widths)
    out = [fmt_row([str(h) for h in headers]), sep]
    out.extend(fmt_row(row) for row in display_rows)
    return "\n".join(out)


def _run_checks(conn: duckdb.DuckDBPyConnection) -> int:
    checks: list[SqlCheck] = [
        SqlCheck(
            name="Q2.1.a sessions overview",
            pass_condition="summary",
            sql="""
SELECT 
    COUNT(*) AS total_sessions,
    COUNT(DISTINCT client_id) AS distinct_clients,
    COUNT(DISTINCT master_user_id) AS distinct_master_users,
    AVG(session_duration_minutes) AS avg_session_duration_min,
    AVG(actions_per_session) AS avg_actions_per_session,
    SUM(CASE WHEN actions_per_session = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS bounce_rate_percent,
    AVG(converted) * 100.0 AS session_conversion_rate_percent
FROM dim_sessions
""".strip(),
        ),
        SqlCheck(
            name="Q2.2.a within-session gap >=30",
            pass_condition="zero_rows",
            sql="""
WITH events_with_session AS (
    SELECT
        s.session_id,
        e.client_id,
        e.timestamp,
        LAG(e.timestamp) OVER (PARTITION BY s.session_id ORDER BY e.timestamp) AS prev_ts
    FROM stg_events e
    JOIN dim_sessions s
      ON e.client_id = s.client_id
     AND e.timestamp >= s.started_at
     AND e.timestamp <= s.ended_at
)
SELECT 
    session_id,
    client_id,
    MAX(date_diff('minute', prev_ts, timestamp)) AS max_gap_minutes
FROM events_with_session
GROUP BY 1,2
HAVING MAX(date_diff('minute', prev_ts, timestamp)) >= 30
ORDER BY max_gap_minutes DESC
""".strip(),
        ),
        SqlCheck(
            name="Q2.2.b between-session gap <30",
            pass_condition="zero_rows",
            sql="""
WITH ordered AS (
    SELECT
        client_id,
        session_id,
        started_at,
        ended_at,
        LAG(ended_at) OVER (PARTITION BY client_id ORDER BY started_at) AS prev_session_end
    FROM dim_sessions
)
SELECT
    client_id,
    session_id,
    date_diff('minute', prev_session_end, started_at) AS gap_minutes
FROM ordered
WHERE prev_session_end IS NOT NULL
  AND date_diff('minute', prev_session_end, started_at) < 30
ORDER BY gap_minutes ASC
""".strip(),
        ),
        SqlCheck(
            name="Q2.3.a event coverage",
            pass_condition="event_coverage",
            sql="""
WITH matched AS (
    SELECT e.client_id, e.timestamp
    FROM stg_events e
    JOIN dim_sessions s
      ON e.client_id = s.client_id
     AND e.timestamp >= s.started_at
     AND e.timestamp <= s.ended_at
)
SELECT
    (SELECT COUNT(*) FROM stg_events) AS total_events,
    (SELECT COUNT(*) FROM matched) AS events_assigned_to_sessions,
    (SELECT COUNT(*) FROM stg_events) - (SELECT COUNT(*) FROM matched) AS unassigned_events
""".strip(),
        ),
        SqlCheck(
            name="Q2.3.b actions_per_session mismatch",
            pass_condition="zero_rows",
            sql="""
WITH actual AS (
    SELECT
        s.session_id,
        COUNT(*) AS actual_events
    FROM dim_sessions s
    JOIN stg_events e
      ON e.client_id = s.client_id
     AND e.timestamp >= s.started_at
     AND e.timestamp <= s.ended_at
    GROUP BY 1
)
SELECT
    s.session_id,
    s.actions_per_session,
    a.actual_events,
    (s.actions_per_session - a.actual_events) AS diff
FROM dim_sessions s
JOIN actual a USING(session_id)
WHERE s.actions_per_session <> a.actual_events
ORDER BY ABS(s.actions_per_session - a.actual_events) DESC
""".strip(),
        ),
        SqlCheck(
            name="Q2.4.a attribution coverage",
            pass_condition="summary",
            sql="""
SELECT
    COUNT(*) AS total_purchases,
    SUM(CASE WHEN lc_session_id IS NOT NULL THEN 1 ELSE 0 END) AS last_click_attributed,
    SUM(CASE WHEN fc_session_id IS NOT NULL THEN 1 ELSE 0 END) AS first_click_attributed,
    SUM(CASE WHEN lc_session_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_unattributed_lc
FROM fct_attribution
""".strip(),
        ),
        SqlCheck(
            name="Q2.4.b lookback >7 days",
            pass_condition="zero_rows",
            sql="""
WITH joined AS (
    SELECT
        a.transaction_id,
        a.purchase_time,
        a.lc_session_id,
        s.started_at AS lc_started_at,
        date_diff('day', s.started_at, a.purchase_time) AS days_before_purchase
    FROM fct_attribution a
    JOIN dim_sessions s
      ON a.lc_session_id = s.session_id
)
SELECT *
FROM joined
WHERE days_before_purchase > 7
ORDER BY days_before_purchase DESC
""".strip(),
        ),
        SqlCheck(
            name="Q2.4.c fc vs lc difference rate",
            pass_condition="summary",
            sql="""
SELECT
    COUNT(*) AS total_attributed,
    SUM(CASE WHEN fc_session_id = lc_session_id THEN 1 ELSE 0 END) AS same_session_fc_lc,
    SUM(CASE WHEN fc_session_id <> lc_session_id THEN 1 ELSE 0 END) AS different_session_fc_lc,
    SUM(CASE WHEN fc_session_id <> lc_session_id THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_different
FROM fct_attribution
WHERE fc_session_id IS NOT NULL AND lc_session_id IS NOT NULL
""".strip(),
        ),
        SqlCheck(
            name="Q2.5.a duplicate raw checkout_completed (diagnostic)",
            pass_condition="diagnostic_dupes",
            sql="""
SELECT
    transaction_id,
    client_id,
    COUNT(*) AS raw_checkout_completed_events,
    MIN(timestamp) AS first_seen,
    MAX(timestamp) AS last_seen
FROM stg_events
WHERE event_name = 'checkout_completed'
  AND transaction_id IS NOT NULL
GROUP BY 1,2
HAVING COUNT(*) > 1
ORDER BY raw_checkout_completed_events DESC
""".strip(),
        ),
        SqlCheck(
            name="Q2.5.b revenue discrepancy >0.01",
            pass_condition="zero_rows",
            sql="""
SELECT 
    transaction_id, 
    MAX(declared_order_revenue) as declared_total, 
    SUM(line_total) as calculated_total,
    (MAX(declared_order_revenue) - SUM(line_total)) as discrepancy
FROM fct_order_items
GROUP BY transaction_id
HAVING ABS(MAX(declared_order_revenue) - SUM(line_total)) > 0.01
ORDER BY discrepancy DESC
""".strip(),
        ),
    ]

    results: list[tuple[str, str, str]] = []
    must_pass_failed = False

    print("\nValidation Checks\n-----------------")
    for check in checks:
        try:
            if check.pass_condition == "summary":
                headers, rows = _fetch_table(conn, check.sql)
                print(f"\n[PASS] {check.name}")
                details = "ok"
                if rows:
                    print(_format_table(headers, rows[:1]))
                    details = "1 row"
                results.append((check.name, "PASS", details))

            elif check.pass_condition == "event_coverage":
                headers, rows = _fetch_table(conn, check.sql)
                unassigned = None
                if rows and "unassigned_events" in headers:
                    unassigned = rows[0][headers.index("unassigned_events")]
                status = "PASS" if (unassigned == 0) else "FAIL"
                print(f"\n[{status}] {check.name}")
                if rows:
                    print(_format_table(headers, rows[:1]))
                details = f"unassigned_events={unassigned}"
                results.append((check.name, status, details))
                if status == "FAIL":
                    must_pass_failed = True

            elif check.pass_condition == "diagnostic_dupes":
                headers, rows = _fetch_table(conn, check.sql)
                print(f"\n[PASS] {check.name}")
                print(_format_table(headers, rows[:10]) if rows else "(no duplicates)")
                results.append((check.name, "PASS", f"duplicate_pairs={len(rows)}"))

            else:  # zero_rows
                headers, rows = _fetch_table(conn, check.sql)
                rowcount = len(rows)
                status = "PASS" if rowcount == 0 else "FAIL"
                print(f"\n[{status}] {check.name}")
                results.append((check.name, status, f"rowcount={rowcount}"))
                if status == "FAIL":
                    must_pass_failed = True
                    if rows:
                        print(_format_table(headers, rows[:10]))

        except Exception as exc:
            status = "FAIL"
            print(f"\n[{status}] {check.name}\nerror={type(exc).__name__}: {exc}")
            results.append((check.name, status, f"error={type(exc).__name__}"))
            if check.pass_condition != "diagnostic_dupes":
                must_pass_failed = True

    print("\nCheck Summary\n-------------")
    print(_format_table(["check_name", "status", "details"], results))
    return 1 if must_pass_failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run DuckDB transformations from schema.sql")
    parser.add_argument(
        "--db",
        default=str(_project_root() / "data" / "processed" / "puffy_main.db"),
        help="Path to DuckDB database file",
    )
    parser.add_argument(
        "--sql",
        default=str(_project_root() / "part2-transformation" / "schema.sql"),
        help="Path to transformation SQL file",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    sql_path = Path(args.sql)

    sql_text = sql_path.read_text(encoding="utf-8")
    declared = _declared_objects(sql_text)

    conn = duckdb.connect(str(db_path))
    _execute_sql_script(conn, sql_text)

    if declared:
        print("Transformation Complete. Created/updated: " + ", ".join(declared) + ".")
    else:
        print("Transformation Complete.")

    exit_code = _run_checks(conn)
    conn.close()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
