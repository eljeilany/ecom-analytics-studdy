from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import duckdb


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _format_md_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    if not headers:
        return ""
    str_rows = [[("" if v is None else str(v)) for v in row] for row in rows]
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in str_rows:
        out.append("| " + " | ".join(row) + " |")
    return "\n".join(out)


def _fetch(conn: duckdb.DuckDBPyConnection, sql: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    cursor = conn.execute(sql)
    headers = [d[0] for d in cursor.description] if cursor.description else []
    rows = cursor.fetchall()
    return headers, rows


@dataclass(frozen=True)
class ReportSection:
    title: str
    sql: str


def main() -> int:
    root = _project_root()
    parser = argparse.ArgumentParser(description="Run analysis queries and write Markdown findings.")
    parser.add_argument(
        "--db",
        default=str(root / "data" / "processed" / "puffy_main.db"),
        help="Path to DuckDB database file",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    out_dir = root / "part3-analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    findings_path = out_dir / "findings.md"

    conn = duckdb.connect(str(db_path))

    sections: list[ReportSection] = [
        ReportSection(
            title="## 1. User Engagement & Behavior",
            sql="""
                SELECT 
                    COUNT(session_id) as total_sessions,
                    AVG(session_duration_minutes) as avg_session_duration_min,
                    AVG(actions_per_session) as avg_actions_per_session,
                    SUM(CASE WHEN actions_per_session = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as bounce_rate_percent,
                    SUM(converted) * 100.0 / COUNT(*) as conversion_rate_percent
                FROM dim_sessions;
                """.strip(),
        ),
        ReportSection(
            title="## 2. Device Usage",
            sql="""
                SELECT 
                    platform,
                    COUNT(session_id) as session_count,
                    AVG(converted) * 100.0 as conversion_rate
                FROM dim_sessions
                GROUP BY 1
                ORDER BY session_count DESC;
                """.strip(),
        ),
        ReportSection(
            title="### 2.1 Device & Browser Usage",
            sql="""
                SELECT 
                    platform,
                    browser,
                    COUNT(session_id) as session_count,
                    AVG(converted) * 100.0 as conversion_rate
                FROM dim_sessions
                GROUP BY 1, 2
                ORDER BY session_count DESC;
                """.strip(),
        ),
        ReportSection(
            title="## 3. Marketing Attribution Overview",
            sql="""
                SELECT 
                    COALESCE(lc_channel, 'Direct/Unattributed') as channel,
                    SUM(revenue) as last_click_revenue,
                    COUNT(DISTINCT transaction_id) as last_click_conversions,
                    SUM(CASE WHEN fc_channel = lc_channel THEN revenue ELSE 0 END) as retained_revenue
                FROM fct_attribution
                GROUP BY 1
                ORDER BY last_click_revenue DESC;
                """.strip(),
        ),
        ReportSection(
            title="## 4. Funnel Drop-off",
            sql="""
                SELECT * FROM rpt_funnel_metrics;
                """.strip(),
        ),
        ReportSection(
            title="## 5. Lead Velocity",
            sql="""
                SELECT 
                    AVG(days_to_convert) as avg_days_to_convert,
                    MEDIAN(days_to_convert) as median_days_to_convert,
                    MIN(days_to_convert) as fastest_conversion,
                    MAX(days_to_convert) as slowest_conversion
                FROM rpt_lead_velocity;
                """.strip(),
        ),
        ReportSection(
            title="## 6. Daily Business KPI Trend",
            sql="""
                WITH daily_sessions AS (
                    SELECT
                        CAST(started_at AS DATE) AS day,
                        COUNT(*) AS sessions,
                        SUM(converted) AS converted_sessions
                    FROM dim_sessions
                    GROUP BY 1
                ),
                daily_purchases AS (
                    SELECT
                        CAST(purchase_time AS DATE) AS day,
                        COUNT(DISTINCT transaction_id) AS orders,
                        SUM(revenue) AS revenue
                    FROM fct_attribution
                    GROUP BY 1
                )
                SELECT
                    s.day,
                    s.sessions,
                    s.converted_sessions,
                    (s.converted_sessions * 100.0 / NULLIF(s.sessions, 0)) AS session_cvr_pct,
                    p.orders,
                    p.revenue,
                    (p.revenue / NULLIF(p.orders, 0)) AS aov
                FROM daily_sessions s
                LEFT JOIN daily_purchases p USING(day)
                ORDER BY s.day;
                """.strip(),
        ),
        ReportSection(
            title="## 7. First Click vs Last Click Revenue",
            sql="""
                SELECT
                model,
                channel,
                revenue,
                orders
                FROM (
                SELECT 
                    'Last Click' AS model,
                    COALESCE(lc_channel, 'Direct/Unattributed') AS channel,
                    SUM(revenue) AS revenue,
                    COUNT(DISTINCT transaction_id) AS orders
                FROM fct_attribution
                GROUP BY 1,2
                UNION ALL
                SELECT 
                    'First Click' AS model,
                    COALESCE(fc_channel, 'Direct/Unattributed') AS channel,
                    SUM(revenue) AS revenue,
                    COUNT(DISTINCT transaction_id) AS orders
                FROM fct_attribution
                GROUP BY 1,2
                ) t
                ORDER BY model, revenue DESC;
                """.strip(),
        ),
        ReportSection(
            title="## 8. New vs Returning Users",
            sql="""
                WITH first_day AS (
                    SELECT
                        master_user_id,
                        MIN(CAST(started_at AS DATE)) AS first_session_day
                    FROM dim_sessions
                    GROUP BY 1
                ),
                tagged AS (
                    SELECT 
                        s.*,
                        CASE 
                            WHEN CAST(s.started_at AS DATE) = f.first_session_day THEN 'New'
                            ELSE 'Returning'
                        END AS user_type
                    FROM dim_sessions s
                    JOIN first_day f USING(master_user_id)
                )
                SELECT
                    user_type,
                    COUNT(*) AS sessions,
                    AVG(converted) * 100.0 AS session_cvr_pct,
                    AVG(actions_per_session) AS avg_actions
                FROM tagged
                GROUP BY 1
                ORDER BY sessions DESC;
                """.strip(),
        ),
    ]

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    md_lines: list[str] = ["# Findings", f"_Generated at: {generated_at}_", ""]

    exit_code = 0
    for section in sections:
        try:
            headers, rows = _fetch(conn, section.sql)
        except Exception as exc:
            exit_code = 1
            md_lines.append(section.title)
            md_lines.append(f"**ERROR:** `{type(exc).__name__}`: {exc}")
            md_lines.append("")
            print(section.title)
            print(f"ERROR: {type(exc).__name__}: {exc}\n")
            continue

        print(section.title)
        print(_format_md_table(headers, rows))
        print("")

        md_lines.append(section.title)
        md_lines.append(_format_md_table(headers, rows))
        md_lines.append("")

    findings_path.write_text("\n".join(md_lines).strip() + "\n", encoding="utf-8")
    conn.close()

    print(f"Findings written to {findings_path}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
