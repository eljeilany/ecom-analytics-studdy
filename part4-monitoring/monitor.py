from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

try:
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = None


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    details: str


class DataMonitor:
    def __init__(self, db_path: Path, project_root: Path) -> None:
        if duckdb is None:
            raise RuntimeError("duckdb is not installed (required to run monitoring)")
        self.db_path = db_path
        self.project_root = project_root
        self.quarantine_dir = project_root / "data" / "quarantine"
        self.conn = duckdb.connect(str(db_path))

    def close(self) -> None:
        self.conn.close()

    def _table_exists(self, table: str) -> bool:
        try:
            self.conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
            return True
        except Exception:
            return False

    def _table_columns(self, table: str) -> set[str]:
        rows = self.conn.execute(f"PRAGMA table_info('{table}')").fetchall()
        return {r[1] for r in rows}

    def _fetchone(self, sql: str) -> tuple[list[str], tuple[Any, ...]] | tuple[list[str], tuple[()]]:
        cursor = self.conn.execute(sql)
        headers = [d[0] for d in cursor.description] if cursor.description else []
        row = cursor.fetchone()
        return headers, (tuple() if row is None else row)

    def _count(self, sql: str) -> int:
        headers, row = self._fetchone(sql)
        if not headers or not row:
            return 0
        return int(row[0])

    # ----------------------------
    # Check A: Infrastructure & Logs
    # ----------------------------
    def check_logs_present_today(self) -> CheckResult:
        if not self._table_exists("pipeline_logs"):
            return CheckResult("A1 pipeline_logs exists", False, "missing table pipeline_logs")

        count_today = self._count("SELECT COUNT(*) FROM pipeline_logs WHERE CAST(run_timestamp AS DATE) = CURRENT_DATE;")
        return CheckResult(
            "A1 logs present today",
            count_today > 0,
            f"logs_today={count_today}",
        )

    def check_quarantine_rate_today(self, max_rate: float = 0.05) -> CheckResult:
        if not self._table_exists("pipeline_logs"):
            return CheckResult("A2 quarantine rate today", False, "missing table pipeline_logs")

        headers, row = self._fetchone(
            """
            SELECT
            SUM(rows_read) AS rows_read,
            SUM(rows_quarantined) AS rows_quarantined,
            SUM(rows_quarantined) * 1.0 / NULLIF(SUM(rows_read), 0) AS quarantine_rate
            FROM pipeline_logs
            WHERE CAST(run_timestamp AS DATE) = CURRENT_DATE;
            """.strip()
        )
        if not row:
            return CheckResult("A2 quarantine rate today", False, "no rows")

        rows_read, rows_quarantined, rate = row[0], row[1], row[2]
        if rows_read is None or int(rows_read) == 0:
            return CheckResult("A2 quarantine rate today", False, "rows_read=0")
        if rate is None:
            return CheckResult("A2 quarantine rate today", False, "rate is NULL")
        passed = float(rate) <= max_rate
        return CheckResult(
            "A2 quarantine rate today",
            passed,
            f"rows_read={int(rows_read)}, rows_quarantined={int(rows_quarantined)}, rate={float(rate):.4f} (max {max_rate:.2f})",
        )

    def check_no_quarantine_files_today(self) -> CheckResult:
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        today = date.today()
        patterns = [today.strftime("%Y%m%d"), today.strftime("%Y-%m-%d")]
        matches: list[str] = []
        for path in self.quarantine_dir.glob("*"):
            name = path.name
            if any(p in name for p in patterns):
                matches.append(name)
        return CheckResult(
            "A3 no quarantine files for today",
            len(matches) == 0,
            "none" if not matches else f"found={matches}",
        )

    # ----------------------------
    # Check B: Business SQL Monitors
    # ----------------------------
    def _events_table(self) -> str:
        if self._table_exists("stg_events"):
            return "stg_events"
        if self._table_exists("raw_events"):
            return "raw_events"
        raise RuntimeError("No events table found (expected stg_events or raw_events)")

    def _orders_table(self) -> tuple[str, str]:
        if self._table_exists("fct_orders"):
            return ("fct_orders", "session_id")
        if self._table_exists("fct_attribution"):
            return ("fct_attribution", "lc_session_id")
        raise RuntimeError("No orders table found (expected fct_orders or fct_attribution)")

    def q41a_freshness(self) -> CheckResult:
        events = self._events_table()
        headers, row = self._fetchone(
            f"""
            SELECT CAST(timestamp AS DATE) AS day, COUNT(*) AS event_count
            FROM {events}
            GROUP BY 1 ORDER BY day DESC LIMIT 1;
            """.strip()
        )
        if not row:
            return CheckResult("Q4.1.a freshness", False, "no data")
        day, event_count = row[0], row[1]
        passed = event_count is not None and int(event_count) > 0
        return CheckResult("Q4.1.a freshness", passed, f"day={day}, event_count={event_count}")

    def q41b_volume_anomaly(self, max_abs_z: float = 3.0) -> CheckResult:
        events = self._events_table()
        headers, row = self._fetchone(
            f"""
            WITH daily AS (
                SELECT CAST(timestamp AS DATE) AS day, COUNT(*) AS events
                FROM {events}
                GROUP BY 1
            ),
            scored AS (
                SELECT day, events,
                    AVG(events) OVER (ORDER BY day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS avg_prev7,
                    STDDEV_POP(events) OVER (ORDER BY day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS sd_prev7
                FROM daily
            )
            SELECT day, (events - avg_prev7) / NULLIF(sd_prev7, 0) AS z_score
            FROM scored ORDER BY day DESC LIMIT 1;
            """.strip()
        )
        if not row:
            return CheckResult("Q4.1.b volume anomaly z-score", False, "no data")

        day, z_score = row[0], row[1]
        if z_score is None:
            return CheckResult("Q4.1.b volume anomaly z-score", True, f"day={day}, z_score=NULL (insufficient history)")

        passed = abs(float(z_score)) < max_abs_z
        return CheckResult("Q4.1.b volume anomaly z-score", passed, f"day={day}, z_score={float(z_score):.3f} (max {max_abs_z})")

    def q42b_missing_revenue_rate(self, max_pct: float = 1.0) -> CheckResult:
        events = self._events_table()
        cols = self._table_columns(events)

        if "revenue" in cols:
            sql = f"""
                SELECT
                SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS pct_missing_revenue
                FROM {events}
                WHERE event_name = 'purchase' OR event_name = 'checkout_completed';
                """.strip()
        else:
            sql = f"""
                SELECT
                SUM(CASE WHEN CAST(event_data AS VARCHAR) NOT LIKE '%value%' AND CAST(event_data AS VARCHAR) NOT LIKE '%price%' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) as pct_missing_revenue
                FROM {events}
                WHERE event_name = 'purchase' OR event_name = 'checkout_completed';
                """.strip()
        headers, row = self._fetchone(sql)
        if not row:
            return CheckResult("Q4.2.b missing revenue rate", False, "no data")
        pct = row[0]
        if pct is None:
            return CheckResult("Q4.2.b missing revenue rate", True, "pct_missing_revenue=NULL (no matching events)")
        passed = float(pct) < max_pct
        return CheckResult("Q4.2.b missing revenue rate", passed, f"pct_missing_revenue={float(pct):.4f} (max {max_pct})")

    def q45a_funnel_health(self, min_engagement_rate: float = 10.0) -> CheckResult:
        if not self._table_exists("dim_sessions"):
            return CheckResult("Q4.5.a funnel health", False, "missing table dim_sessions")

        cols = self._table_columns("dim_sessions")
        day_col = "session_start_at" if "session_start_at" in cols else ("started_at" if "started_at" in cols else None)
        count_col = "event_count" if "event_count" in cols else ("actions_per_session" if "actions_per_session" in cols else None)
        if day_col is None or count_col is None:
            return CheckResult("Q4.5.a funnel health", False, "dim_sessions missing required columns")

        headers, row = self._fetchone(
            f"""
            SELECT
                CAST({day_col} AS DATE) AS day,
                SUM(CASE WHEN {count_col} > 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) as engagement_rate
            FROM dim_sessions
            GROUP BY 1 ORDER BY day DESC LIMIT 1;
            """.strip()
        )
        if not row:
            return CheckResult("Q4.5.a funnel health", False, "no data")
        day, engagement_rate = row[0], row[1]
        if engagement_rate is None:
            return CheckResult("Q4.5.a funnel health", False, f"day={day}, engagement_rate=NULL")
        passed = float(engagement_rate) > min_engagement_rate
        return CheckResult(
            "Q4.5.a funnel health",
            passed,
            f"day={day}, engagement_rate={float(engagement_rate):.2f} (min {min_engagement_rate})",
        )

    def q46a_unattributed_purchase_rate(self) -> CheckResult:
        table, session_col = self._orders_table()
        headers, row = self._fetchone(
            f"""
            SELECT
                COUNT(*) as total_orders,
                SUM(CASE WHEN {session_col} IS NULL THEN 1 ELSE 0 END) as orphan_orders
            FROM {table};
            """.strip()
        )
        if not row:
            return CheckResult("Q4.6.a unattributed purchase rate", False, "no data")
        total_orders, orphan_orders = row[0], row[1]
        if total_orders is None:
            return CheckResult("Q4.6.a unattributed purchase rate", False, "total_orders=NULL")
        passed = int(orphan_orders or 0) == 0
        return CheckResult(
            "Q4.6.a unattributed purchase rate",
            passed,
            f"total_orders={int(total_orders)}, orphan_orders={int(orphan_orders or 0)}",
        )

    def run_all(self) -> tuple[int, list[CheckResult]]:
        failures: list[CheckResult] = []
        results: list[CheckResult] = []

        # Check A
        results.append(self.check_logs_present_today())
        results.append(self.check_quarantine_rate_today())
        results.append(self.check_no_quarantine_files_today())

        # Check B
        results.append(self.q41a_freshness())
        results.append(self.q41b_volume_anomaly())
        results.append(self.q42b_missing_revenue_rate())
        results.append(self.q45a_funnel_health())
        results.append(self.q46a_unattributed_purchase_rate())

        for r in results:
            if not r.passed:
                failures.append(r)

        return (0 if not failures else 1), results


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Production monitoring for Puffy analytics pipeline.")
    parser.add_argument(
        "--db",
        default=str(_project_root() / "data" / "processed" / "puffy_main.db"),
        help="Path to DuckDB database file",
    )
    args = parser.parse_args()

    root = _project_root()
    monitor = DataMonitor(Path(args.db), root)
    try:
        exit_code, results = monitor.run_all()
    finally:
        monitor.close()

    if exit_code == 0:
        print("✅ MONITORING PASSED")
        for r in results:
            print(f"- PASS {r.name}: {r.details}")
        return 0

    failures = [r for r in results if not r.passed]
    print("❌ MONITORING FAILED: " + "; ".join(f"{f.name} ({f.details})" for f in failures))
    for r in results:
        prefix = "PASS" if r.passed else "FAIL"
        print(f"- {prefix} {r.name}: {r.details}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

