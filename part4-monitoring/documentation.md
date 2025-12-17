# Part 4: Production Monitoring

## 1. Monitoring Philosophy
Our goal is **"Defense in Depth"**. I monitor the pipeline at three distinct layers—Infrastructure, Data Quality, and Business Logic—to ensure we catch issues before they impact executive dashboards.

The strategy prioritizes **Actionability** over noise. Every alert must require a specific human intervention (e.g., "Check the quarantine folder" or "Investigate marketing spend").

## 2. What We Monitor & Why

I implemented a `DataMonitor` class (`part4-monitoring/monitor.py`) that runs post-pipeline execution. It enforces the following checks:

### A. Infrastructure & Ingestion

* **Metric:** `rows_read > 0` and `log_entry_exists_today`.
* **Why:** Ensures the pipeline actually ran and ingested new files. Dashboards looking "flat" are often just stale data.
* **Detection:** Checks the internal `pipeline_logs` audit table.

### B. Data Quality
* **Metric:** `Quarantine Rate` (Quarantined Rows / Total Rows).
* **Threshold:** > 5% triggers an alert.
* **Why:** A sudden spike in rejected rows usually indicates an upstream schema change (e.g., a new `event_name` or broken JSON) that requires immediate engineering attention.
* **Real-World Result:** During our test run, this monitor **fired correctly**, detecting a **5.82% rejection rate** (2,908 rows missing `client_id`). This confirms the threshold is sensitive enough to catch the "missing identity" bug.

### C. Business Logic

* **Metric 1: Freshness & Volume (Z-Score)**
* **Logic:** Compares today's volume against a 7-day moving average (Z-Score < 3.0).
* **Why:** Detects silent failures (e.g., the tracking pixel stops firing on the checkout page) where data flows but volume collapses.


* **Metric 2: Revenue Reality**
* **Logic:** `pct_missing_revenue < 1%`.
* **Why:** Ensures the critical "Purchase" events actually contain value data.


* **Metric 3: Attribution Coverage**
* **Logic:** `orphan_orders == 0` (Orders with no `session_id`).
* **Why:** Prevents "Dark Revenue" that cannot be attributed to marketing spend.

## 3. Detection Methodology

* **Implementation:** The `monitor.py` script acts as a unit test for the data.
* **Status Codes:**
* **Exit Code 0 (Green):** All systems go.
* **Exit Code 1 (Red):** Critical failure. In a production environment (Airflow/GitHub Actions), this strictly blocks downstream dashboard refreshes to prevent executives from making decisions on bad data.

## 4. Practicality & Operations

The system is designed to avoid having too man Alerts:
* **Automated Context:** The alert message explicitly states *why* it failed (e.g., `rate=0.0582 (max 0.05)`), allowing the on-call engineer to triage immediately without digging into logs.
* **Sensible Thresholds:** We allow a 5% margin of error for clickstream noise (bots, malformed packets) but stop hard at 6%, striking a balance between data purity and operational stability.