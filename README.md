
# Analytics Assessment:

## 📋 Executive Summary
This repository contains a reproducible, containerized ELT pipeline designed to ingest, validate, transform, and monitor e-commerce clickstream data.

The system focuses on **Data Quality First**. It rejects bad data at the door (schema-on-read validation), transforms raw events into business-ready dimensional models (Sessionization & Attribution), and includes a "Red Alert" monitoring system that halts the pipeline if critical thresholds are breached.

---

## Architecture
* **Ingestion:** Python + **Polars** (High-performance CSV parsing) + Pydantic (Strict Schema Validation)
* **Storage & Compute:** DuckDB (In-process SQL OLAP)
* **Transformation:** SQL (Window Functions for Sessionization)
* **Orchestration:** Functional Python Scripts
* **Monitoring:** Automated Data Unit Tests

---

## Quick Start (Docker)
The entire pipeline is containerized for 100% reproducibility.

**1. Build the Image**

```bash
docker build -t puffy-analytics .
```

Place raw CSV files in `data/raw/` with headers:
`client_id, page_url, referrer, timestamp, event_name, event_data, user_agent`

* **Data Quality validator**

Run the validator:
```bash
python part1-data-quality/validator.py
```
Invalid rows are written per-file to `data/quarantine/<file>_errors.csv`.

* **Ingestion (DuckDB)**

Loads validated events into DuckDB at `data/processed/puffy_main.db` and writes invalid rows to `data/quarantine/<file>_errors.csv`.
```bash
python part1-data-quality/ingest.py
```
Audit trail is written to the `pipeline_logs` table inside DuckDB.

* **Transformation (DuckDB SQL)**

Builds the transformed tables/views defined in `part2-transformation/schema.sql` and runs post-transform validation checks.
```bash
python part2-transformation/transform.py
```
Optional: point to a different DB/SQL file:

```bash
python part2-transformation/transform.py --db data/processed/puffy_main.db --sql part2-transformation/schema.sql
```
* **Analysis**

Generates a Markdown report at `part3-analysis/findings.md` by running the assignment SQL queries against DuckDB.
```bash
python part3-analysis/analysis.py
```
* **Production Monitoring**

Runs post-pipeline data health checks (fails with exit code `1` if any check fails).
```bash
python part4-monitoring/monitor.py
```

**2. Run the Full Pipeline**

You can run the entire sequence inside the container:

```bash
docker run --rm -v "$PWD:/app" puffy-analytics bash -c "
  python part1-data-quality/ingest.py && \
  python part2-transformation/transform.py && \
  python part3-analysis/analysis.py && \
  python part4-monitoring/monitor.py"
```

## Project Components
### 1. The Gatekeeper (Ingestion & Quality)
**Goal:** Prevent "Garbage In, Garbage Out."

* **Optimization:** Uses **Polars** to accelerate the reading and parsing of raw CSV files, providing a significant speedup over standard Python libraries before handing data off to the validator.
* **Logic:** Every row is validated against a strict `Pydantic` model.
* **Outcome:**
* **Valid Data:** Loaded into `raw_events` table in DuckDB.
* **Invalid Data:** Quarantined to `data/quarantine/` with an `error_reason`.
* **Audit:** A `pipeline_logs` table tracks error rates per file.


* **Observation:** During the assessment, this gatekeeper correctly identified and rejected **~5.8%** of rows due to missing `client_id`s.

```bash
python part1-data-quality/ingest.py
```

### 2. The Engine (Transformation)
**Goal:** Turn "Clicks" into "Sessions" and "Orders."
* **Tech:** Pure DuckDB SQL.
* **Key Logic:**
* **Sessionization:** 30-minute inactivity window using `LAG()` window functions.
* **Attribution:** First-Touch vs. Last-Touch modeling.
* **Revenue Extraction:** JSON parsing of the `event_data` blob.

```bash
python part2-transformation/transform.py
```

### 3. The Insight (Analysis)
**Goal:** Answer the business questions.
* **Output:** Generates `part3-analysis/findings.md`.
* **Key Metrics:** Daily Trends, Funnel Conversion, and Channel Attribution (First vs. Last Click).
```bash
python part3-analysis/analysis.py
```

### 4. The Watchdog (Production Monitoring)
**Goal:** Automated Defense in Depth.
* **Logic:** Runs post-pipeline to verify "Business Reality."
* **Checks:**
* Is the data fresh?
* Is the error rate < 5%?
* Is revenue > $0?
* **Status:** The current run exits with **Code 1 (Failure)** because the error rate (5.8%) exceeded our strict safety threshold (5.0%). **This is intentional behavior** to prevent silent data corruption.

```bash
python part4-monitoring/monitor.py
```


## Design Decisions
| Decision | Why? |
| --- | --- |
| **Polars** | Chosen for the ingestion layer to handle large CSV reads significantly faster than Pandas or the standard library, optimizing the I/O bottleneck. |
| **DuckDB** | Faster than Pandas for aggregation, handles larger-than-memory datasets, and supports full SQL window functions for complex sessionization. |
| **Pydantic** | Provides robust, strictly typed validation that is self-documenting and easy to maintain. |
| **Quarantine Pattern** | Instead of crashing on bad data, we segregate it. This allows the business to keep running while engineers investigate the "bad" pile. |
| **Strict Monitoring** | We chose to fail the pipeline on a 5% error rate. In a financial context, missing 5% of orders is unacceptable and requires human intervention. |

---