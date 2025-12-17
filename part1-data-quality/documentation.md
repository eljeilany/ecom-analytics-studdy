
# Data Quality Framework & Ingestion Report

## 1. Objective

The goal of the Part 1 Data Quality framework is to act as a **strict gatekeeper** for the Puffy analytics warehouse. By validating data *before* it enters the transformation layer, we prevent "garbage in, garbage out" scenarios where broken upstream data corrupts business metrics (e.g., revenue, session counts).

## 2. Validation Framework

I implemented a schema-on-read approach using **Pydantic** for rigorous type checking and **DuckDB** for transactional storage.

| Check Type | Validation Rule | Business Justification ("The Why") |
| --- | --- | --- |
| **Identity** | `client_id` must be a non-empty string. | **Critical:** Events without a user identifier cannot be sessionized or attributed. Including them creates "ghost" sessions that skew conversion rates. **Action:** Hard Fail (Quarantine). |
| **Schema Integrity** | Headers must match the spec (normalized). | Upstream systems often introduce "drift" (e.g., changing `client_id` to `clientId`). I normalize these automatically to prevent pipeline fragility. |
| **Timeliness** | `timestamp` must be a valid ISO8601 datetime. | Essential for ordering events. Without a valid time, we cannot calculate "Time on Site" or attribution windows. |
| **Structure** | `event_data` must be parseable JSON. | This field contains revenue and product details. If it is malformed text, we cannot extract `order_value` later. |
| **Taxonomy** | `event_name` must match the Allow List. | Prevents typos (e.g., `Page_View` vs `page_viewed`) from fracturing analytics into separate buckets. |

## 3. Data Audit: Issues Identified & Remediation
During the initial ingestion of the provided dataset, our framework detected significant data quality anomalies.

### A. Critical Failures (Quarantined)
* **Issue:** **Missing `client_id**`
* **Impact:** Approximately **~2,900 rows (approx. 5.8%)** completely lacked a `client_id`.
* **Root Cause:** Likely a tracking pixel failure or a bot that blocked cookies/identifiers.
* **Decision:** **REJECT**. These rows were moved to `data/quarantine/`. I prioritized the accuracy of our "User" metrics over raw volume counts.



### B. Schema Drift (Auto-Corrected)
* **Issue:** **Inconsistent Column Naming**
* **Observation:** Several files used `clientId` (camelCase) while others used `client_id` (snake_case).
* **Decision:** Implemented **Automated Header Normalization**. The pipeline now detects and renames these columns on the fly, ensuring 100% data capture without manual intervention.



### C. Metadata Gaps (Accepted)
* **Issue:** **Null `event_data**`
* **Observation:** Over 90% of rows had `null` or empty strings in the `event_data` field.
* **Initial Status:** The pipeline initially rejected these.
* **Decision:** **ACCEPT**. I adjusted the logic to treat `null` as an empty JSON object `{}`. A page view often has no extra metadata, and rejecting these would have caused a massive undercounting of site traffic.

### D. Taxonomy Gaps (Patched)
* **Issue:** **Undocumented Event: `checkout_completed**`
* **Observation:** The dataset contained an event type `checkout_completed` that was not in the original spec. 
* **Decision:** **PATCH**. I added this to the valid Enum list to ensure we capture successful checkouts, which are critical for the Funnel Analysis in Part 3.



## 4. Conclusion
The pipeline is now stable. It has successfully ingested **~47,000 valid events** into `puffy_main.db` while isolating **~2,900 invalid records** for engineering review. Now we have established a "clean" foundation for the Sessionization and Attribution modeling in Part 2.