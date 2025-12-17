# Data Transformation Documentation
## 1. Methodology & Architecture
The solution follows a multi-layered **Medallion Architecture** designed to transform raw event logs into trusted, analytics-ready tables. The pipeline progresses from cleaning to enrichment, and finally to business logic application.

### Pipeline Layers:
1. **Bronze (Raw):** `raw_events`
* Ingestion point containing the immutable validated data from PART1.

2. **Silver (Cleaned & Enriched):** `stg_events`, `dim_user_map`, `fct_order_items`
* **Trust Layer:** JSON payload in event_data is parsed, and `transaction_id` is collected but not used because collisions.
* **Identity Layer:** A `dim_user_map` is created to link anonymous cookies (`client_id`) to stable user identities (`master_user_id`) using captured annonimised emails.
* **Context Layer:** User agents are parsed into readable Device/OS/Browser fields, and URLs are parsed for UTM parameters.

3. **Gold (Aggregated & Business Logic):** `dim_sessions`, `fct_attribution`
* **Sessionization:** Events are grouped into visits to calculate engagement metrics.
* **Attribution:** Conversions are linked back to historical sessions to assign marketing credit.


## 2. Key Definitions & Business Logic

### A. Defining "Users" (Identity Stitching) I try to treat "User Identity" as fluid rather than static.

* **The Problem:** Users browse on multiple devices (Mobile, Desktop), generating different `client_id` cookies.
* **The Solution:** I implemented **Identity Stitching**. If a user ever identifies themselves (via `email_filled_on_popup` or purchase), I map their `client_id` to that email. The logic would need to improved for production (maybe by using data from the User agents)
* **Result:** All historical anonymous activity on that device is retroactively attributed to the "Real Person" (`master_user_id`), allowing for Cross-Device Attribution.

### B. Defining "Sessions"A session is defined as a continuous period of user activity.

* **Time Logic:** A standard **30-minute inactivity window** is used (This is pretty common, we can consider other values later). If a user is inactive for 30+ minutes, their next event triggers a new session ID.
* **Source Logic:** The traffic source (e.g., "5AD0EB") of the **first event** in the session defines the source for the *entire* session.

### C. Defining Attribution
I implemented a **7-Day Lookback Window** with support for both First-Click and Last-Click models.

* **Lookback Constraint:** A purchase is only attributed to sessions that started within 7 days prior to the transaction meaning the `checkout_completed` event.
* **Model Definitions:**
* **Last-Click:** 100% credit goes to the *most recent* session start time relative to the purchase.
* **First-Click:** 100% credit goes to the *earliest* session start time within the 7-day window.

* **Source "Waterfall" Logic:** To accurately classify traffic, I prioritize signals in this order:
1. **UTM Parameters** (Highest Priority - Explicit Tracking)
2. **Organic Search Referrers** (Google/Bing/Yahoo)
3. **Direct** (Fallback)

## 3. Trade-offs & Design Decisions
### 1. Infrastructure: DuckDB as the Warehouse
* **Decision:** We utilized **DuckDB** as the analytical engine rather than a traditional cloud warehouse (Snowflake/BigQuery) or a standard transactional DB (Postgres).
* **Why:** DuckDB's vectorized query execution engine allowed for extremely fast transformations on the local file set without the overhead of network latency or complex infrastructure setup. It is optimized specifically for OLAP (Online Analytical Processing) workloads like this attribution pipeline.
* **Trade-off:** While DuckDB offers superior performance for this scale of data, it runs in-process. For a production environment with terabytes of streaming data, we would migrate this logic to a distributed system, though the SQL dialect would remain largely compatible.

### 2. Session Granularity: Multiple Conversions
* **Observation:** We identified **4 sessions** that contained more than one `checkout_completed` event.
* **Decision:** We chose **not** to force a new session for every purchase. A session remains defined strictly by time (30-minute inactivity).
* **Why:** If a user buys an item, continues browsing, and buys again 10 minutes later, this is behaviorally a single "visit." Splitting it artificially would inflate session counts and dilute engagement metrics.
* **Impact:** Our attribution model credits the *session source* for the total revenue of all purchases within that visit.

### 3. Financial Integrity: Item Price vs. Quantity Discrepancy
* **Observation:** During the revenue reconciliation audit, we found **15 transactions** where the declared `total_revenue` equaled the sum of `item_price` but ignored the `quantity`. (i.e., Revenue = \sum Price instead of \sum Price \times Quantity).
* **Decision:** We defaulted to using the top-level **`event_data.revenue`** for all attribution and reporting.
* **Why:** In e-commerce logging, top-level revenue fields usually come from the payment gateway response (the actual money charged), whereas the `items` array is often constructed by the frontend/datalayer and is more prone to logic errors.
* **Next Step:** These 15 specific transaction IDs should be isolated in an audit table and should be flagged to the engineering team for a frontend bug fix.

### 4. Handling Unreliable Transaction IDs
* **Observation:** The raw data contained duplicate `transaction_id`s for different purchase events.
* **Decision:** We abandoned `transaction_id` as a primary key for deduplication. Instead, we generated a **Surrogate Key** (`purchase_pk`) based on `client_id + timestamp`.
* **Trade-off:** This ensures we capture all revenue even if IDs collide, avoiding data loss at the cost of strict reliance on the source system's IDs.

### 5. Attribution Window & Data Completeness
* **Decision:** We enforced a strict **7-Day Lookback Window** via an `INNER JOIN` between purchases and sessions.
* **Observation:** In this specific 14-day dataset, **100% of revenue was successfully attributed** (Total Revenue in `fct_attribution` matches `stg_events` exactly).
* **Trade-off:** While no data was lost in this sample, this design explicitly chooses to drop revenue from users who have not visited in >7 days rather than attributing it to "Direct/None." This prioritizes the accuracy of marketing models over gross financial reporting in the attribution table.