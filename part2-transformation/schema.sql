-- Transformation layer (DuckDB SQL)
-- Creates/refreshes staging, dimensions, facts, and reporting views.

-- stg_events: Base staged events with parsed identity, marketing, and device fields.
CREATE OR REPLACE TABLE stg_events AS
SELECT
    -- 1. Core Identifiers
    client_id,
    timestamp,
    event_name,
    page_url,
    referrer,
    user_agent,

    -- 2. Identity & Transaction Fields (For Trust Layer & User Stitching)
    json_extract_path_text(event_data, 'user_email') as user_email,
    -- If transaction_id is missing in JSON, we can leave it null (I found in testing that it's not reliable)
    json_extract_path_text(event_data, 'transaction_id') as transaction_id,
    CAST(json_extract_path_text(event_data, 'revenue') AS DOUBLE) as revenue,
    -- Keep items as raw JSON to "explode" later in fct_order_items
    json_extract(event_data, 'items') as items_json,

    -- 3. Marketing Channel Parsing
    regexp_extract(page_url, 'utm_source=([^&]+)', 1) as utm_source,
    regexp_extract(page_url, 'utm_medium=([^&]+)', 1) as utm_medium,
    regexp_extract(page_url, 'utm_campaign=([^&]+)', 1) as utm_campaign,
    
    CASE 
        WHEN page_url LIKE '%utm_%' THEN 'Paid'
        WHEN referrer IS NULL OR referrer = '' THEN 'Direct'
        WHEN lower(coalesce(referrer, '')) LIKE '%google%'
        OR lower(coalesce(referrer, '')) LIKE '%bing%'
        OR lower(coalesce(referrer, '')) LIKE '%yahoo%' THEN 'Organic Search'
        ELSE 'Referral' 
    END as channel_group,

    -- 4. Device & Platform Parsing (RESTORED)
    -- Platform / Device Type
    CASE 
        WHEN user_agent ILIKE '%mobile%' AND user_agent NOT ILIKE '%ipad%' AND user_agent NOT ILIKE '%tablet%' THEN 'Mobile'
        WHEN user_agent ILIKE '%ipad%' OR user_agent ILIKE '%tablet%' THEN 'Tablet'
        ELSE 'Desktop' 
    END as platform,

    -- Operating System
    CASE 
        WHEN user_agent ILIKE '%android%' THEN 'Android'
        WHEN user_agent ILIKE '%iphone%' THEN 'iOS'
        WHEN user_agent ILIKE '%ipad%' THEN 'iOS'
        WHEN user_agent ILIKE '%mac os%' THEN 'MacOS'
        WHEN user_agent ILIKE '%windows%' THEN 'Windows'
        WHEN user_agent ILIKE '%linux%' THEN 'Linux'
        ELSE 'Other/Unknown'
    END as os,

    -- Browser
    CASE 
        WHEN user_agent ILIKE '%chrome%' AND user_agent NOT ILIKE '%edg%' THEN 'Chrome'
        WHEN user_agent ILIKE '%safari%' AND user_agent NOT ILIKE '%chrome%' AND user_agent NOT ILIKE '%android%' THEN 'Safari'
        WHEN user_agent ILIKE '%firefox%' THEN 'Firefox'
        WHEN user_agent ILIKE '%edg%' THEN 'Edge'
        ELSE 'Other' 
    END as browser

FROM raw_events;

-- ----------------------------


-- dim_user_map: Device (`client_id`) -> stitched user identifier (`master_user_id`) using latest email signal.
CREATE OR REPLACE TABLE  dim_user_map AS
WITH email_signals AS (
    SELECT DISTINCT
        client_id,
        user_email,
        timestamp
    FROM stg_events
    WHERE user_email IS NOT NULL
)
SELECT 
    client_id,
    -- Assign the most recently used email as the "Master ID" for this device
    arg_max(user_email, timestamp) as master_user_id
FROM email_signals
GROUP BY client_id;

-- ----------------------------

-- dim_sessions: Sessionized rollups (30+ minute inactivity = new session) with engagement + channel fields.
CREATE OR REPLACE TABLE dim_sessions AS
WITH session_base AS (
    SELECT 
        s.*,
        -- Join Map to get Master ID
        COALESCE(u.master_user_id, s.client_id) as master_user_id,
        
        LAG(timestamp) OVER (PARTITION BY s.client_id ORDER BY timestamp) as prev_ts,
        CASE WHEN date_diff('minute', prev_ts, timestamp) >= 30 OR prev_ts IS NULL THEN 1 ELSE 0 END as is_new
    FROM stg_events s
    LEFT JOIN dim_user_map u ON s.client_id = u.client_id
),
grouped AS (
    SELECT *, SUM(is_new) OVER (PARTITION BY client_id ORDER BY timestamp) as session_rank
    FROM session_base
)
SELECT 
    md5(client_id || '-' || CAST(session_rank AS VARCHAR)) as session_id,
    master_user_id, -- CRITICAL: Use this for attribution joins
    client_id,
    MIN(timestamp) as started_at,
    MAX(timestamp) as ended_at,
    date_diff('second', MIN(timestamp), MAX(timestamp)) / 60.0 as session_duration_minutes, -- Req #6
    
    
    arg_min(channel_group, timestamp) as channel,
    arg_min(utm_source, timestamp) as source,
    arg_min(utm_medium, timestamp) as medium,
    arg_min(utm_campaign, timestamp) as campaign,
    
    arg_min(platform, timestamp) as platform,
    arg_min(os, timestamp) as os,
    arg_min(browser, timestamp) as browser,
    
    -- ENGAGEMENT METRICS (Counting specific actions)
    COUNT(*) as actions_per_session, -- Req #6
    COUNT(CASE WHEN event_name = 'page_viewed' THEN 1 END) as page_views,
    COUNT(CASE WHEN event_name = 'product_added_to_cart' THEN 1 END) as cart_adds,
    MAX(CASE WHEN event_name = 'checkout_started' THEN 1 ELSE 0 END) as did_checkout,
    MAX(CASE WHEN event_name = 'email_filled_on_popup' THEN 1 ELSE 0 END) as captured_email,
    MAX(CASE WHEN event_name = 'checkout_completed' THEN 1 ELSE 0 END) as converted
FROM grouped
GROUP BY client_id, session_rank, master_user_id;


-- ----------------------------

-- fct_attribution: First/last-click attribution for completed checkouts within a 7-day lookback window.
CREATE OR REPLACE TABLE fct_attribution AS
WITH purchases AS (
    SELECT 
        s.transaction_id, 
        s.client_id,
        s.timestamp as purchase_time, 
        s.revenue,
        -- Generate a unique key for the EVENT
        md5(s.client_id || CAST(s.timestamp AS VARCHAR)) as purchase_pk,
        
        -- Identity Layer
        COALESCE(u.master_user_id, s.client_id) as purchaser_master_id
        
    FROM stg_events s
    LEFT JOIN dim_user_map u ON s.client_id = u.client_id
    WHERE s.event_name = 'checkout_completed'
)
SELECT
    transaction_id,
    purchaser_master_id,
    revenue,
    purchase_time,
    
    -- LAST CLICK ATTRIBUTION
    MAX(CASE WHEN last_click_rank = 1 THEN session_id END) as lc_session_id,
    MAX(CASE WHEN last_click_rank = 1 THEN channel END) as lc_channel,
    MAX(CASE WHEN last_click_rank = 1 THEN source END) as lc_source,
    MAX(CASE WHEN last_click_rank = 1 THEN medium END) as lc_medium,
    
    -- FIRST CLICK ATTRIBUTION
    MAX(CASE WHEN first_click_rank = 1 THEN session_id END) as fc_session_id,
    MAX(CASE WHEN first_click_rank = 1 THEN channel END) as fc_channel,
    MAX(CASE WHEN first_click_rank = 1 THEN source END) as fc_source,
    MAX(CASE WHEN first_click_rank = 1 THEN medium END) as fc_medium

FROM (
    SELECT
        p.transaction_id,
        p.purchaser_master_id,
        p.revenue,
        p.purchase_time,
        p.purchase_pk,
        s.session_id,
        s.channel,
        s.source,
        s.medium,
        
        ROW_NUMBER() OVER (
            PARTITION BY p.purchase_pk 
            ORDER BY s.started_at DESC
        ) as last_click_rank,
        
        ROW_NUMBER() OVER (
            PARTITION BY p.purchase_pk 
            ORDER BY s.started_at ASC
        ) as first_click_rank
        
    FROM purchases p
    INNER JOIN dim_sessions s
        ON p.purchaser_master_id = s.master_user_id
        AND s.started_at <= p.purchase_time
        AND s.started_at >= (p.purchase_time - INTERVAL 7 DAY)
) attribution_window
-- Group by the unique event key (purchase_pk) to prevent merging different orders
GROUP BY purchase_pk, transaction_id, purchaser_master_id, revenue, purchase_time;


-- ----------------------------

-- fct_order_items: Exploded line items from the `items_json` array on checkout completion events.
CREATE OR REPLACE TABLE fct_order_items AS
WITH exploded_items AS (
    SELECT 
        transaction_id,
        timestamp,
        client_id,
        revenue as declared_order_revenue,
        -- DuckDB specific: Unnest the JSON list into rows
        UNNEST(json_transform(items_json, '["JSON"]')) as item_json
    FROM stg_events
    WHERE event_name = 'checkout_completed'
    -- REQUIREMENT #1: Transaction Deduplication (Simple version)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id, client_id ORDER BY timestamp) = 1
)
SELECT 
    transaction_id,
    timestamp,
    client_id,
    declared_order_revenue,
    
    -- Extract Item Details
    json_extract_path_text(item_json, 'item_id') as item_id,
    json_extract_path_text(item_json, 'item_name') as item_name,
    CAST(json_extract_path_text(item_json, 'item_price') AS DOUBLE) as item_price,
    CAST(json_extract_path_text(item_json, 'quantity') AS INTEGER) as quantity,
    
    -- Line Item Total
    (CAST(json_extract_path_text(item_json, 'item_price') AS DOUBLE) * CAST(json_extract_path_text(item_json, 'quantity') AS INTEGER)) as line_total

FROM exploded_items;
-- ----------------------------

-- rpt_lead_velocity: Lead-to-purchase velocity (days between email capture and first purchase).
CREATE OR REPLACE VIEW rpt_lead_velocity AS
with rpt_lead_velocity_cte as (
SELECT 
    u.master_user_id,
    MIN(CASE WHEN event_name = 'email_filled_on_popup' THEN timestamp END) as lead_captured_at,
    MIN(CASE WHEN event_name = 'checkout_completed' THEN timestamp END) as first_purchase_at,
    
    date_diff('day', 
        MIN(CASE WHEN event_name = 'email_filled_on_popup' THEN timestamp END), 
        MIN(CASE WHEN event_name = 'checkout_completed' THEN timestamp END)
    ) as days_to_convert
    
FROM stg_events s
LEFT JOIN dim_user_map u ON s.client_id = u.client_id
GROUP BY u.master_user_id
HAVING lead_captured_at IS NOT NULL AND first_purchase_at IS NOT NULL and days_to_convert>=0
)
select * from rpt_lead_velocity_cte;

-- ----------------------------

-- rpt_funnel_metrics: High-level funnel counts across sessions (view -> cart -> checkout -> purchase).
CREATE OR REPLACE VIEW rpt_funnel_metrics AS
SELECT 
    COUNT(DISTINCT session_id) as total_sessions,
    COUNT(DISTINCT CASE WHEN page_views > 0 THEN session_id END) as step_1_view,
    COUNT(DISTINCT CASE WHEN cart_adds > 0 THEN session_id END) as step_2_cart,
    COUNT(DISTINCT CASE WHEN did_checkout > 0 THEN session_id END) as step_3_checkout,
    COUNT(DISTINCT CASE WHEN converted > 0 THEN session_id END) as step_4_purchase
FROM dim_sessions;
