# Data Dictionary

## Dataset Overview

- 14 days of e-commerce event data (Feb 23 - Mar 8, 2025)

- Date-partitioned CSV files (one per day)

- \~2,000-2,500 events per day

## Column Definitions

**client_id** (string)

- Cookie-based device identifier

- Same user on different devices/browsers = different client_ids

**page_url** (string)

- Full URL where event occurred

- May contain query parameters (utm_source, utm_medium, utm_campaign,
  > etc.)

- Query parameter values are anonymized/hashed

**referrer** (string, nullable)

- URL of the previous page

- Null/empty = direct traffic or tracking limitation

- Domains other than puffy.com/google.com/bing.com are anonymized

**timestamp** (string, ISO 8601)

- Event timestamp in UTC

- Format: YYYY-MM-DDTHH:MM:SS.sssZ

**event_name** (string)

- Event type identifier

- Values: page_viewed, email_filled_on_popup, product_added_to_cart,
  > checkout_started, purchase

**event_data** (string, JSON)

- Event-specific parameters as JSON string

- Structure varies by event_name

**user_agent** (string)

- Browser/device identification string

- Parse to extract device type, browser, OS
