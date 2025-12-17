from __future__ import annotations

import csv
import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional

try:
    import polars as pl
except ImportError:  # pragma: no cover
    pl = None

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None

from pydantic import BaseModel, ConfigDict, field_validator


class EventName(str, Enum):
    page_viewed = "page_viewed"
    purchase = "purchase"
    checkout_started = "checkout_started"
    checkout_completed = "checkout_completed"
    product_added_to_cart = "product_added_to_cart"
    email_filled_on_popup = "email_filled_on_popup"


class RawEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    client_id: str
    timestamp: datetime
    event_name: EventName
    event_data: dict[str, Any]
    page_url: str
    referrer: Optional[str] = None
    user_agent: str

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalize_timestamp(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("timestamp is null")
        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            raw = value.strip()
            if raw == "":
                raise ValueError("timestamp is empty")

            candidate = raw
            if " " in candidate and "T" not in candidate:
                candidate = candidate.replace(" ", "T", 1)
            if candidate.endswith("Z"):
                candidate = candidate[:-1] + "+00:00"

            try:
                return datetime.fromisoformat(candidate)
            except ValueError:
                if pd is None:
                    raise ValueError("timestamp is not a valid datetime") from None
                parsed = pd.to_datetime(raw, errors="raise")
                return parsed.to_pydatetime()  # type: ignore[no-any-return]

        if isinstance(value, (int, float)):
            if pd is None:
                raise ValueError("timestamp numeric provided but pandas unavailable")
            unit = "ms" if value > 10_000_000_000 else "s"
            parsed = pd.to_datetime(value, unit=unit, errors="raise")
            return parsed.to_pydatetime()  # type: ignore[no-any-return]

        if isinstance(value, bool):
            raise ValueError("timestamp is not a valid datetime")

        return value

    @field_validator("client_id", "page_url", "user_agent")
    @classmethod
    def non_empty_string(cls, value: Any) -> str:
        if value is None:
            raise ValueError("required field is null")
        if not isinstance(value, str):
            value = str(value)
        value = value.strip()
        if value == "":
            raise ValueError("required field is empty")
        return value

    @field_validator("event_data", mode="before")
    @classmethod
    def parse_event_data(cls, value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            raw = value.strip()
            if raw == "":
                return {}
            if raw.lower() == "null":
                return {}
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"event_data is not valid JSON: {exc.msg}") from exc
            if not isinstance(parsed, dict):
                raise ValueError("event_data JSON must be an object")
            return parsed
        raise ValueError("event_data must be a JSON string or dict")

    @field_validator("event_name", mode="before")
    @classmethod
    def normalize_event_name(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().lower()
        return value


EXPECTED_HEADERS = [
    "client_id",
    "page_url",
    "referrer",
    "timestamp",
    "event_name",
    "event_data",
    "user_agent",
]

CORE_HEADERS = [
    "client_id",
    "page_url",
    "timestamp",
    "event_name",
    "event_data",
    "user_agent",
]

_BOM = "\ufeff"


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def sanitize_header(name: Any) -> str:
    if name is None:
        return ""
    if not isinstance(name, str):
        name = str(name)
    return name.replace(_BOM, "").strip()


def normalize_column_name(name: str) -> str:
    raw = sanitize_header(name)
    lower = raw.lower()

    if lower in {"clientid", "client_id"} or raw == "clientId":
        return "client_id"
    if lower in {"eventname", "event_name"} or raw == "eventName":
        return "event_name"
    if lower in {"eventdata", "event_data"} or raw == "eventData":
        return "event_data"
    if lower in {"pageurl", "page_url"} or raw == "pageUrl":
        return "page_url"
    if lower in {"useragent", "user_agent"} or raw == "userAgent":
        return "user_agent"
    if lower == "referrer":
        return "referrer"
    if lower == "timestamp":
        return "timestamp"
    if lower == "date":
        return "date"
    if lower == "time":
        return "time"

    return raw


def choose_value(existing: Any, candidate: Any) -> Any:
    if existing is None:
        return candidate
    if isinstance(existing, str) and existing.strip() == "":
        return candidate
    return existing


def normalize_row(
    row: dict[str, Any],
    column_map: dict[str, str],
    *,
    derive_timestamp_from_date_time: bool,
) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for src_key, value in row.items():
        norm_key = column_map.get(src_key, normalize_column_name(src_key))
        normalized[norm_key] = choose_value(normalized.get(norm_key), value)

    if derive_timestamp_from_date_time and "timestamp" not in normalized:
        date_part = normalized.get("date")
        time_part = normalized.get("time")
        if date_part and time_part:
            normalized["timestamp"] = f"{date_part} {time_part}"
        elif date_part:
            normalized["timestamp"] = date_part
        elif time_part:
            normalized["timestamp"] = time_part

    return {k: v for k, v in normalized.items() if k in EXPECTED_HEADERS}


def iter_csv_rows(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Reads CSV rows and returns:
      - list of row dicts with keys normalized to EXPECTED_HEADERS
      - a header report {headers, normalized_headers, extra_columns, missing_core}
    """
    headers: list[str] = []
    rows: list[dict[str, Any]] = []

    if pl is not None:
        try:
            df = pl.read_csv(
                path,
                ignore_errors=False,
                infer_schema_length=0,
                null_values=["", "null", "NULL", "None"],
            )
            rename_map = {c: sanitize_header(c) for c in df.columns}
            df = df.rename(rename_map)
            headers = list(df.columns)
            rows = list(df.iter_rows(named=True))
        except Exception:
            rows = []

    if not rows:
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            raw_reader = csv.reader(f)
            try:
                raw_headers = next(raw_reader)
            except StopIteration:
                empty_report = {
                    "headers": [],
                    "normalized_headers": [],
                    "extra_columns": [],
                    "missing_core": CORE_HEADERS,
                }
                return ([], empty_report)

            headers = [sanitize_header(h) for h in raw_headers]
            dict_reader = csv.DictReader(f, fieldnames=headers)
            rows = [dict(row) for row in dict_reader]

    normalized_headers = [normalize_column_name(h) for h in headers]
    has_timestamp = "timestamp" in normalized_headers
    has_date = "date" in normalized_headers
    has_time = "time" in normalized_headers

    derive_timestamp_from_date_time = (not has_timestamp) and (has_date and has_time)

    column_map: dict[str, str] = {h: normalize_column_name(h) for h in headers}
    if (not has_timestamp) and has_date and not has_time:
        column_map = {k: ("timestamp" if v == "date" else v) for k, v in column_map.items()}
        normalized_headers = ["timestamp" if h == "date" else h for h in normalized_headers]
    elif (not has_timestamp) and has_time and not has_date:
        column_map = {k: ("timestamp" if v == "time" else v) for k, v in column_map.items()}
        normalized_headers = ["timestamp" if h == "time" else h for h in normalized_headers]

    normalized_rows = [
        normalize_row(row, column_map, derive_timestamp_from_date_time=derive_timestamp_from_date_time)
        for row in rows
    ]

    effective_header_set = set(normalized_headers)
    if derive_timestamp_from_date_time:
        effective_header_set.add("timestamp")
        effective_header_set.discard("date")
        effective_header_set.discard("time")

    extra_columns = sorted([c for c in effective_header_set if c not in EXPECTED_HEADERS])
    missing_core = sorted([c for c in CORE_HEADERS if c not in effective_header_set])

    return (
        normalized_rows,
        {
            "headers": headers,
            "normalized_headers": sorted(effective_header_set),
            "extra_columns": extra_columns,
            "missing_core": missing_core,
        },
    )

