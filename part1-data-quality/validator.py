from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from shared import EXPECTED_HEADERS, RawEvent, iter_csv_rows, project_root


def main() -> int:
    root = project_root()
    raw_dir = root / "data" / "raw"
    quarantine_dir = root / "data" / "quarantine"
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(raw_dir.glob("*.csv"))

    passed = 0
    failed = 0
    error_counts: Counter[str] = Counter()

    for csv_path in csv_files:
        current_filename = csv_path.name
        file_failures: list[dict[str, Any]] = []
        rows, header_report = iter_csv_rows(csv_path)

        extra_columns = header_report.get("extra_columns", [])
        if extra_columns:
            print(f"WARNING: File {current_filename} has extra columns: {extra_columns}")

        missing_core = header_report.get("missing_core", [])
        if missing_core:
            print(f"CRITICAL WARNING: File {current_filename} is missing core columns: {missing_core}")

        for row in rows:
            try:
                RawEvent.model_validate(row)
                passed += 1
            except ValidationError as exc:
                failed += 1
                failure_row: dict[str, Any] = {k: row.get(k) for k in EXPECTED_HEADERS}
                summarized_errors: list[str] = []
                for err in exc.errors(include_url=False):
                    loc = ".".join(str(p) for p in err.get("loc", ()))
                    msg = str(err.get("msg", "validation error"))
                    summary = f"{loc}: {msg}" if loc else msg
                    summarized_errors.append(summary)
                    error_counts[summary] += 1
                failure_row["error_reason"] = " | ".join(summarized_errors)
                file_failures.append(failure_row)

        if file_failures:
            out_path = quarantine_dir / f"{Path(current_filename).stem}_errors.csv"
            with out_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=[*EXPECTED_HEADERS, "error_reason"])
                writer.writeheader()
                writer.writerows(file_failures)

    print(
        "Checked "
        + str(len(csv_files))
        + " files.\n"
        + str(passed)
        + " rows passed.\n"
        + str(failed)
        + " rows failed (saved to quarantine)."
    )

    if error_counts:
        print("Top 5 validation errors:")
        for message, count in error_counts.most_common(5):
            print(f"- {count}x: {message}")
    else:
        print("Top 5 validation errors:\n- (none)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
