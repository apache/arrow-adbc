#!/usr/bin/env python3

"""Generate exhaustive PostgreSQL NUMERIC round-trip fixtures.

This intentionally avoids property-testing dependencies for fixture generation.
For one small precision we can enumerate the entire coefficient domain directly
and check the generated corpus into the repository.
"""

from __future__ import annotations

import csv
from pathlib import Path

OUTPUT = Path(__file__).with_name("numeric_roundtrip_fixtures.csv")
PRECISION = 4


def format_numeric_text(coefficient: int, scale: int) -> str:
    """Match PostgreSQL NUMERIC text formatting for these generated values."""
    if coefficient == 0:
        return "0"

    sign = "-" if coefficient < 0 else ""
    digits = str(abs(coefficient))

    if scale <= 0:
        return sign + digits + ("0" * (-scale))

    if len(digits) <= scale:
        digits = digits.rjust(scale + 1, "0")

    split = len(digits) - scale
    integer_part = digits[:split]
    fractional_part = digits[split:].rstrip("0")

    if not fractional_part:
        return sign + integer_part

    return f"{sign}{integer_part}.{fractional_part}"


def iter_rows(precision: int):
    limit = (10**precision) - 1
    for scale in range(-precision, precision + 1):
        for coefficient in range(-limit, limit + 1):
            yield (
                precision,
                scale,
                coefficient,
                format_numeric_text(coefficient, scale),
            )


def write_rows(output: Path, precision: int) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)

    row_count = 0
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("precision", "scale", "coefficient", "expected"))
        for row in iter_rows(precision):
            writer.writerow(row)
            row_count += 1

    return row_count


def main() -> int:
    row_count = write_rows(OUTPUT, PRECISION)
    print(f"Wrote {row_count} rows to {OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
