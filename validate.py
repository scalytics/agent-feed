#!/usr/bin/env python3
"""Validate an OpenAI ChatGPT product feed (CSV, Parquet, or JSONL.GZ).

Required discovery fields and enums are from
https://developers.openai.com/commerce/specs/file-upload/products
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

REQUIRED_FIELDS = [
    "item_id",
    "title",
    "description",
    "url",
    "brand",
    "seller_name",
    "image_url",
    "availability",
    "price",
    "target_countries",
    "store_country",
]

AVAILABILITY = {"in_stock", "out_of_stock", "pre_order", "backorder", "unknown"}
ISO2 = re.compile(r"^[A-Z]{2}$")
MONEY = re.compile(r"^[0-9]+(?:\.[0-9]+)? [A-Z]{3}$")
BOOL_STR = {"true", "false"}
FORBIDDEN = ("—", "–", "inside your perimeter")
STREAMING_ALONE = re.compile(r"(?<![A-Za-z])streaming(?![A-Za-z])", re.I)


def is_http_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def parse_countries(value):
    if isinstance(value, list):
        return [str(x) for x in value]
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("["):
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError("target_countries JSON must be an array")
        return [str(x) for x in data]
    return [text]


def parse_variant(value):
    if isinstance(value, dict):
        return {str(k): str(v) for k, v in value.items()}
    text = str(value).strip()
    if not text:
        return {}
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("variant_dict must be a JSON object")
    return {str(k): str(v) for k, v in data.items()}


def as_bool(value):
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in BOOL_STR:
        return text == "true"
    return None


def load_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        raw = fh.read()
    if raw.startswith("\ufeff"):
        raise ValueError("CSV has a UTF-8 BOM; write plain UTF-8 without a BOM")
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row")
        return list(reader)


def load_parquet(path: Path) -> list[dict]:
    import pandas as pd

    frame = pd.read_parquet(path)
    rows = []
    for record in frame.to_dict(orient="records"):
        clean = {}
        for key, value in record.items():
            if value is None:
                continue
            try:
                if value != value:
                    continue
            except Exception:
                pass
            if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
                try:
                    value = value.tolist()
                except Exception:
                    pass
            clean[str(key)] = value
        rows.append(clean)
    return rows


def load_jsonl_gz(path: Path) -> list[dict]:
    rows = []
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON on line {line_no}: {exc}") from exc
            if not isinstance(obj, dict):
                raise ValueError(f"line {line_no} is not a JSON object")
            rows.append(obj)
    return rows


def validate_row(row: dict, label: str) -> list[str]:
    errors = []
    for field in REQUIRED_FIELDS:
        if not str(row.get(field, "")).strip() and row.get(field) is not False:
            errors.append(f"{label}: required field '{field}' is missing or empty")

    title = str(row.get("title") or "")
    description = str(row.get("description") or "")
    if title and len(title) > 150:
        errors.append(f"{label}: title is {len(title)} characters; limit is 150")
    if description and len(description) > 5000:
        errors.append(f"{label}: description is {len(description)} characters; limit is 5000")
    for text, name in ((title, "title"), (description, "description")):
        for token in FORBIDDEN:
            if token.lower() in text.lower() or token in text:
                errors.append(f"{label}: {name} contains forbidden wording {token!r}")
        if "iren" in text.lower():
            errors.append(f"{label}: {name} must not mention IREN")
        if STREAMING_ALONE.search(text):
            errors.append(f"{label}: {name} says 'streaming' on its own; say Kafka")

    for url_field in (
        "url",
        "image_url",
        "seller_url",
        "seller_privacy_policy",
        "seller_tos",
    ):
        value = row.get(url_field)
        if value in (None, ""):
            continue
        if not is_http_url(str(value)):
            errors.append(f"{label}: {url_field} is not an absolute http(s) URL: {value!r}")

    availability = str(row.get("availability") or "").strip()
    if availability and availability not in AVAILABILITY:
        errors.append(
            f"{label}: availability {availability!r} is not one of {sorted(AVAILABILITY)}"
        )

    price = str(row.get("price") or "").strip()
    if price:
        if not MONEY.match(price):
            errors.append(
                f"{label}: price {price!r} must look like '11750.00 USD' (amount space ISO-4217)"
            )
        else:
            amount = float(price.split()[0])
            if amount <= 0:
                errors.append(f"{label}: price must be a positive amount; got {price!r}")

    if "target_countries" in row and str(row.get("target_countries") or "").strip():
        try:
            countries = parse_countries(row["target_countries"])
        except Exception as exc:
            errors.append(f"{label}: target_countries is not a JSON array of country codes: {exc}")
            countries = []
        if not countries:
            errors.append(f"{label}: target_countries is empty")
        for code in countries:
            if not ISO2.match(str(code)):
                errors.append(
                    f"{label}: target_countries value {code!r} must be an uppercase ISO 3166-1 alpha-2 code"
                )

    store = str(row.get("store_country") or "").strip()
    if store and not ISO2.match(store):
        errors.append(
            f"{label}: store_country {store!r} must be an uppercase ISO 3166-1 alpha-2 code"
        )

    for flag in (
        "is_eligible_search",
        "is_eligible_checkout",
        "listing_has_variations",
    ):
        if row.get(flag) in (None, ""):
            continue
        if as_bool(row[flag]) is None:
            errors.append(f"{label}: {flag} must be true or false, got {row[flag]!r}")

    if as_bool(row.get("listing_has_variations")):
        group_id = str(row.get("group_id") or "").strip()
        item_id = str(row.get("item_id") or "").strip()
        if not group_id:
            errors.append(f"{label}: group_id is required when listing_has_variations is true")
        elif group_id == item_id:
            errors.append(f"{label}: group_id must differ from item_id for variants")
        try:
            variant = parse_variant(row.get("variant_dict") or "{}")
        except Exception as exc:
            errors.append(f"{label}: variant_dict is invalid: {exc}")
            variant = {}
        if not variant:
            errors.append(f"{label}: variant_dict is required when listing_has_variations is true")

    for key, value in row.items():
        if isinstance(value, str) and value.strip().lower() in {"n/a", "null", "none"}:
            errors.append(f"{label}: {key} uses a placeholder string {value!r}; omit the field instead")
        if key != "availability" and isinstance(value, str) and value.strip().lower() == "unknown":
            errors.append(f"{label}: {key} uses 'unknown'; that token is only valid for availability")

    return errors


def check_file_size(path: Path) -> list[str]:
    size = path.stat().st_size
    limit = 20 * 1024 * 1024
    if size > limit:
        return [f"{path}: {size} bytes exceeds the 20 MiB upload limit"]
    return []


def load_any(path: Path) -> list[dict]:
    name = path.name.lower()
    suffix = path.suffix.lower()
    if name.endswith(".jsonl.gz"):
        return load_jsonl_gz(path)
    if suffix == ".csv":
        return load_csv(path)
    if suffix == ".parquet":
        return load_parquet(path)
    raise ValueError(f"unsupported file type: {path}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", type=Path)
    args = parser.parse_args()

    all_errors = []
    seen_ids_by_file = {}
    for path in args.files:
        if not path.exists():
            all_errors.append(f"{path}: file not found")
            continue
        all_errors.extend(check_file_size(path))
        try:
            rows = load_any(path)
        except Exception as exc:
            all_errors.append(f"{path}: failed to read: {exc}")
            continue
        if not rows:
            all_errors.append(f"{path}: expected at least one product row")
        ids = []
        variant_keys = []
        for index, row in enumerate(rows, start=1):
            all_errors.extend(validate_row(row, f"{path} row {index}"))
            item_id = str(row.get("item_id") or "")
            ids.append(item_id)
            if as_bool(row.get("listing_has_variations")):
                try:
                    variant = parse_variant(row.get("variant_dict") or "{}")
                except Exception:
                    variant = {}
                variant_keys.append((str(row.get("group_id") or ""), tuple(sorted(variant.items()))))
        if len(ids) != len(set(ids)):
            all_errors.append(f"{path}: duplicate item_id values")
        if len(variant_keys) != len(set(variant_keys)):
            all_errors.append(f"{path}: duplicate variant_dict values inside a group")
        seen_ids_by_file[str(path)] = ids

    id_sets = list(seen_ids_by_file.values())
    if len(id_sets) > 1 and any(ids != id_sets[0] for ids in id_sets[1:]):
        all_errors.append("item_id lists differ across the generated feed files")

    if all_errors:
        print("VALIDATION FAILED")
        for err in all_errors:
            print(f"- {err}")
        return 1
    print("VALIDATION PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
