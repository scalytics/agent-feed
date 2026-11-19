#!/usr/bin/env python3
"""Validate OpenAI ChatGPT product feeds built from products.yaml.

Discovery required fields, enums and money format follow the OpenAI
product feed schema:
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

import yaml

ROOT = Path(__file__).resolve().parent
# OpenAI discovery requires the nine Basic product data fields plus
# target_countries and store_country (Geo tagging). price is required
# and must be a positive amount; 0 is not valid. availability must be
# one of the enum values below. availability_date is not required for
# pre_order in the current spec. See:
# https://developers.openai.com/commerce/specs/file-upload/products
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
            obj = json.loads(line)
            if not isinstance(obj, dict):
                raise ValueError(f"line {line_no} is not a JSON object")
            rows.append(obj)
    return rows


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


def validate_row(row: dict, label: str, *, require_price: bool, ban_lascaris: bool) -> list[str]:
    errors = []
    required = list(REQUIRED_FIELDS)
    if not require_price:
        required = [field for field in required if field != "price"]
    for field in required:
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
            errors.append(f"{label}: {name} contains a forbidden product name")
        if STREAMING_ALONE.search(text):
            errors.append(f"{label}: {name} says 'streaming' on its own; say Kafka")
        if ban_lascaris and "lascaris" in text.lower():
            errors.append(f"{label}: {name} must not mention Lascaris")

    for url_field in ("url", "image_url", "seller_url", "seller_privacy_policy", "seller_tos"):
        value = row.get(url_field)
        if value in (None, ""):
            continue
        if not is_http_url(str(value)):
            errors.append(f"{label}: {url_field} is not an absolute http(s) URL: {value!r}")

    availability = str(row.get("availability") or "").strip()
    if availability and availability not in AVAILABILITY:
        errors.append(f"{label}: availability {availability!r} is not one of {sorted(AVAILABILITY)}")

    price = str(row.get("price") or "").strip()
    if require_price and price:
        if not MONEY.match(price):
            errors.append(
                f"{label}: price {price!r} must look like '11750.00 USD' (amount space ISO-4217)"
            )
        else:
            amount = float(price.split()[0])
            if amount <= 0:
                errors.append(f"{label}: price must be a positive amount; got {price!r}")
    elif require_price and not price:
        errors.append(f"{label}: required field 'price' is missing or empty")

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
        errors.append(f"{label}: store_country {store!r} must be an uppercase ISO 3166-1 alpha-2 code")

    for flag in ("is_eligible_search", "is_eligible_checkout", "listing_has_variations"):
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
    if size > 20 * 1024 * 1024:
        return [f"{path}: {size} bytes exceeds the 20 MiB upload limit"]
    return []


def validate_files(files: list[Path], *, require_price: bool, ban_lascaris: bool) -> list[str]:
    errors = []
    id_sets = []
    for path in files:
        if not path.exists():
            errors.append(f"{path}: file not found")
            continue
        errors.extend(check_file_size(path))
        try:
            rows = load_any(path)
        except Exception as exc:
            errors.append(f"{path}: failed to read: {exc}")
            continue
        if not rows:
            errors.append(f"{path}: expected at least one product row")
            continue
        ids = []
        variant_keys = []
        for index, row in enumerate(rows, start=1):
            errors.extend(
                validate_row(
                    row,
                    f"{path} row {index}",
                    require_price=require_price,
                    ban_lascaris=ban_lascaris,
                )
            )
            ids.append(str(row.get("item_id") or ""))
            if as_bool(row.get("listing_has_variations")):
                try:
                    variant = parse_variant(row.get("variant_dict") or "{}")
                except Exception:
                    variant = {}
                variant_keys.append((str(row.get("group_id") or ""), tuple(sorted(variant.items()))))
        if len(ids) != len(set(ids)):
            errors.append(f"{path}: duplicate item_id values")
        if len(variant_keys) != len(set(variant_keys)):
            errors.append(f"{path}: duplicate variant_dict values inside a group")
        id_sets.append(ids)
    if len(id_sets) > 1 and any(ids != id_sets[0] for ids in id_sets[1:]):
        errors.append("item_id lists differ across the generated files for this feed")
    return errors


def feed_files(directory: Path) -> list[Path]:
    return [
        directory / "products.csv",
        directory / "products.parquet",
        directory / "products.jsonl.gz",
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="*", type=Path)
    parser.add_argument(
        "--allow-missing-price",
        action="store_true",
        help=(
            "Skip the required price check when validating individual files. "
            "OpenAI rejects rows without a positive price, so do not use this "
            "for a feed you will register."
        ),
    )
    args = parser.parse_args()

    errors = []
    if args.files:
        errors.extend(
            validate_files(
                args.files,
                require_price=not args.allow_missing_price,
                ban_lascaris=any("kafshield" in str(path) for path in args.files),
            )
        )
    else:
        source = yaml.safe_load((ROOT / "products.yaml").read_text(encoding="utf-8"))
        root_slug = source.get("root_feed") or "lascaris"
        for product in source["products"]:
            slug = product["slug"]
            offers = product.get("offers") or []
            if not (offers and all(offer.get("prices") for offer in offers)):
                errors.append(
                    f"{slug}: OpenAI requires a positive price on every row; "
                    "add prices in products.yaml. Spec: "
                    "https://developers.openai.com/commerce/specs/file-upload/products"
                )
            errors.extend(
                validate_files(
                    feed_files(ROOT / slug),
                    require_price=True,
                    ban_lascaris=(product["id"] == "KAFSHIELD"),
                )
            )
        # Root feed must be an exact copy of the Lascaris feed files.
        for name in ("products.csv", "products.parquet", "products.jsonl.gz"):
            root = ROOT / name
            original = ROOT / root_slug / name
            if not root.exists() or not original.exists():
                errors.append(f"{root}: missing root copy of the {root_slug} feed")
                continue
            if root.read_bytes() != original.read_bytes():
                errors.append(f"{root}: does not match {original}")
        errors.extend(
            validate_files(
                [ROOT / "products.csv", ROOT / "products.parquet", ROOT / "products.jsonl.gz"],
                require_price=True,
                ban_lascaris=False,
            )
        )

    if errors:
        print("VALIDATION FAILED")
        for err in errors:
            print(f"- {err}")
        return 1
    print("VALIDATION PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
