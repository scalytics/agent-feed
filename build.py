#!/usr/bin/env python3
"""Build the OpenAI product feed from products.yaml."""

from __future__ import annotations

import csv
import gzip
import json
import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "products.yaml"
FORBIDDEN = ("—", "–", "inside your perimeter")
STREAMING_ALONE = re.compile(r"(?<![A-Za-z])streaming(?![A-Za-z])", re.I)

FEED_FIELDS = [
    "item_id",
    "group_id",
    "listing_has_variations",
    "variant_dict",
    "offer_id",
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
    "product_category",
    "seller_url",
    "seller_privacy_policy",
    "seller_tos",
    "is_eligible_search",
    "is_eligible_checkout",
]


def check_copy(label: str, text: str) -> None:
    lower = text.lower()
    for token in FORBIDDEN:
        if token.lower() in lower or token in text:
            raise SystemExit(f"{label}: forbidden wording found: {token!r}")
    if "iren" in lower:
        raise SystemExit(f"{label}: IREN must not appear in the feed")
    if STREAMING_ALONE.search(text):
        raise SystemExit(f"{label}: say Kafka, not 'streaming' on its own")


def load_source() -> dict:
    with SOURCE.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not data or "products" not in data or "seller" not in data:
        raise SystemExit("products.yaml must define seller and products")
    return data


def build_rows(data: dict) -> list[dict]:
    seller = data["seller"]
    defaults = data.get("defaults") or {}
    rows: list[dict] = []
    for product in data["products"]:
        product_id = product["id"]
        for offer in product.get("offers") or []:
            prices = offer.get("prices") or {}
            if not prices:
                continue
            for currency, price in prices.items():
                currency = str(currency).upper()
                item_id = f"{product_id}-{offer['id']}-{currency}"
                title = f"{offer['title']} ({currency})"
                description = " ".join(str(offer["description"]).split())
                extra = (offer.get("extra_storage") or {}).get(currency)
                if extra:
                    description = (
                        f"{description} Additional storage is {extra} per TB per year."
                    )
                check_copy(f"{item_id} title", title)
                check_copy(f"{item_id} description", description)
                if len(title) > 150:
                    raise SystemExit(f"{item_id}: title is {len(title)} characters")
                if len(description) > 5000:
                    raise SystemExit(
                        f"{item_id}: description is {len(description)} characters"
                    )
                package_key = {
                    "SUPPORT": "Support",
                    "OPERATED": "Operated on IBM Cloud",
                }.get(offer["id"], offer["id"].replace("_", " ").title())
                variant = {"currency": currency, "package": package_key}
                row = {
                    "item_id": item_id,
                    "group_id": product_id,
                    "listing_has_variations": True,
                    "variant_dict": variant,
                    "offer_id": item_id,
                    "title": title,
                    "description": description,
                    "url": product["url"],
                    "brand": seller["brand"],
                    "seller_name": seller["seller_name"],
                    "image_url": product["image_url"],
                    "availability": defaults.get("availability", "unknown"),
                    "price": price,
                    "target_countries": list(seller["target_countries"]),
                    "store_country": seller["store_country"],
                    "product_category": defaults.get("product_category", ""),
                    "seller_url": seller["seller_url"],
                    "seller_privacy_policy": seller["seller_privacy_policy"],
                    "seller_tos": seller["seller_tos"],
                    "is_eligible_search": bool(defaults.get("is_eligible_search", True)),
                    "is_eligible_checkout": bool(
                        defaults.get("is_eligible_checkout", False)
                    ),
                }
                rows.append(row)
    if not rows:
        raise SystemExit("no feed rows were produced")
    ids = [row["item_id"] for row in rows]
    if len(ids) != len(set(ids)):
        raise SystemExit("duplicate item_id values")
    return rows


def to_csv_row(row: dict) -> dict:
    out = {}
    for key in FEED_FIELDS:
        value = row[key]
        if key in {"variant_dict"}:
            out[key] = json.dumps(value, separators=(",", ":"), sort_keys=True)
        elif key == "target_countries":
            out[key] = json.dumps(value, separators=(",", ":"))
        elif isinstance(value, bool):
            out[key] = "true" if value else "false"
        else:
            out[key] = value
    return out


def to_jsonl_row(row: dict) -> dict:
    return {key: row[key] for key in FEED_FIELDS}


def write_csv(rows: list[dict], path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FEED_FIELDS, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for row in rows:
            writer.writerow(to_csv_row(row))


def write_jsonl_gz(rows: list[dict], path: Path) -> None:
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(json.dumps(to_jsonl_row(row), ensure_ascii=False, separators=(",", ":")))
            fh.write("\n")


def write_parquet(rows: list[dict], path: Path) -> None:
    # Keep structured fields as JSON strings so CSV, JSONL and Parquet stay aligned.
    frame_rows = [to_csv_row(row) for row in rows]
    # Restore booleans and lists in parquet
    parquet_rows = []
    for src, raw in zip(rows, frame_rows):
        item = dict(raw)
        item["listing_has_variations"] = src["listing_has_variations"]
        item["is_eligible_search"] = src["is_eligible_search"]
        item["is_eligible_checkout"] = src["is_eligible_checkout"]
        item["target_countries"] = list(src["target_countries"])
        item["variant_dict"] = json.dumps(
            src["variant_dict"], separators=(",", ":"), sort_keys=True
        )
        parquet_rows.append(item)
    table = pa.Table.from_pandas(pd.DataFrame(parquet_rows), preserve_index=False)
    pq.write_table(table, path, compression="zstd")


def write_index(rows: list[dict], path: Path) -> None:
    items = []
    for row in rows:
        items.append(
            "<tr>"
            f"<td><code>{row['item_id']}</code></td>"
            f"<td>{row['title']}</td>"
            f"<td>{row['price']}</td>"
            "</tr>"
        )
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Scalytics product feed</title>
  <style>
    body {{ font-family: Georgia, serif; margin: 2.5rem auto; max-width: 46rem; line-height: 1.45; color: #111; }}
    h1 {{ font-size: 1.6rem; }}
    a {{ color: #0b57d0; }}
    table {{ border-collapse: collapse; width: 100%; margin: 1.2rem 0; }}
    th, td {{ text-align: left; padding: 0.4rem 0.5rem; border-bottom: 1px solid #ddd; vertical-align: top; }}
    code, pre {{ font-family: ui-monospace, Menlo, monospace; font-size: 0.9rem; }}
    ul {{ padding-left: 1.2rem; }}
  </style>
</head>
<body>
  <h1>Scalytics product feed</h1>
  <p>This is the public OpenAI product feed for ChatGPT, built from
  <code>products.yaml</code> in the
  <a href="https://github.com/scalytics/agent-feed">scalytics/agent-feed</a>
  repository. Use one of these stable URLs as the Hosted URL in the OpenAI
  Business Hub. Parquet is the preferred format.</p>
  <ul>
    <li><a href="products.parquet">products.parquet</a> (Zstandard)</li>
    <li><a href="products.csv">products.csv</a> (UTF-8)</li>
    <li><a href="products.jsonl.gz">products.jsonl.gz</a></li>
  </ul>
  <table>
    <thead><tr><th>item_id</th><th>title</th><th>price</th></tr></thead>
    <tbody>
      {''.join(items)}
    </tbody>
  </table>
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")


def main() -> None:
    data = load_source()
    rows = build_rows(data)
    write_csv(rows, ROOT / "products.csv")
    write_jsonl_gz(rows, ROOT / "products.jsonl.gz")
    write_parquet(rows, ROOT / "products.parquet")
    write_index(rows, ROOT / "index.html")
    print(f"wrote {len(rows)} rows:")
    for row in rows:
        print(f"  {row['item_id']}: {row['price']}")


if __name__ == "__main__":
    main()
