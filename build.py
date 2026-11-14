#!/usr/bin/env python3
"""Build one OpenAI product feed per product from products.yaml."""

from __future__ import annotations

import csv
import gzip
import json
import re
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "products.yaml"
FORBIDDEN = ("—", "–", "inside your perimeter")
STREAMING_ALONE = re.compile(r"(?<![A-Za-z])streaming(?![A-Za-z])", re.I)

PACKAGE_LABELS = {
    "SUPPORT": "Support",
    "OPERATED": "Operated on IBM Cloud",
    "MANAGED": "Managed on IBM Cloud",
    "ENTERPRISE": "Enterprise",
}

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


def check_copy(label: str, text: str, *, ban_lascaris: bool = False) -> None:
    lower = text.lower()
    for token in FORBIDDEN:
        if token.lower() in lower or token in text:
            raise SystemExit(f"{label}: forbidden wording found: {token!r}")
    if "iren" in lower:
        raise SystemExit(f"{label}: that name must not appear in the feed")
    if STREAMING_ALONE.search(text):
        raise SystemExit(f"{label}: say Kafka, not 'streaming' on its own")
    if ban_lascaris and "lascaris" in lower:
        raise SystemExit(f"{label}: this feed must not mention Lascaris")


def load_source() -> dict:
    with SOURCE.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not data or "products" not in data or "seller" not in data:
        raise SystemExit("products.yaml must define seller and products")
    return data


def offer_has_prices(offer: dict) -> bool:
    prices = offer.get("prices") or {}
    return bool(prices)


def product_is_complete(product: dict) -> bool:
    offers = product.get("offers") or []
    return bool(offers) and all(offer_has_prices(offer) for offer in offers)


def build_rows(product: dict, seller: dict, defaults: dict) -> list[dict]:
    product_id = product["id"]
    ban_lascaris = product_id == "KAFSHIELD"
    rows: list[dict] = []
    offers = [offer for offer in (product.get("offers") or [])]
    complete = product_is_complete(product)
    use_variants = complete and sum(len(o.get("prices") or {}) for o in offers) > 1

    for offer in offers:
        prices = offer.get("prices") or {}
        if complete:
            currency_prices = list(prices.items())
        else:
            currency_prices = [("USD", None)] if not prices else list(prices.items())

        for currency, price in currency_prices:
            currency = str(currency).upper()
            if complete:
                item_id = f"{product_id}-{offer['id']}-{currency}"
                title = f"{offer['title']} ({currency})"
            else:
                item_id = f"{product_id}-{offer['id']}"
                title = offer["title"]
            description = " ".join(str(offer["description"]).split())
            extra = (offer.get("extra_storage") or {}).get(currency)
            if extra:
                description = f"{description} Additional storage is {extra} per TB per year."
            check_copy(f"{item_id} title", title, ban_lascaris=ban_lascaris)
            check_copy(f"{item_id} description", description, ban_lascaris=ban_lascaris)
            if len(title) > 150:
                raise SystemExit(f"{item_id}: title is {len(title)} characters")
            if len(description) > 5000:
                raise SystemExit(f"{item_id}: description is {len(description)} characters")

            package_key = PACKAGE_LABELS.get(
                offer["id"], offer["id"].replace("-", " ").replace("_", " ").title()
            )
            row = {
                "item_id": item_id,
                "group_id": product_id,
                "title": title,
                "description": description,
                "url": offer.get("url") or product["url"],
                "brand": seller["brand"],
                "seller_name": seller["seller_name"],
                "image_url": product["image_url"],
                "availability": defaults.get("availability", "unknown"),
                "target_countries": list(seller["target_countries"]),
                "store_country": seller["store_country"],
                "product_category": product.get("product_category")
                or defaults.get("product_category", ""),
                "seller_url": seller["seller_url"],
                "seller_privacy_policy": seller["seller_privacy_policy"],
                "seller_tos": seller["seller_tos"],
                "is_eligible_search": bool(defaults.get("is_eligible_search", True)),
                "is_eligible_checkout": bool(defaults.get("is_eligible_checkout", False)),
            }
            if price:
                row["price"] = price
                row["offer_id"] = item_id
            if use_variants:
                row["listing_has_variations"] = True
                row["variant_dict"] = {"currency": currency, "package": package_key}
            rows.append(row)

    ids = [row["item_id"] for row in rows]
    if len(ids) != len(set(ids)):
        raise SystemExit(f"{product_id}: duplicate item_id values")
    return rows


def fields_for(rows: list[dict]) -> list[str]:
    used = []
    for key in FEED_FIELDS:
        if any(key in row for row in rows):
            used.append(key)
    return used


def to_csv_row(row: dict, fields: list[str]) -> dict:
    out = {}
    for key in fields:
        if key not in row:
            continue
        value = row[key]
        if key in {"variant_dict", "target_countries"}:
            out[key] = json.dumps(value, separators=(",", ":"), sort_keys=(key == "variant_dict"))
        elif isinstance(value, bool):
            out[key] = "true" if value else "false"
        else:
            out[key] = value
    return out


def write_csv(rows: list[dict], path: Path) -> None:
    fields = fields_for(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for row in rows:
            writer.writerow(to_csv_row(row, fields))


def write_jsonl_gz(rows: list[dict], path: Path) -> None:
    fields = fields_for(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            obj = {key: row[key] for key in fields if key in row}
            fh.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
            fh.write("\n")


def write_parquet(rows: list[dict], path: Path) -> None:
    fields = fields_for(rows)
    parquet_rows = []
    for row in rows:
        item = {}
        for key in fields:
            if key not in row:
                continue
            value = row[key]
            if key == "variant_dict":
                item[key] = json.dumps(value, separators=(",", ":"), sort_keys=True)
            elif key == "target_countries":
                item[key] = list(value)
            else:
                item[key] = value
        parquet_rows.append(item)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(pd.DataFrame(parquet_rows), preserve_index=False)
    pq.write_table(table, path, compression="zstd")


def write_feed(rows: list[dict], directory: Path) -> None:
    write_csv(rows, directory / "products.csv")
    write_jsonl_gz(rows, directory / "products.jsonl.gz")
    write_parquet(rows, directory / "products.parquet")


def copy_feed(src: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    for name in ("products.csv", "products.parquet", "products.jsonl.gz"):
        shutil.copy2(src / name, dest / name)


def write_index(products_meta: list[dict], root_slug: str, path: Path) -> None:
    # nicer headings
    heading = {
        "LASCARIS": "Lascaris",
        "KAFSHIELD": "KafShield",
        "KEROVAN": "Kerovan",
        "HELIOS": "Helios",
        "TRAINING": "Training",
        "SERVICES": "Services",
    }
    # rebuild headings properly
    blocks = []
    for meta in products_meta:
        slug = meta["slug"]
        rows = meta["rows"]
        complete = meta["complete"]
        note = "" if complete else " This feed has no price yet and is not ready to register."
        items = []
        for row in rows:
            price = row.get("price") or "price missing"
            items.append(
                "<tr>"
                f"<td><code>{row['item_id']}</code></td>"
                f"<td>{row['title']}</td>"
                f"<td>{price}</td>"
                "</tr>"
            )
        root_note = (
            " This is also published at the site root, so an already registered Hosted URL keeps working."
            if slug == root_slug
            else ""
        )
        title = heading.get(meta["id"], meta["id"])
        blocks.append(
            f"<h2>{title}</h2>"
            f"<p>Stable URLs:"
            f" <a href=\"{slug}/products.parquet\">{slug}/products.parquet</a>,"
            f" <a href=\"{slug}/products.csv\">{slug}/products.csv</a>,"
            f" <a href=\"{slug}/products.jsonl.gz\">{slug}/products.jsonl.gz</a>."
            f"{root_note}{note}</p>"
            "<table><thead><tr><th>item_id</th><th>title</th><th>price</th></tr></thead>"
            f"<tbody>{''.join(items)}</tbody></table>"
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Scalytics product feeds</title>
  <style>
    body {{ font-family: Georgia, serif; margin: 2.5rem auto; max-width: 52rem; line-height: 1.45; color: #111; }}
    h1 {{ font-size: 1.6rem; }}
    h2 {{ font-size: 1.2rem; margin-top: 2rem; }}
    a {{ color: #0b57d0; }}
    table {{ border-collapse: collapse; width: 100%; margin: 0.8rem 0 1.4rem; }}
    th, td {{ text-align: left; padding: 0.35rem 0.45rem; border-bottom: 1px solid #ddd; vertical-align: top; }}
    code {{ font-family: ui-monospace, Menlo, monospace; font-size: 0.86rem; }}
    ul {{ padding-left: 1.2rem; }}
  </style>
</head>
<body>
  <h1>Scalytics product feeds</h1>
  <p>These are the public OpenAI product feeds for ChatGPT, built from
  <code>products.yaml</code> in the
  <a href="https://github.com/scalytics/agent-feed">scalytics/agent-feed</a>
  repository. There is one feed per product or catalog. Use a parquet URL
  as the Hosted URL in the OpenAI Business Hub. Parquet is the format
  OpenAI prefers.</p>
  <p>The original root files remain the Lascaris feed:</p>
  <ul>
    <li><a href="products.parquet">products.parquet</a></li>
    <li><a href="products.csv">products.csv</a></li>
    <li><a href="products.jsonl.gz">products.jsonl.gz</a></li>
  </ul>
  {''.join(blocks)}
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")


def main() -> None:
    data = load_source()
    seller = data["seller"]
    defaults = data.get("defaults") or {}
    root_slug = data.get("root_feed") or "lascaris"
    metas = []
    root_dir = None
    for product in data["products"]:
        rows = build_rows(product, seller, defaults)
        if not rows:
            raise SystemExit(f"{product['id']}: no feed rows were produced")
        directory = ROOT / product["slug"]
        write_feed(rows, directory)
        complete = product_is_complete(product)
        metas.append(
            {
                "id": product["id"],
                "slug": product["slug"],
                "rows": rows,
                "complete": complete,
            }
        )
        status = "complete" if complete else "missing price"
        print(f"{product['slug']}: {len(rows)} rows ({status})")
        for row in rows:
            print(f"  {row['item_id']}: {row.get('price') or 'price missing'}")
        if product["slug"] == root_slug:
            root_dir = directory
    if root_dir is None:
        raise SystemExit(f"root_feed {root_slug!r} was not found in products.yaml")
    copy_feed(root_dir, ROOT)
    write_index(metas, root_slug, ROOT / "index.html")
    print(f"copied {root_slug} feed to the site root")


if __name__ == "__main__":
    main()
