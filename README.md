# Scalytics product feed

This repository publishes a product feed for the OpenAI ChatGPT Business
Hub Hosted URL option. The live files are:

- https://scalytics.github.io/agent-feed/products.parquet (preferred)
- https://scalytics.github.io/agent-feed/products.csv
- https://scalytics.github.io/agent-feed/products.jsonl.gz

Paste one of those URLs into the Business Hub when you upload products.
Parquet is the format OpenAI prefers.

## What is in the feed

Each published offer has one row per currency (EUR and USD). The first
row is Lascaris Support in USD, which is the main Lascaris price.

Only facts and prices from scalytics.io are used. A product with no
published price is left out:

- Kerovan has a free pilot from mid November 2026, then a yearly support
  package with no amount listed.
- Helios is priced as a scoped project. The pricing page says "Talk to
  us" and lists no amount.
- The KafShield page at /kafshield is the Lascaris Kafka backup page.
  Pricing has no separate KafShield amount. That backup capability is
  part of Lascaris Support and Operated.

Seller name is Scalytics, Inc. Store country is US.

## How to update

1. Edit `products.yaml`. This file is the source of truth.
2. Install dependencies once: `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`
3. Run `.venv/bin/python build.py`
4. Run `.venv/bin/python validate.py products.csv products.parquet products.jsonl.gz`
5. Commit the YAML and the generated `products.csv`, `products.parquet`,
   `products.jsonl.gz` and `index.html`.
6. Push to `main`. GitHub Actions builds, validates and publishes the
   files to GitHub Pages at the URLs above.

Do not invent prices, ratings, GTINs or stock counts. If an offer has no
real price, leave it out of `products.yaml` and mention it in this
README. Write plain English, do not use em dashes, say Kafka rather than
"streaming" on its own, and never write "inside your perimeter". If you
mention ISO 27001, say it is in progress.
