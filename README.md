# Scalytics product feeds

This repository publishes product feeds for the OpenAI ChatGPT Business
Hub Hosted URL option. There is one feed per product or catalog.

Lascaris (also published at the site root, so an already registered URL
keeps working):

- https://scalytics.github.io/agent-feed/products.parquet
- https://scalytics.github.io/agent-feed/lascaris/products.parquet

The other feeds:

- https://scalytics.github.io/agent-feed/kafshield/products.parquet
- https://scalytics.github.io/agent-feed/kerovan/products.parquet
- https://scalytics.github.io/agent-feed/helios/products.parquet
- https://scalytics.github.io/agent-feed/training/products.parquet
- https://scalytics.github.io/agent-feed/services/products.parquet

Each folder also has `products.csv` and `products.jsonl.gz`. Paste a
parquet URL into the Business Hub. Parquet is the format OpenAI prefers.

## What is in each feed

Each published offer has one row per currency (EUR and USD). There are
no bundle rows.

- Lascaris: Support and Operated on IBM Cloud. Support USD is first.
- KafShield: Support and managed on IBM Cloud, as a standalone product.
- Kerovan: Support (self-run on your Kafka) and Enterprise. Kerovan is
  an add-on for Lascaris or for Confluent, Apache Kafka, Amazon MSK,
  Redpanda or Strimzi.
- Helios: Support (self-run, one site) and Enterprise (up to 5 sites).
  Helios is an add-on for Lascaris or for those same Kafka installations.
  The software is Apache 2.0; the price is for support, signed reports
  and response times, not a licence.
- Training: priced workshops from the Scalytics pricing page (Kafka
  Deep Dive, Apache Wayang Workshop, and Advanced training), plus the
  Confluent Platform Onboarding and Confluent Cloud Training cohorts.
- Services: published add-ons, sprints, retainers and other service
  offers from the pricing page.

Seller name is Scalytics, Inc. Store country is US.

The Confluent Platform Onboarding and Confluent Cloud Training cohorts
from https://www.scalytics.io/enablement are in the training feed, each
as a public per-seat price and a private flat fee for up to 12
participants. The two range-priced SAP, ERP and SCADA offers
(production implementation, and custom connector development) are in
the services feed at the published low end, with "from" in the title
and description. These prices were set by Scalytics sales on
2026-11-14.

## How to update

1. Edit `products.yaml`. This file is the source of truth.
2. Install dependencies once: `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`
3. Run `.venv/bin/python build.py`
4. Run `.venv/bin/python validate.py`
5. Commit the YAML, the generated feed files in each product folder,
   the root `products.csv`, `products.parquet` and `products.jsonl.gz`,
   and `index.html`.
6. Push to `main`. GitHub Actions builds, validates and publishes the
   files to GitHub Pages at the URLs above.

Do not invent prices, ratings, GTINs or stock counts. Do not add bundle
rows. Write plain English, do not use em dashes, say Kafka rather than
"streaming" on its own, and never write "inside your perimeter". If you
mention ISO 27001, say it is in progress.
