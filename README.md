# Olist E-commerce Lakehouse

A Databricks project by **Jamille Ghazaleh**, connecting data engineering
with sales and delivery analytics. The pipeline preserves source records, applies
explicit quality rules and serves an interactive dashboard in English.

**Stack:** Python, PySpark, SQL, Delta Lake, Unity Catalog, Databricks AI/BI.

## Results

Five original Olist CSVs contain **448,369 source rows**. After validation, the
order fact contains 99,433 accepted orders. Sales analysis covers delivered
orders with valid items:

| View | Eligible orders | Item value, excluding freight | On-time delivery |
| --- | ---: | ---: | ---: |
| All dates and states | 96,470 | BRL 13,220,248.93 | 93.23% |
| DF, purchases in 2018 | 1,193 | BRL 164,284.94 | 93.29% |

The dashboard supports purchase-date and customer-state filters, monthly trends,
state comparisons and a monthly detail table. Values describe this historical
public dataset; they are not accounting revenue or measured commercial impact.

## Pipeline

| Layer | Purpose |
| --- | --- |
| Landing | Original CSVs in a Unity Catalog managed volume |
| Bronze | Delta snapshots retaining original strings and provenance metadata |
| Silver | Typed records, exact-duplicate collapse, conflict and foreign-key checks |
| Quality | Rejected records with original payloads, reasons and row-balance summaries |
| Gold | One row per accepted order, plus monthly and state summaries |
| Dashboard | Consistent date/state filtering before metric aggregation |

Items and payment components are aggregated separately by order before joining.
This prevents multi-item, multi-payment orders from multiplying monetary values.
Eight delivered orders without delivery dates are quarantined, together with
associated child records. The source remains preserved and coverage is explicit.

## Run the project

Follow [GETTING_STARTED.md](GETTING_STARTED.md). Import the five standalone
`.ipynb` notebooks into Databricks, upload the unchanged original CSVs to the
landing volume and run the notebooks in order.

| Notebook | Purpose |
| --- | --- |
| `00_setup` | Project schemas and managed landing volume |
| `01_bronze` | Original CSV ingestion into Delta snapshots |
| `02_silver` | Typing, quality checks, deduplication and quarantine |
| `03_explorar` | Read-only exploration and safe aggregation diagnostics |
| `04_gold` | Order fact, summaries and reconciliation |

The `03_explorar` filename is retained for compatibility; its content is in English.
Standalone notebooks include dependencies. The alternative source route imports
all seven `.py` files from `notebooks/` into the same Workspace folder, using
`%run ./_shared` and `%run ./_gold`. No pip installation is needed in Databricks.

Import `dashboard/Olist_Analytics.lvdash.json` through Dashboards and select the
existing SQL warehouse. See [docs/DASHBOARD.md](docs/DASHBOARD.md).

## Modeling and metric decisions

| Entity | Business key |
| --- | --- |
| customers | `customer_id` |
| products | `product_id` |
| orders | `order_id` |
| items | `order_id`, `order_item_id` |
| payments | `order_id`, `payment_sequential` |

`customer_unique_id` identifies repeat customers; do not deduplicate customer
records on that field. Products without a category remain valid. Integral decimal
values such as `500.0` can become integers; fractional values are rejected.
Postal prefixes retain leading zeroes. Conflicting valid versions of the same
business key are all quarantined rather than resolved arbitrarily.

Gold retains all accepted order statuses in the detailed fact. Sales summaries
include delivered orders with valid items. Delivery performance compares calendar
dates and excludes unknown assessments from the rate denominator. Missing
components remain distinguishable from genuine zero amounts. Read the complete
[metric contract](docs/GOLD_METRICS.md).

## Validation

Nine transformation tests, real-data reconciliation, Gold edge cases and a
repeat snapshot passed locally in Spark. Dashboard widget queries were checked
in four scenarios, including an empty period. Local persistence uses Parquet and
views; the notebooks use Delta and Unity Catalog in Databricks.

The owner also ran the workflow in Databricks. The captured Silver summary
matched the reference counts, and the dashboard totals/date-state checklist
was manually confirmed on October 28, 2026. Local automated checks and manual
cloud evidence are documented separately in [docs/VALIDATION.md](docs/VALIDATION.md).

For local validation, use Java 17 and Python 3.10+, install `requirements-dev.txt`,
place original CSVs under `data/raw/`, and run:

```bash
python -m unittest discover -s tests -v
python scripts/validate_real_data.py
python scripts/validate_gold.py
```

Dashboard generation/validation also uses `requirements-dashboard.txt`:

```bash
python scripts/build_dashboard.py --catalog workspace
python scripts/validate_dashboard.py
```

These local scripts do not authenticate to a Databricks account.

## Scope and extensions

This version uses full snapshots of project-owned tables. Each write is atomic
per table; the entire multi-table run is not one transaction. Repeating unchanged
inputs preserves business values, while ingestion timestamps can change.

Future extensions are separate star-schema dimensions, simulated monthly arrivals,
idempotent incremental MERGE and Lakeflow Jobs orchestration. No ML model is
included in this analytics project.

## Portfolio and Git practice

- [Case study and CV wording](docs/PORTFOLIO.md)
- [Git workflow to practice](docs/GIT_WORKFLOW.md)
- [LinkedIn post draft](docs/LINKEDIN_POST.md)

## Data source

[**Brazilian E-Commerce Public Dataset by Olist**](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce),
Olist and collaborators, historical transactions from 2016–2018.
The publisher lists **CC BY-NC-SA 4.0** for the dataset:
https://creativecommons.org/licenses/by-nc-sa/4.0/

`data/source_manifest.json` records original filenames, counts and hashes.
Raw CSVs are excluded from Git. No employer data is used.

Official platform references:

- https://docs.databricks.com/aws/en/getting-started/free-edition
- https://docs.databricks.com/aws/en/notebooks/notebook-export-import
- https://docs.databricks.com/aws/en/lakehouse/medallion
