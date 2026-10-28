# Olist E-commerce Lakehouse

**Jamille Ghazaleh · Personal project · October 2026**

**Stack:** Databricks Free Edition, PySpark, Python, SQL, Delta Lake,
Unity Catalog and Databricks AI/BI dashboards.

## Business question

How can historical e-commerce transactions support consistent analysis of
delivered order value and delivery performance across Brazilian states?

The project processes five original Olist CSV datasets with **448,369 source
rows**, covering customers, products, orders, items and payment components.
It preserves the original data, applies explicit quality rules and serves an
interactive sales and delivery dashboard in English.

## Implementation

- **Bronze:** ingest the original CSVs into Delta tables with source-file,
  ingestion-time and batch metadata.
- **Silver:** validate types, keys, duplicates, lifecycle dates and parent-child
  relationships. Retain rejected records with reasons and the original payload.
- **Gold:** create an order fact with one row per accepted order, plus monthly
  and state summaries. Aggregate items and payments separately before joining
  to prevent inflated amounts.
- **Dashboard:** filter by purchase date and customer state, inspect delivered
  orders, item value and on-time delivery, and compare monthly and state trends.

## Validated reference results

| View | Delivered orders with valid items | Item value, excluding freight | On-time delivery |
| --- | ---: | ---: | ---: |
| Full observed period, all states | 96,470 | BRL 13,220,248.93 | 93.23% |
| Federal District (DF), purchases in 2018 | 1,193 | BRL 164,284.94 | 93.29% |

The local Spark checks reconcile dashboard queries with the curated data.
The project owner also confirmed the dashboard totals and date/state filter
checklist in Databricks on October 28, 2026. The dashboard remains a draft.

## Decisions that make the results trustworthy

| Decision | Reason |
| --- | --- |
| Separate item and payment aggregation at order grain | Both entities can have multiple rows per order; joining them directly can multiply amounts. |
| Quarantine delivered orders without delivery dates | Eight such orders, eight related items and eight payment components are excluded from Silver under the documented rules. The source is retained. |
| Use evaluable orders as the delivery-rate denominator | Compare calendar delivery dates with promised dates; exclude unknown or invalid assessments. The global rate is 89,936 / 96,470. |
| Separate item value from freight | The dashboard measures delivered item value; it does not establish accounting revenue. |
| Keep purchase month explicit | The dataset is historical, and boundary months may be partial. Trends describe purchase cohorts. |

## Validation and scope

Validation includes nine transformation tests, real-data reconciliation, Gold
edge cases, repeat-snapshot checks and dashboard SQL checks for four filter
scenarios, including an empty period. Native rendering and filter use were
checked manually by the owner; automated tests ran locally.

This version uses full snapshots. Incremental MERGE processing and Lakeflow
Jobs orchestration are future extensions. No machine-learning model is part
of this project.

## CV wording

**Olist E-commerce Lakehouse | Databricks, PySpark, SQL, Delta Lake**

- Built a Databricks lakehouse with Bronze, Silver and Gold layers to process
  448,369 source rows across five e-commerce datasets, using PySpark, SQL,
  Delta tables and Unity Catalog.
- Implemented data quality checks and quarantine handling, reconciled order-level
  metrics, and created an interactive dashboard with date/state filters for
  delivered item value and on-time delivery across 96,470 eligible orders.

Place this entry under **Projects** and list Databricks, PySpark, SQL and Delta
Lake among the tools used in the project.

## Data attribution

Source: **Brazilian E-Commerce Public Dataset by Olist**, Olist and collaborators,
historical transactions from 2016–2018.

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Dataset license: **CC BY-NC-SA 4.0**. This project uses public source data.
The amounts above are analytical results on this dataset, not a measured
commercial impact or cost saving.
