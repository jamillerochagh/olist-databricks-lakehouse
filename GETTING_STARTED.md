# Run the Olist lakehouse

Reproduce the snapshot pipeline and English dashboard in Databricks Free Edition.
Use the same existing catalog throughout; the reference catalog is `workspace`.

## Import and prepare

Create an `olist-lakehouse` folder in Workspace. Import the five notebooks from
`standalone/`, starting with `00_setup.ipynb`. Select **Serverless** and use
**Run all**. Each notebook includes its dependencies; no package installation
or shared-notebook imports are required.

Alternatively, import all seven `.py` source notebooks into the same folder.
This route uses `%run ./_shared` and, for Gold, `%run ./_gold`.

Run `00_setup` to create `olist_landing`, `olist_bronze`, `olist_silver` and
`olist_quality`, plus a managed volume at `/Volumes/<catalog>/olist_landing/raw`.
Names prefixed with `olist_` are reserved for this project.

## Upload source data

Download the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
Keep these five files unchanged:

- `olist_customers_dataset.csv`
- `olist_products_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`

In Catalog, open `<catalog> > olist_landing > Volumes > raw` and upload the CSVs
directly to the volume root. Preserve filenames and headers. Expected counts
and SHA-256 hashes are in `data/source_manifest.json`. Raw CSVs are excluded
from the code archive and repository.

## Run the pipeline

Run `01_bronze`, then `02_silver`. Compare the quality summary:

| Entity | Bronze | Silver | Rejected | Duplicates collapsed |
| --- | ---: | ---: | ---: | ---: |
| customers | 99,441 | 99,441 | 0 | 0 |
| products | 32,951 | 32,951 | 0 | 0 |
| orders | 99,441 | 99,433 | 8 | 0 |
| items | 112,650 | 112,642 | 8 | 0 |
| payments | 103,886 | 103,878 | 8 | 0 |

Inspect rejected records in `olist_quality.<entity>_rejected`. They retain
`_raw_payload`, `_rejection_reasons` and source metadata. A row can have multiple
reasons, so reason occurrences can exceed rejected-row counts.

Run the read-only `03_explorar` notebook, then `04_gold`. Gold creates
`fact_orders`, `monthly_sales` and `sales_by_state`. The overview is 99,433
accepted orders, 96,470 eligible delivered orders, BRL 13,220,248.93 in item value
and 23 purchase months. Read [docs/GOLD_METRICS.md](docs/GOLD_METRICS.md).

These are full snapshots. Rerunning unchanged inputs should preserve business
counts and values; ingestion timestamps can change. Each table is written
separately. After fixing an interrupted run, rerun to complete the snapshot.
Avoid reading partially updated summaries.

## Import the dashboard

In **Dashboards**, choose **Import dashboard from file** and select
`dashboard/Olist_Analytics.lvdash.json`. If requested, select the existing
**Serverless Starter Warehouse** SQL warehouse. Dashboard compute uses a SQL
warehouse, separate from notebook Serverless compute.

Refresh data and follow [docs/DASHBOARD.md](docs/DASHBOARD.md). Keep the dashboard
as a draft while reviewing it.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Permission error | Use an existing catalog where the account can create schemas and a volume. |
| `%run` dependency missing | Keep source notebooks together, or use the standalone notebooks. |
| Missing input file | Check the catalog, volume, exact filename and absence of extra subfolders. |
| Header mismatch | Use the unchanged original CSV. |
| Missing Bronze/Silver table | Verify the catalog and preceding notebook run. |
| Interrupted writes | Resolve the error and rerun to complete the snapshot. |

See [docs/VALIDATION.md](docs/VALIDATION.md) for evidence and scope.
