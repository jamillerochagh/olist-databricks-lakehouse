# Olist · Sales & Delivery

`dashboard/Olist_Analytics.lvdash.json` defines a native Databricks AI/BI dashboard:
one page, one SQL dataset, two filters, three counters, two charts and one monthly
table. All authored labels and notes are in English; amounts are identified as BRL.
The file contains no order records, account identifiers, connection credentials,
publication settings or sharing settings.

## Import or update

1. Run `04_gold` using the project catalog (`workspace` in the reference setup).
2. Open **Dashboards → Import dashboard from file** and choose the definition.
3. If asked, select the existing **Serverless Starter Warehouse** SQL warehouse.
4. Refresh the data and begin with all dates and states.

For an existing draft, use **⋮ → File actions → Replace**, select the updated
file and confirm **Overwrite**. Refresh the data afterward. Keep the dashboard
as a draft during review; local scripts do not publish it.

## Fields and measures

| Element | Meaning |
| --- | --- |
| Purchase date | Filter on `purchase_date` |
| Customer state (Brazil) | Multiple-value filter on `customer_state` |
| Delivered orders | Count of eligible delivered orders with valid items |
| Item value (BRL) | Sum of item prices, excluding freight |
| On-time delivery (%) | Average of nullable per-order 100/0 indicators |
| Monthly item value | Sum of item value by purchase month |
| Item value by state | Sum of item value by customer state |
| Monthly breakdown | Counts, item value, freight, average order item value and delivery-rate components |

All visuals share an order-level dataset from `olist_gold.fact_orders`, restricted
to eligible delivered orders. Date/state filters apply before aggregation.
Unknown delivery assessments are excluded from the rate denominator; a zero
denominator produces a missing rate. Numeric formatting preserves the 0–100
indicator scale. See [GOLD_METRICS.md](GOLD_METRICS.md) for full definitions.

## Manual check

| Filter selection | Delivered orders | Item value (BRL) | On-time delivery (%) |
| --- | ---: | ---: | ---: |
| All dates and states | 96,470 | 13,220,248.93 | 93.23 |
| DF, 2018-01-01 through 2018-12-31 | 1,193 | 164,284.94 | 93.29 |

Check both views, verify chart/table response, then clear filters and confirm
that the full-period totals return. The owner confirmed that this checklist
matched in Databricks on October 28, 2026. The manual record is
`validation_databricks_user_report.json`.

## Local generation and validation

Install `requirements-dashboard.txt`, then run:

```bash
python scripts/build_dashboard.py --catalog workspace
```

For real-data SQL checks, also install `requirements-dev.txt`, place the original
CSVs in `data/raw/`, and run `python scripts/validate_dashboard.py`.

Structural checks use the Databricks Labs LSQL 0.17.0 model and validate layout,
references, percentage formatting and `main_query` bindings. Derived per-order
calculations live in dataset SQL; visual fields use simple aggregates.
The SQL suite covers four filter scenarios and reconciles charts and tables.

Local Spark does not execute native Lakeview rendering or the internal filter
associativity predicate. Those behaviors were checked separately in Databricks.

Official references:

- https://docs.databricks.com/aws/en/dashboards/automate/import-export
- https://docs.databricks.com/aws/en/dashboards/manage/filters
- https://github.com/databrickslabs/lsql
