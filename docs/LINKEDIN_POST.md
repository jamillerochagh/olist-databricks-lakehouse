I built an e-commerce lakehouse in Databricks Free Edition, from raw CSVs to an interactive sales and delivery dashboard.

The project processes 448,369 source records across five Olist datasets using Python, PySpark, SQL, Delta Lake and Unity Catalog.

I organized the pipeline into Bronze, Silver and Gold layers, with data quality checks, quarantine records and explicit metric definitions. The dashboard analyzes 96,470 delivered orders with valid items and supports purchase-date and customer-state filters.

The most useful lesson was learning how data modeling changes the reliability of a result: aggregate items and payments separately before joining, preserve rejected records, and use the correct denominator for delivery rates.

I validated the pipeline locally and checked the dashboard totals and filters in Databricks.

#Databricks #PySpark #SQL #DataAnalytics #DataEngineering
