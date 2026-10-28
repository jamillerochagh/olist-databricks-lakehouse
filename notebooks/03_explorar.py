# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Explore data quality and initial metrics
# MAGIC Read-only Silver diagnostics. These queries explain coverage and safe aggregation
# MAGIC before the formal Gold tables are materialized.

# COMMAND ----------
# MAGIC %run ./_shared

# COMMAND ----------
cfg = config(spark, dbutils)
display(spark.table(table_name(cfg['catalog'], 'quality', 'load_summary')))
orders = table_name(cfg['catalog'], 'silver', 'orders')
items = table_name(cfg['catalog'], 'silver', 'items')

# COMMAND ----------
display(spark.sql(f'''
SELECT order_status, COUNT(*) AS orders
FROM {orders}
GROUP BY order_status
ORDER BY orders DESC
'''))

# COMMAND ----------
# Aggregate items by order BEFORE joining to avoid multiplied amounts.
# KPI: delivered item value, excluding freight; not accounting revenue.
display(spark.sql(f'''
WITH order_values AS (
 SELECT order_id, SUM(price) AS items_value, SUM(freight_value) AS freight_value
 FROM {items}
 GROUP BY order_id
)
SELECT date_format(o.order_purchase_timestamp, 'yyyy-MM') AS purchase_month,
 COUNT(*) AS delivered_orders_with_valid_items,
 ROUND(SUM(v.items_value), 2) AS delivered_items_value,
 ROUND(AVG(v.items_value), 2) AS average_items_value_per_order
FROM {orders} o
JOIN order_values v ON o.order_id = v.order_id
WHERE o.order_status = 'delivered'
GROUP BY date_format(o.order_purchase_timestamp, 'yyyy-MM')
ORDER BY purchase_month
'''))
