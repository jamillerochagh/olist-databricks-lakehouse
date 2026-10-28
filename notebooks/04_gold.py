# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold: order facts and dashboard metrics
# MAGIC Create a snapshot with one row per accepted order and summaries by purchase month and customer state.
# MAGIC Compare actual and promised delivery dates at calendar-day granularity.

# COMMAND ----------
# MAGIC %run ./_shared

# COMMAND ----------
# MAGIC %run ./_gold

# COMMAND ----------
cfg = config(spark, dbutils)
sources = {entity: table_name(cfg['catalog'], 'silver', entity)
           for entity in ['orders', 'customers', 'items', 'payments']}
required_columns = {
    'orders': ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
               'order_delivered_customer_date', 'order_estimated_delivery_date'],
    'customers': ['customer_id', 'customer_unique_id', 'customer_city', 'customer_state'],
    'items': ['order_id', 'price', 'freight_value'],
    'payments': ['order_id', 'payment_value'],
}
for entity, name in sources.items():
    columns = set(spark.table(name).columns)
    missing = set(required_columns[entity]) - columns
    if missing:
        raise ValueError('Missing Silver columns for ' + entity + ': ' + ', '.join(sorted(missing)))
for entity, key in [('orders', 'order_id'), ('customers', 'customer_id')]:
    source = spark.table(sources[entity])
    if source.filter(F.col(key).isNull()).count() or source.select(key).distinct().count() != source.count():
        raise AssertionError('Invalid or duplicate Silver key: ' + entity)

spark.sql('CREATE SCHEMA IF NOT EXISTS ' + quote_identifier(cfg['catalog']) + '.`olist_gold`')
fact_name = table_name(cfg['catalog'], 'gold', 'fact_orders')
monthly_name = table_name(cfg['catalog'], 'gold', 'monthly_sales')
state_name = table_name(cfg['catalog'], 'gold', 'sales_by_state')

# COMMAND ----------
fact = build_gold_orders(spark, sources['orders'], sources['customers'], sources['items'], sources['payments'])
write_snapshot(fact, fact_name)
print('Order fact written:', fact_name)

# COMMAND ----------
write_snapshot(build_gold_summary(spark, fact_name, 'month'), monthly_name)
write_snapshot(build_gold_summary(spark, fact_name, 'state'), state_name)
print('Monthly and state summaries written.')

# COMMAND ----------
gold_validation = validate_gold(spark, sources, fact_name, monthly_name, state_name)
print('Validation passed:', gold_validation)
display(gold_overview(spark, fact_name))
print('Gold complete: 3 Delta tables in ' + cfg['catalog'] + '.olist_gold.')

# COMMAND ----------
display(spark.table(monthly_name).orderBy('purchase_month'))
display(spark.table(state_name).orderBy(F.desc('delivered_items_value')))
