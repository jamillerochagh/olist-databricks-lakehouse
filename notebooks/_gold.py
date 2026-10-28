# Databricks notebook source
# MAGIC %md
# MAGIC # Olist Gold transformations
# MAGIC An order fact and two business summaries, with reconciliation checks. No external calls.

# COMMAND ----------
from pyspark.sql import functions as F


def build_gold_orders(spark, orders, customers, items, payments):
    """Read Silver tables and return one row for each accepted order."""
    return spark.sql(f"""
WITH item_totals AS (
  SELECT order_id, COUNT(*) AS item_rows,
         SUM(price) AS items_value, SUM(freight_value) AS freight_value
  FROM {items}
  GROUP BY order_id
), payment_totals AS (
  SELECT order_id, COUNT(*) AS payment_rows, SUM(payment_value) AS payment_value
  FROM {payments}
  GROUP BY order_id
)
SELECT o.order_id, o.customer_id, c.customer_unique_id,
       c.customer_city, c.customer_state, o.order_status,
       o.order_purchase_timestamp,
       TO_DATE(o.order_purchase_timestamp) AS purchase_date,
       TO_DATE(DATE_TRUNC('MONTH', o.order_purchase_timestamp)) AS purchase_month,
       TO_DATE(o.order_delivered_customer_date) AS delivery_date,
       TO_DATE(o.order_estimated_delivery_date) AS estimated_delivery_date,
       COALESCE(i.item_rows, 0) AS item_rows,
       i.items_value, i.freight_value,
       COALESCE(p.payment_rows, 0) AS payment_rows, p.payment_value,
       COALESCE(i.item_rows, 0) > 0 AS has_valid_items,
       COALESCE(p.payment_rows, 0) > 0 AS has_valid_payments,
       o.order_status = 'delivered' AND COALESCE(i.item_rows, 0) > 0 AS is_sales_eligible,
       p.payment_value - i.items_value - i.freight_value AS payment_minus_items_and_freight,
       CASE WHEN o.order_status = 'delivered'
                 AND o.order_delivered_customer_date IS NOT NULL
                 AND o.order_delivered_customer_date >= o.order_purchase_timestamp
            THEN DATEDIFF(TO_DATE(o.order_delivered_customer_date), TO_DATE(o.order_purchase_timestamp))
            ELSE CAST(NULL AS INT)
       END AS delivery_days,
       CASE WHEN o.order_status = 'delivered'
                 AND o.order_delivered_customer_date IS NOT NULL
                 AND o.order_delivered_customer_date >= o.order_purchase_timestamp
                 AND TO_DATE(o.order_estimated_delivery_date) >= TO_DATE(o.order_purchase_timestamp)
            THEN TO_DATE(o.order_delivered_customer_date) <= TO_DATE(o.order_estimated_delivery_date)
            ELSE CAST(NULL AS BOOLEAN)
       END AS on_time_delivery
FROM {orders} o
LEFT JOIN {customers} c ON o.customer_id = c.customer_id
LEFT JOIN item_totals i ON o.order_id = i.order_id
LEFT JOIN payment_totals p ON o.order_id = p.order_id
""")


def build_gold_summary(spark, fact_orders, grain):
    """Aggregate the eligible delivered-order population by month or state."""
    column = {'month': 'purchase_month', 'state': 'customer_state'}.get(grain)
    if column is None:
        raise ValueError('Allowed grain: month or state.')
    return spark.sql(f"""
SELECT {column},
       COUNT(*) AS delivered_orders_with_valid_items,
       SUM(items_value) AS delivered_items_value,
       SUM(freight_value) AS delivered_freight_value,
       ROUND(AVG(items_value), 2) AS average_items_value_per_order,
       ROUND(AVG(delivery_days), 2) AS average_delivery_days,
       COUNT(on_time_delivery) AS sla_evaluable_orders,
       SUM(CASE WHEN on_time_delivery THEN 1 ELSE 0 END) AS on_time_orders,
       SUM(CASE WHEN NOT on_time_delivery THEN 1 ELSE 0 END) AS late_orders,
       ROUND(100.0 * SUM(CASE WHEN on_time_delivery THEN 1 ELSE 0 END)
             / NULLIF(COUNT(on_time_delivery), 0), 2) AS on_time_rate_pct
FROM {fact_orders}
WHERE is_sales_eligible
GROUP BY {column}
""")


def gold_overview(spark, fact_orders):
    return spark.sql(f"""
SELECT COUNT(*) AS orders_in_gold,
       SUM(CASE WHEN is_sales_eligible THEN 1 ELSE 0 END) AS eligible_delivered_orders,
       SUM(CASE WHEN is_sales_eligible THEN items_value ELSE CAST(0 AS DECIMAL(18,2)) END)
         AS delivered_items_value_brl,
       COUNT(DISTINCT CASE WHEN is_sales_eligible THEN purchase_month END) AS purchase_months
FROM {fact_orders}
""")


def validate_gold(spark, sources, fact_orders, monthly_sales, sales_by_state):
    """Reconcile row grain, exact monetary totals, summaries and SLA denominators."""
    fact = spark.table(fact_orders)
    original = spark.table(sources['orders'])
    order_count = original.count()
    if fact.count() != order_count or fact.select('order_id').distinct().count() != order_count:
        raise AssertionError('Gold must contain one row per Silver order.')
    if fact.filter(F.col('order_id').isNull() | F.col('customer_unique_id').isNull()
                   | F.col('customer_state').isNull() | F.col('purchase_month').isNull()).count():
        raise AssertionError('Missing Gold key, customer, state or month.')
    if fact.filter(F.col('delivery_days') < 0).count():
        raise AssertionError('Negative delivery duration in Gold.')
    totals = fact.agg(F.sum('items_value').alias('items_value'),
                      F.sum('freight_value').alias('freight_value'),
                      F.sum('payment_value').alias('payment_value'),
                      F.sum('item_rows').alias('item_rows'),
                      F.sum('payment_rows').alias('payment_rows')).first()
    item_totals = spark.table(sources['items']).agg(F.sum('price').alias('items_value'),
                                                   F.sum('freight_value').alias('freight_value')).first()
    payment_total = spark.table(sources['payments']).agg(F.sum('payment_value').alias('value')).first()['value']
    for column in ['items_value', 'freight_value']:
        if (totals[column] or 0) != (item_totals[column] or 0):
            raise AssertionError('Duplicated or missing Gold amount: ' + column)
    if (totals['payment_value'] or 0) != (payment_total or 0):
        raise AssertionError('Duplicated or missing Gold payment amount.')
    for entity, count_column in [('items', 'item_rows'), ('payments', 'payment_rows')]:
        if (totals[count_column] or 0) != spark.table(sources[entity]).count():
            raise AssertionError('Row counts do not reconcile: ' + entity)
    eligible = fact.filter('is_sales_eligible')
    expected = eligible.agg(F.count('*').alias('orders'), F.sum('items_value').alias('items_value'),
                            F.sum('freight_value').alias('freight_value'),
                            F.count('on_time_delivery').alias('sla_orders'),
                            F.sum(F.when(F.col('on_time_delivery'), 1).otherwise(0)).alias('on_time')).first()
    result = {'orders_in_gold': order_count, 'eligible_delivered_orders': expected['orders'],
              'delivered_items_value_brl': str(expected['items_value'] or 0)}
    for name, group_column in [(monthly_sales, 'purchase_month'), (sales_by_state, 'customer_state')]:
        df = spark.table(name)
        count = df.count()
        if df.select(group_column).distinct().count() != count or df.filter(F.col(group_column).isNull()).count():
            raise AssertionError('Invalid summary grain: ' + group_column)
        actual = df.agg(F.sum('delivered_orders_with_valid_items').alias('orders'),
                         F.sum('delivered_items_value').alias('items_value'),
                         F.sum('delivered_freight_value').alias('freight_value'),
                         F.sum('sla_evaluable_orders').alias('sla_orders'),
                         F.sum('on_time_orders').alias('on_time')).first()
        for key in ['orders', 'items_value', 'freight_value', 'sla_orders', 'on_time']:
            if (actual[key] or 0) != (expected[key] or 0):
                raise AssertionError('Summary does not reconcile: ' + group_column + '.' + key)
        invalid = ((F.col('on_time_orders') + F.col('late_orders') != F.col('sla_evaluable_orders'))
                   | (F.col('sla_evaluable_orders') > F.col('delivered_orders_with_valid_items'))
                   | (F.col('on_time_rate_pct') < 0) | (F.col('on_time_rate_pct') > 100)
                   | ((F.col('sla_evaluable_orders') == 0) & F.col('on_time_rate_pct').isNotNull())
                   | ((F.col('sla_evaluable_orders') > 0) & F.col('on_time_rate_pct').isNull()))
        if df.filter(invalid).count():
            raise AssertionError('Invalid delivery-rate denominator or percentage: ' + group_column)
        result[group_column + '_rows'] = count
    return result
