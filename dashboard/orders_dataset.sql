SELECT order_id, purchase_date, purchase_month, customer_state,
       items_value, freight_value, delivery_days,
       CASE WHEN on_time_delivery IS NOT NULL THEN 1 ELSE 0 END AS sla_evaluable,
       CASE WHEN on_time_delivery THEN 1 ELSE 0 END AS on_time,
       CASE WHEN NOT on_time_delivery THEN 1 ELSE 0 END AS late,
       CAST(CASE WHEN on_time_delivery THEN 100
                 WHEN NOT on_time_delivery THEN 0
                 ELSE NULL END AS DOUBLE) AS on_time_percent_per_order
FROM `workspace`.`olist_gold`.`fact_orders`
WHERE is_sales_eligible
