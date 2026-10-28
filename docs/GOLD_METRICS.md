# Gold metric contract

## Tables and grain

Gold uses accepted Silver records. Quarantined records remain available for
investigation and are not reincorporated into the analytical population.

| Table | Grain | Population |
| --- | --- | --- |
| `fact_orders` | `order_id` | Every accepted Silver order, any status |
| `monthly_sales` | `purchase_month` | Delivered orders with valid items, grouped by purchase month |
| `sales_by_state` | `customer_state` | The same eligible orders, grouped by the customer state associated with each order |

Tables live in `<catalog>.olist_gold`. Customer attributes are denormalized into
the order fact. Items and payments are aggregated independently by order before
joining to avoid multiplying amounts. Missing components yield zero component
counts and null amounts; an absent item is distinct from an item with zero price.

## Measures

| Field | Definition |
| --- | --- |
| `is_sales_eligible` | Delivered order with at least one valid Silver item |
| `delivered_items_value` | Eligible item prices in BRL, excluding freight |
| `delivered_freight_value` | Freight for the same items, presented separately |
| `average_items_value_per_order` | Item value divided by eligible order count |
| `delivery_days` | Calendar days between purchase and valid delivery dates |
| `on_time_delivery` | True for delivery on/before the promised day; false when late; null when not evaluable |
| `sla_evaluable_orders` | Eligible orders with a non-null delivery assessment |
| `on_time_orders` | Eligible orders with a true delivery assessment |
| `late_orders` | Eligible orders with a false delivery assessment |
| `on_time_rate_pct` | `100 × on_time_orders / sla_evaluable_orders`; null for a zero denominator |
| `payment_minus_items_and_freight` | Payment minus item value and freight when components exist; an investigation field |

Delivery must not precede purchase, and the promised day must not precede the
purchase day. Delivery at 18:00 on the promised day is on time. Timestamps retain
their loaded interpretation; this milestone adds no business-timezone conversion.

The dashboard averages a nullable per-order indicator: 100 for on-time delivery,
0 for late delivery and null for an unknown assessment. It uses a numeric format
on the 0–100 scale, with the percent unit in the title.

## Coverage and reference values

Eight delivered orders lack delivery dates and are quarantined, together with
eight associated items and eight payment components. These exclusions implement
the analytical policy; they do not prove that transactions never occurred.

| View | Eligible orders | Item value (BRL) | On-time delivery |
| --- | ---: | ---: | ---: |
| All dates and states | 96,470 | 13,220,248.93 | 93.23% |
| DF, purchases in 2018 | 1,193 | 164,284.94 | 93.29% |

The full-period rate uses 89,936 on-time orders and 96,470 evaluable orders.
Use purchase month and account for potentially partial boundary months. Item
value is a measure of delivered items, not accounting revenue.

For combined date/state filters, recalculate on `fact_orders`. Do not sum
averages or take an unweighted average of monthly/state rates.

## Reconciliation

Checks cover order grain, monetary totals, component counts, summaries and SLA
denominators. Edge cases include multiple items/payments, same-day delivery,
missing/invalid promises, zero denominators, absent components, zero item price
and canceled orders. An inflated amount must fail even when order counts match.

`validation_gold.json` records local Spark checks and a repeat snapshot.
Local persistence uses Parquet and views; the Databricks notebook writes Delta.
Manual dashboard checks are recorded separately in
`validation_databricks_user_report.json`.
