"""Execute dashboard queries locally with filtered real Gold data.

This validates SQL and metric behavior. Native Lakeview import, rendering and
filter interactions must still be confirmed in the user's workspace.
"""
import importlib.util
import json
import tempfile
from decimal import Decimal
from pathlib import Path

import sqlglot
from sqlglot import exp
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def widget_sql(query):
    # Associativity is supplied internally by Lakeview, not by the SQL dataset.
    fields = [f for f in query['fields'] if not f['name'].endswith('_associativity')]
    expressions = [(f, sqlglot.parse_one(f['expression'], read='databricks')) for f in fields]
    aggregated = any(any(tree.find_all(exp.AggFunc)) for _, tree in expressions)
    select = ', '.join(f['expression'] + ' AS `' + f['name'] + '`' for f in fields)
    prefix = 'SELECT DISTINCT ' if not aggregated and not query.get('disaggregated') else 'SELECT '
    sql = prefix + select + ' FROM validation_dashboard_dataset'
    groups = [f['expression'] for f, tree in expressions if not any(tree.find_all(exp.AggFunc))]
    if aggregated and groups:
        sql += ' GROUP BY ' + ', '.join(groups)
    if query.get('orders'):
        sql += ' ORDER BY ' + ', '.join(order['expression'] + ' ' + order['direction'] for order in query['orders'])
    return sql


def validate_percentage_fixtures(spark, dataset_sql, payload):
    counter = next(item['widget'] for item in payload['pages'][0]['layout']
                   if item['widget'].get('spec', {}).get('widgetType') == 'counter'
                   and item['widget']['queries'][0]['query']['fields'][0]['expression']
                   == 'AVG(`on_time_percent_per_order`)')
    cases = [
        ('one_on_time_one_late_one_unknown', ['TRUE', 'FALSE', 'CAST(NULL AS BOOLEAN)'], 50.0),
        ('one_evaluable_two_unknown', ['TRUE', 'CAST(NULL AS BOOLEAN)', 'CAST(NULL AS BOOLEAN)'], 100.0),
        ('no_evaluable_deadlines', ['CAST(NULL AS BOOLEAN)', 'CAST(NULL AS BOOLEAN)'], None),
    ]
    results = {}
    for name, flags, expected in cases:
        values = ', '.join('(' + str(i) + ', ' + flag + ')' for i, flag in enumerate(flags))
        spark.sql(f"""SELECT CAST(id AS STRING) order_id,
            DATE'2026-01-01' purchase_date, DATE'2026-01-01' purchase_month,
            'DF' customer_state, CAST(1 AS DECIMAL(18,2)) items_value,
            CAST(0 AS DECIMAL(18,2)) freight_value, 2 delivery_days,
            on_time_delivery, TRUE is_sales_eligible
            FROM VALUES {values} AS flags(id, on_time_delivery)""").createOrReplaceTempView('validation_gold_orders')
        spark.sql(dataset_sql).createOrReplaceTempView('validation_dashboard_dataset')
        actual = spark.sql(widget_sql(counter['queries'][0]['query'])).first()['metric']
        assert (actual is None and expected is None) or abs(actual-expected) < 1e-10
        results[name] = {'expected_percent': expected, 'actual_percent': actual, 'result': 'passed'}
    return results


def main():
    S = module('olist_shared', ROOT / 'notebooks/_shared.py')
    G = module('olist_gold', ROOT / 'notebooks/_gold.py')
    payload = json.loads((ROOT / 'dashboard/Olist_Analytics.lvdash.json').read_text())
    dataset_sql = ''.join(payload['datasets'][0]['queryLines'])
    assert isinstance(sqlglot.parse_one(dataset_sql, read='databricks'), exp.Select)
    spark = (SparkSession.builder.master('local[2]').appName('olist-dashboard-validation')
             .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
             .config('spark.ui.enabled', 'false').config('spark.ui.showConsoleProgress', 'false')
             .config('spark.sql.shuffle.partitions', '4').config('spark.sql.ansi.enabled', 'true').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    local_dataset_sql = dataset_sql.replace('`workspace`.`olist_gold`.`fact_orders`', 'validation_gold_orders')
    assert 'workspace' not in local_dataset_sql
    percentage_fixtures = validate_percentage_fixtures(spark, local_dataset_sql, payload)
    print(json.dumps({'percentage_fixtures': percentage_fixtures}), flush=True)
    scenarios = {}
    with tempfile.TemporaryDirectory(prefix='olist-dashboard-validation-') as staging:
        parents = {}
        for entity in S.BUILD_ORDER:
            raw = S.read_raw(spark, str(ROOT / 'data/raw' / S.DATASETS[entity]['file']), entity)
            clean, _ = S.curate(entity, raw, parents)
            path = str(Path(staging) / entity)
            clean.write.mode('overwrite').parquet(path)
            parents[entity] = spark.read.parquet(path)
            parents[entity].createOrReplaceTempView('s_' + entity)
            print(json.dumps({'prepared': entity, 'rows': parents[entity].count()}), flush=True)
        fact = G.build_gold_orders(spark, 's_orders', 's_customers', 's_items', 's_payments')
        path = str(Path(staging) / 'gold')
        fact.write.mode('overwrite').parquet(path)
        spark.read.parquet(path).createOrReplaceTempView('validation_gold_orders')
        dataset_sql = local_dataset_sql
        for name, predicate in [
            ('all', 'TRUE'),
            ('df_2018', "customer_state = 'DF' AND purchase_date BETWEEN DATE'2018-01-01' AND DATE'2018-12-31'"),
            ('df_go_2017', "customer_state IN ('DF','GO') AND purchase_date BETWEEN DATE'2017-01-01' AND DATE'2017-12-31'"),
            ('empty', "purchase_date >= DATE'2030-01-01'"),
        ]:
            spark.sql('SELECT * FROM (' + dataset_sql + ') data WHERE ' + predicate).createOrReplaceTempView('validation_dashboard_dataset')
            expected = spark.sql(f"""SELECT COUNT(*) orders, SUM(items_value) items_value,
                SUM(CASE WHEN on_time_delivery THEN 1 ELSE 0 END) AS on_time_orders,
                COUNT(on_time_delivery) AS sla_evaluable_orders,
                SUM(CASE WHEN on_time_delivery THEN 1 ELSE 0 END) /
                NULLIF(COUNT(on_time_delivery),0) AS rate
                FROM validation_gold_orders WHERE is_sales_eligible AND ({predicate})""").first()
            expected_months = {row['month']: row for row in spark.sql(f"""
                SELECT purchase_month AS month,
                       SUM(CASE WHEN on_time_delivery THEN 1 ELSE 0 END) AS on_time_orders,
                       COUNT(on_time_delivery) AS sla_orders,
                       AVG(items_value) AS average_items
                FROM validation_gold_orders
                WHERE is_sales_eligible AND ({predicate})
                GROUP BY purchase_month
            """).collect()}
            counters = {}
            chart_totals = {}
            evaluated = 0
            for item in payload['pages'][0]['layout']:
                widget = item['widget']
                if 'spec' not in widget:
                    continue
                kind = widget['spec']['widgetType']
                rows = spark.sql(widget_sql(widget['queries'][0]['query'])).collect()
                evaluated += 1
                if kind == 'counter':
                    expression = widget['queries'][0]['query']['fields'][0]['expression']
                    counters[expression] = rows[0]['metric']
                elif kind in ['line', 'bar']:
                    chart_totals[kind] = sum((r['total_items'] for r in rows), Decimal('0.00'))
                elif kind == 'table':
                    chart_totals[kind] = sum((r['items_value'] for r in rows), Decimal('0.00'))
                    assert sum(r['orders'] for r in rows) == expected['orders']
                    for row in rows:
                        baseline = expected_months[row['month']]
                        assert row['sla_orders'] == baseline['sla_orders']
                        assert row['average_items'] == baseline['average_items']
                        expected_pct = (100.0 * baseline['on_time_orders'] / baseline['sla_orders']) if baseline['sla_orders'] else None
                        assert (row['on_time_pct'] is None and expected_pct is None) or abs(row['on_time_pct']-expected_pct) < 1e-9
            assert counters['COUNT(`order_id`)'] == expected['orders']
            assert counters['SUM(`items_value`)'] == expected['items_value']
            percent = counters['AVG(`on_time_percent_per_order`)']
            rate = percent/100.0 if percent is not None else None
            assert (rate is None and expected['rate'] is None) or abs(rate-expected['rate']) < 1e-12
            assert all(total == (expected['items_value'] or 0) for total in chart_totals.values())
            scenarios[name] = {'orders': expected['orders'], 'items_value_brl': str(expected['items_value']),
                               'on_time_orders': expected['on_time_orders'] or 0,
                               'sla_evaluable_orders': expected['sla_evaluable_orders'],
                               'on_time_rate': rate, 'on_time_percent': percent, 'widget_queries_executed': evaluated,
                               'charts_and_table_reconciled': True}
            print(json.dumps({'scenario': name, **scenarios[name]}, default=str), flush=True)
        assert scenarios['all']['orders'] == 96470
        assert scenarios['all']['items_value_brl'] == '13220248.93'
        assert 0 < scenarios['df_2018']['orders'] < scenarios['all']['orders']
        assert scenarios['empty']['orders'] == 0 and scenarios['empty']['on_time_rate'] is None
    spark.stop()
    result = {
        'scope': 'Local Spark on original CSVs, curated Silver and Gold. Field predicates applied before widget aggregation. Native Lakeview import, rendering, associativity and UI interactions are not executed locally.',
        'percentage_fixtures': percentage_fixtures, 'scenarios': scenarios, 'result': 'passed'
    }
    (ROOT / 'docs/validation_dashboard_queries.json').write_text(json.dumps(result, default=str, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result, default=str, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
