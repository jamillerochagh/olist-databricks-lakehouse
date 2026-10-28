"""Validate Gold with edge cases and real CSVs in local Spark, using Parquet.

This does not validate Unity Catalog, Delta writes or the user's workspace.
Run from the project root after installing requirements-dev.txt.
"""
import csv
import importlib.util
import json
import tempfile
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def fixture_checks(spark, staging, G):
    def source(name, schema, rows):
        # Names are supplied separately to keep decimal type parameters intact.
        names = {
            'orders': ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_delivered_customer_date', 'order_estimated_delivery_date'],
            'customers': ['customer_id', 'customer_unique_id', 'customer_city', 'customer_state'],
            'items': ['order_id', 'price', 'freight_value'],
            'payments': ['order_id', 'payment_value'],
        }[name]
        path = Path(staging) / ('fixture_' + name + '.csv')
        with path.open('w', newline='') as handle:
            writer = csv.writer(handle)
            writer.writerow(names)
            writer.writerows(rows)
        frame = (spark.read.option('header', True).option('timestampFormat', 'yyyy-MM-dd HH:mm:ss')
                 .schema(schema).csv(str(path)))
        frame.createOrReplaceTempView('f_' + name)
    ids = [f'{i:032x}' for i in range(1, 7)]
    customer_ids = [f'{i:032x}' for i in range(20, 26)]
    source('customers', 'customer_id string, customer_unique_id string, customer_city string, customer_state string',
           [(cid, f'{90 + i // 2:032x}', 'brasilia', 'DF') for i, cid in enumerate(customer_ids)])
    source('orders', 'order_id string, customer_id string, order_status string, order_purchase_timestamp timestamp, order_delivered_customer_date timestamp, order_estimated_delivery_date timestamp', [
        (ids[0], customer_ids[0], 'delivered', '2026-01-01 09:00:00', '2026-01-03 18:00:00', '2026-01-03 00:00:00'),
        (ids[1], customer_ids[1], 'delivered', '2026-01-01 09:00:00', '2026-01-04 10:00:00', '2026-01-03 00:00:00'),
        (ids[2], customer_ids[2], 'delivered', '2026-01-01 09:00:00', '2026-01-03 10:00:00', None),
        (ids[3], customer_ids[3], 'delivered', '2026-01-01 09:00:00', '2026-01-03 10:00:00', '2026-01-03 00:00:00'),
        (ids[4], customer_ids[4], 'canceled', '2026-01-01 09:00:00', None, '2026-01-03 00:00:00'),
        (ids[5], customer_ids[5], 'delivered', '2026-01-01 09:00:00', '2026-01-02 10:00:00', '2025-12-31 00:00:00'),
    ])
    source('items', 'order_id string, price decimal(18,2), freight_value decimal(18,2)', [
        (ids[0], '10.00', '1.00'), (ids[0], '20.00', '2.00'), (ids[1], '40.00', '5.00'),
        (ids[2], '0.00', '0.00'), (ids[4], '7.00', '1.00'), (ids[5], '80.00', '0.00'),
    ])
    source('payments', 'order_id string, payment_value decimal(18,2)', [
        (ids[0], '15.00'), (ids[0], '18.00'), (ids[1], '45.00'), (ids[4], '8.00'), (ids[5], '80.00'),
    ])
    fact = G.build_gold_orders(spark, 'f_orders', 'f_customers', 'f_items', 'f_payments')
    path = str(Path(staging) / 'fixture_fact')
    fact.write.mode('overwrite').parquet(path)
    fact = spark.read.parquet(path)
    fact.createOrReplaceTempView('f_fact')
    rows = {r.order_id: r.asDict() for r in fact.collect()}
    assert len(rows) == 6
    assert rows[ids[0]]['item_rows'] == rows[ids[0]]['payment_rows'] == 2
    assert rows[ids[0]]['items_value'] == Decimal('30.00')
    assert rows[ids[0]]['payment_value'] == Decimal('33.00')
    assert rows[ids[0]]['payment_minus_items_and_freight'] == 0
    assert rows[ids[0]]['on_time_delivery'] is True and rows[ids[0]]['delivery_days'] == 2
    assert rows[ids[1]]['on_time_delivery'] is False
    assert rows[ids[2]]['on_time_delivery'] is None
    assert rows[ids[2]]['is_sales_eligible'] is True and rows[ids[2]]['has_valid_payments'] is False
    assert rows[ids[3]]['items_value'] is None and rows[ids[3]]['item_rows'] == 0
    assert rows[ids[3]]['is_sales_eligible'] is False
    assert rows[ids[4]]['is_sales_eligible'] is False and rows[ids[4]]['on_time_delivery'] is None
    assert rows[ids[5]]['on_time_delivery'] is None
    month = G.build_gold_summary(spark, 'f_fact', 'month')
    state = G.build_gold_summary(spark, 'f_fact', 'state')
    month.createOrReplaceTempView('f_month')
    state.createOrReplaceTempView('f_state')
    m = month.first()
    assert m.delivered_orders_with_valid_items == 4
    assert m.delivered_items_value == Decimal('150.00')
    assert m.sla_evaluable_orders == 2 and m.on_time_orders == m.late_orders == 1
    assert m.on_time_rate_pct == Decimal('50.00')
    sources = {e: 'f_' + e for e in ['orders', 'customers', 'items', 'payments']}
    G.validate_gold(spark, sources, 'f_fact', 'f_month', 'f_state')
    fact.filter(F.col('order_id').isin(ids[2], ids[5])).createOrReplaceTempView('f_no_sla')
    zero = G.build_gold_summary(spark, 'f_no_sla', 'month').first()
    assert zero.sla_evaluable_orders == 0 and zero.on_time_rate_pct is None
    fact.withColumn('items_value', F.col('items_value') * 2).createOrReplaceTempView('f_inflated')
    caught = False
    try:
        G.validate_gold(spark, sources, 'f_inflated', 'f_month', 'f_state')
    except AssertionError as error:
        caught = 'Duplicated or missing' in str(error)
    assert caught, 'Validation must detect inflated amounts even when row counts are unchanged.'
    return ['multiple_items_and_payments', 'calendar_day_deadline', 'late_delivery',
            'unknown_and_invalid_deadline', 'zero_denominator', 'missing_items_vs_zero_value',
            'missing_payments', 'canceled_order_cohort', 'inflated_amount_detection']


def main():
    S = module('olist_shared', ROOT / 'notebooks/_shared.py')
    G = module('olist_gold', ROOT / 'notebooks/_gold.py')
    notebook_path = ROOT / 'standalone/04_gold.ipynb'
    if not notebook_path.exists():
        raise FileNotFoundError('Place 04_gold.ipynb in the project standalone/ folder.')
    spark = (SparkSession.builder.master('local[2]').appName('olist-gold-validation')
             .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
             .config('spark.ui.enabled', 'false').config('spark.ui.showConsoleProgress', 'false')
             .config('spark.sql.shuffle.partitions', '4').config('spark.sql.ansi.enabled', 'true').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    with tempfile.TemporaryDirectory(prefix='olist-gold-validation-') as staging:
        fixtures = fixture_checks(spark, staging, G)
        print(json.dumps({'fixtures': fixtures, 'result': 'passed'}), flush=True)
        parents = {}
        for entity in S.BUILD_ORDER:
            raw = S.read_raw(spark, str(ROOT / 'data/raw' / S.DATASETS[entity]['file']), entity)
            clean, _ = S.curate(entity, raw, parents)
            path = str(Path(staging) / ('silver_' + entity))
            clean.write.mode('overwrite').parquet(path)
            parents[entity] = spark.read.parquet(path)
            parents[entity].createOrReplaceTempView('v_silver_' + entity)
            print(json.dumps({'prepared': entity, 'rows': parents[entity].count()}), flush=True)

        class LocalSpark:
            def __getattr__(self, name):
                return getattr(spark, name)
            def sql(self, query, *args, **kwargs):
                if query.startswith('CREATE SCHEMA IF NOT EXISTS '):
                    assert query == 'CREATE SCHEMA IF NOT EXISTS `workspace`.`olist_gold`'
                    return spark.sql('SELECT 1')
                return spark.sql(query, *args, **kwargs)
        seen = {}
        current = None
        def display(df):
            rows = [row.asDict(recursive=True) for row in df.collect()]
            seen[current] = rows
            print(json.dumps({'cell': current, 'rows': len(rows)}), flush=True)
        def local_snapshot(df, name):
            view_name = name.strip('`')
            path = str(Path(staging) / view_name)
            df.write.mode('overwrite').parquet(path)
            spark.read.parquet(path).createOrReplaceTempView(view_name)
        widgets = SimpleNamespace(get=lambda key: 'workspace', text=lambda *args: None)
        scope = {'spark': LocalSpark(), 'dbutils': SimpleNamespace(widgets=widgets), 'display': display}
        notebook = json.loads(notebook_path.read_text())
        for c in notebook['cells']:
            if c['cell_type'] != 'code':
                continue
            current = c['id']
            exec(compile(''.join(c['source']), '04_gold.ipynb:' + current, 'exec'), scope)
            if current == 'shared-helpers':
                scope['table_name'] = lambda catalog, layer, entity: '`v_' + layer + '_' + entity + '`'
                scope['write_snapshot'] = local_snapshot
        overview = seen['validation'][0]
        assert overview['orders_in_gold'] == 99433
        assert overview['eligible_delivered_orders'] == 96470
        assert overview['delivered_items_value_brl'] == Decimal('13220248.93')
        assert overview['purchase_months'] == 23
        assert scope['gold_validation']['customer_state_rows'] == 27
        # Re-run the Gold snapshot only, to check that overwrite does not accumulate orders.
        for ident in ['fact', 'marts', 'validation']:
            c = next(c for c in notebook['cells'] if c['id'] == ident)
            current = ident
            exec(compile(''.join(c['source']), '04_gold.ipynb:repeat:' + ident, 'exec'), scope)
        assert seen['validation'][0] == overview
        result = {
            'scope': 'Local Spark 4.0.1; real CSVs curated with project functions. All Gold notebook cells executed with temporary Parquet and views replacing Delta/Unity Catalog. Not a cloud execution test.',
            'fixture_checks': fixtures, 'overview': overview, 'validation': scope['gold_validation'],
            'repeat_snapshot_same_overview': True, 'result': 'passed',
        }
        output = ROOT / 'docs/validation_gold.json'
        output.write_text(json.dumps(result, default=str, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps(result, default=str, ensure_ascii=False), flush=True)
    spark.stop()


if __name__ == '__main__':
    main()
