"""Testes reais com Spark; fixtures sinteticas pequenas, sem alegar execucao no Databricks."""
import importlib.util
import unittest
import csv
import tempfile
import uuid
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('shared', ROOT / 'notebooks/_shared.py')
S = importlib.util.module_from_spec(spec)
spec.loader.exec_module(S)


def identifier(number):
    return format(number, '032x')


class Transformations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[2]').appName('olist-unit-tests')
                     .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1').config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '2')
                     .config('spark.sql.ansi.enabled', 'true').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.staging = tempfile.TemporaryDirectory(prefix='olist-fixtures-')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.staging.cleanup()

    def frame(self, entity, rows):
        fields = S.DATASETS[entity]['columns']
        path = Path(self.staging.name) / (uuid.uuid4().hex + '.csv')
        with path.open('w', newline='', encoding='utf-8') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        return S.read_raw(self.spark, str(path), entity).withColumn('_ingested_at', F.to_timestamp(F.lit('2026-01-01 00:00:00')))


    def customer(self, n=1, **overrides):
        return {'customer_id': identifier(n), 'customer_unique_id': identifier(n+100), 'customer_zip_code_prefix': '01234', 'customer_city': 'Brasília', 'customer_state': 'df', **overrides}

    def order(self, **overrides):
        return {'order_id': identifier(10), 'customer_id': identifier(1), 'order_status': 'delivered', 'order_purchase_timestamp': '2018-01-01 10:00:00', 'order_approved_at': '2018-01-01 11:00:00', 'order_delivered_carrier_date': '2018-01-02 10:00:00', 'order_delivered_customer_date': '2018-01-03 10:00:00', 'order_estimated_delivery_date': '2018-01-04 00:00:00', **overrides}

    def customer_parent(self):
        return S.curate('customers', self.frame('customers', [self.customer()]))[0]

    def test_exact_duplicates_collapse_but_repeat_customers_remain(self):
        a = self.customer()
        b = self.customer(2, customer_unique_id=a['customer_unique_id'])
        clean, bad = S.curate('customers', self.frame('customers', [a, a, b]))
        self.assertEqual(clean.count(), 2)
        self.assertEqual(bad.count(), 0)
        self.assertEqual(clean.select('customer_unique_id').distinct().count(), 1)
        self.assertEqual({r.customer_zip_code_prefix for r in clean.collect()}, {'01234'})

    def test_conflicting_key_is_quarantined_not_arbitrarily_selected(self):
        clean, bad = S.curate('customers', self.frame('customers', [self.customer(), self.customer(customer_city='Recife')]))
        self.assertEqual(clean.count(), 0)
        rows = bad.collect()
        self.assertEqual(len(rows), 2)
        self.assertTrue(all('conflicting_business_key' in r._rejection_reasons for r in rows))
        self.assertTrue(all(r._raw_payload for r in rows))

    def test_bad_zip_is_not_silently_truncated(self):
        clean, bad = S.curate('customers', self.frame('customers', [self.customer(customer_zip_code_prefix='123456')]))
        self.assertEqual(clean.count(), 0)
        self.assertIn('invalid_zip_prefix', bad.first()._rejection_reasons)

    def test_null_or_non_hex_keys_are_quarantined(self):
        clean, bad = S.curate('customers', self.frame('customers', [self.customer(customer_id=None), self.customer(customer_id='invalid')]))
        self.assertEqual(clean.count(), 0)
        self.assertEqual(bad.count(), 2)

    def test_decimal_formatted_integer_is_preserved_and_fraction_rejected(self):
        rows = [{'product_id': identifier(1), 'product_weight_g': '500.0'},
                {'product_id': identifier(2), 'product_weight_g': '500.5'}]
        clean, bad = S.curate('products', self.frame('products', rows))
        self.assertEqual(clean.first().product_weight_g, 500)
        self.assertIsNone(clean.first().product_category_name)
        self.assertEqual(bad.count(), 1)

    def test_order_status_and_delivery_rules(self):
        rows = [self.order(order_id=identifier(10)),
                self.order(order_id=identifier(11), order_status='canceled', order_delivered_customer_date=None),
                self.order(order_id=identifier(12), order_delivered_customer_date=None),
                self.order(order_id=identifier(13), order_purchase_timestamp='not-a-date'),
                self.order(order_id=identifier(14), order_delivered_customer_date='2017-12-31 10:00:00')]
        clean, bad = S.curate('orders', self.frame('orders', rows), {'customers': self.customer_parent()})
        self.assertEqual(clean.count(), 2)
        self.assertEqual(bad.count(), 3)
        reasons = [reason for row in bad.collect() for reason in row._rejection_reasons]
        self.assertIn('invalid_timestamp:order_purchase_timestamp', reasons)
        self.assertIn('delivery_before_purchase', reasons)

    def test_orphan_order_is_quarantined(self):
        clean, bad = S.curate('orders', self.frame('orders', [self.order(customer_id=identifier(999))]), {'customers': self.customer_parent()})
        self.assertEqual(clean.count(), 0)
        self.assertIn('missing_parent:customers.customer_id', bad.first()._rejection_reasons)

    def test_invalid_decimal_does_not_crash_ansi_mode(self):
        order_parent = self.spark.sql("SELECT '" + identifier(10) + "' AS order_id")
        rows = [{'order_id': identifier(10), 'payment_sequential': '1', 'payment_type': 'credit_card', 'payment_installments': '1', 'payment_value': 'abc'}]
        clean, bad = S.curate('payments', self.frame('payments', rows), {'orders': order_parent})
        self.assertEqual(clean.count(), 0)
        self.assertIn('invalid_type:payment_value', bad.first()._rejection_reasons)

    def test_sql_identifiers_are_validated(self):
        for value in ['workspace; DROP TABLE x', 'a.b', '', 'workspace`']:
            with self.assertRaises(ValueError):
                S.quote_identifier(value)


if __name__ == '__main__':
    unittest.main(verbosity=2)
