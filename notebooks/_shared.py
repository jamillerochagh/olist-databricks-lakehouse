# Databricks notebook source
# MAGIC %md
# MAGIC # Shared input contracts and transformations
# MAGIC Reusable functions for the Olist snapshot pipeline. This notebook installs no packages,
# MAGIC creates no resources on import and makes no external calls. Other source notebooks use `%run ./_shared`.

# COMMAND ----------
import re
from pyspark.sql import functions as F, types as T, Window

DATASETS = {
    'customers': {
        'file': 'olist_customers_dataset.csv',
        'columns': ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state'],
        'key': ['customer_id'],
        'ids': ['customer_id', 'customer_unique_id'],
        'casts': {}, 'timestamps': []},
    'products': {
        'file': 'olist_products_dataset.csv',
        'columns': ['product_id', 'product_category_name', 'product_name_lenght', 'product_description_lenght', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'],
        'key': ['product_id'], 'ids': ['product_id'],
        'casts': {name: 'int' for name in ['product_name_lenght', 'product_description_lenght', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']},
        'timestamps': []},
    'orders': {
        'file': 'olist_orders_dataset.csv',
        'columns': ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date'],
        'key': ['order_id'], 'ids': ['order_id', 'customer_id'], 'casts': {},
        'timestamps': ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']},
    'items': {
        'file': 'olist_order_items_dataset.csv',
        'columns': ['order_id', 'order_item_id', 'product_id', 'seller_id', 'shipping_limit_date', 'price', 'freight_value'],
        'key': ['order_id', 'order_item_id'], 'ids': ['order_id', 'product_id', 'seller_id'],
        'casts': {'order_item_id': 'int', 'price': 'decimal(18,2)', 'freight_value': 'decimal(18,2)'},
        'timestamps': ['shipping_limit_date']},
    'payments': {
        'file': 'olist_order_payments_dataset.csv',
        'columns': ['order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value'],
        'key': ['order_id', 'payment_sequential'], 'ids': ['order_id'],
        'casts': {'payment_sequential': 'int', 'payment_installments': 'int', 'payment_value': 'decimal(18,2)'},
        'timestamps': []},
}
BUILD_ORDER = ['customers', 'products', 'orders', 'items', 'payments']
FOREIGN_KEYS = {
    'orders': [('customer_id', 'customers', 'customer_id')],
    'items': [('order_id', 'orders', 'order_id'), ('product_id', 'products', 'product_id')],
    'payments': [('order_id', 'orders', 'order_id')],
}


def quote_identifier(name):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', name):
        raise ValueError('Use an identifier containing letters, numbers and underscores: ' + repr(name))
    return '`' + name + '`'


def table_name(catalog, layer, entity):
    return '.'.join(quote_identifier(x) for x in [catalog, 'olist_' + layer, entity])


def config(spark, dbutils):
    default_catalog = spark.sql('SELECT current_catalog()').first()[0]
    try:
        catalog = dbutils.widgets.get('catalog').strip()
    except Exception:
        dbutils.widgets.text('catalog', default_catalog, 'Existing catalog')
        catalog = dbutils.widgets.get('catalog').strip()
    quote_identifier(catalog)
    return {'catalog': catalog, 'raw_path': '/Volumes/' + catalog + '/olist_landing/raw'}


def read_raw(spark, path, entity):
    spec = DATASETS[entity]
    schema = T.StructType([T.StructField(c, T.StringType(), True) for c in spec['columns']])
    # FAILFAST and enforceSchema=false check header column count and order.
    return (spark.read.option('header', True).option('encoding', 'UTF-8')
            .option('mode', 'FAILFAST').option('enforceSchema', False)
            .schema(schema).csv(path)
            .select('*', F.col('_metadata.file_path').alias('_source_file'))
            .withColumn('_ingested_at', F.current_timestamp())
            .withColumn('_batch_id', F.lit('initial_snapshot')))


def _add_reasons(df, rules):
    # Native Spark expressions; no Python UDFs, RDDs or SparkContext access.
    reasons = F.filter(F.array(*[
        F.when(F.coalesce(condition, F.lit(False)), F.lit(label))
        for label, condition in rules
    ]), lambda value: value.isNotNull())
    return df.withColumn('_rejection_reasons', reasons)


def normalize(entity, source):
    spec = DATASETS[entity]
    missing = set(spec['columns']) - set(source.columns)
    if missing:
        raise ValueError('Missing columns in ' + entity + ': ' + ', '.join(sorted(missing)))
    df = source.withColumn('_raw_payload', F.to_json(F.struct(*[F.col(c) for c in spec['columns']])))
    for c in spec['columns']:
        df = df.withColumn(c, F.when(F.length(F.trim(F.col(c))) > 0, F.trim(F.col(c))))
    rules = []
    for c, kind in spec['casts'].items():
        numeric = F.expr('try_cast(`' + c + '` as decimal(20,6))')
        value = F.expr('try_cast(try_cast(`' + c + '` as decimal(20,6)) as int)') if kind == 'int' else F.expr('try_cast(`' + c + '` as ' + kind + ')')
        invalid = value.isNull() | ((numeric != value.cast('decimal(20,6)')) if kind == 'int' else F.lit(False))
        flag = '_bad_' + c
        df = df.withColumn(flag, F.col(c).isNotNull() & invalid).withColumn(c, value)
        rules.append(('invalid_type:' + c, F.col(flag)))
        rules.append(('negative:' + c, F.col(c) < 0))
    for c in spec['timestamps']:
        value = F.try_to_timestamp(F.col(c), F.lit('yyyy-MM-dd HH:mm:ss'))
        flag = '_bad_' + c
        df = df.withColumn(flag, F.col(c).isNotNull() & value.isNull()).withColumn(c, value)
        rules.append(('invalid_timestamp:' + c, F.col(flag)))
    for c in spec['ids']:
        df = df.withColumn(c, F.lower(F.col(c)))
        rules.append(('invalid_id:' + c, F.col(c).isNull() | ~F.col(c).rlike('^[0-9a-f]{32}$')))
    for c in spec['key']:
        rules.append(('missing_key:' + c, F.col(c).isNull()))
        if spec['casts'].get(c) == 'int':
            rules.append(('non_positive_key:' + c, F.col(c) <= 0))
    if entity == 'customers':
        df = df.withColumn('customer_state', F.upper(F.col('customer_state')))
        df = df.withColumn('_bad_zip', F.col('customer_zip_code_prefix').isNotNull() & ~F.col('customer_zip_code_prefix').rlike('^[0-9]{1,5}$'))
        rules.append(('invalid_zip_prefix', F.col('_bad_zip')))
        df = df.withColumn('customer_zip_code_prefix', F.lpad(F.col('customer_zip_code_prefix'), 5, '0'))
        states = 'AC AL AP AM BA CE DF ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO'.split()
        rules += [('invalid_state', F.col('customer_state').isNull() | ~F.col('customer_state').isin(states)),
                  ('invalid_zip_prefix', F.col('customer_zip_code_prefix').isNull() | ~F.col('customer_zip_code_prefix').rlike('^[0-9]{5}$'))]
    if entity == 'orders':
        states = ['created', 'approved', 'invoiced', 'processing', 'shipped', 'delivered', 'unavailable', 'canceled']
        df = df.withColumn('order_status', F.lower(F.col('order_status')))
        rules += [('invalid_order_status', F.col('order_status').isNull() | ~F.col('order_status').isin(states)),
                  ('missing_purchase_timestamp', F.col('order_purchase_timestamp').isNull()),
                  ('delivered_without_delivery_date', (F.col('order_status') == 'delivered') & F.col('order_delivered_customer_date').isNull()),
                  ('delivery_before_purchase', F.col('order_delivered_customer_date') < F.col('order_purchase_timestamp'))]
    if entity == 'items':
        for c in ['price', 'freight_value']:
            rules.append(('missing_value:' + c, F.col(c).isNull()))
    if entity == 'payments':
        df = df.withColumn('payment_type', F.lower(F.col('payment_type')))
        rules += [('missing_value:payment_value', F.col('payment_value').isNull()),
                  ('missing_value:payment_installments', F.col('payment_installments').isNull()),
                  ('missing_payment_type', F.col('payment_type').isNull())]
    df = _add_reasons(df, rules)
    canonical = F.to_json(F.struct(*[F.col(c) for c in spec['columns']]))
    return df.withColumn('_record_hash', F.sha2(canonical, 256))


def curate(entity, source, parents=None):
    """Silver snapshot: validation, key conflicts, deduplication and referential integrity."""
    spec = DATASETS[entity]
    df = normalize(entity, source)
    valid = df.filter(F.size('_rejection_reasons') == 0)
    conflicts = (valid.groupBy(*spec['key']).agg(F.countDistinct('_record_hash').alias('_variants'))
                 .filter(F.col('_variants') > 1).select(*spec['key']).withColumn('_conflict', F.lit(True)))
    df = df.join(conflicts, spec['key'], 'left')
    df = df.withColumn('_rejection_reasons', F.concat('_rejection_reasons',
        F.when(F.col('_conflict') == True, F.array(F.lit('conflicting_business_key'))).otherwise(F.array().cast('array<string>'))))
    rejected = df.filter(F.size('_rejection_reasons') > 0)
    candidates = (df.filter(F.size('_rejection_reasons') == 0)
                  .withColumn('_rank', F.row_number().over(Window.partitionBy(*spec['key']).orderBy('_record_hash', '_raw_payload')))
                  .filter(F.col('_rank') == 1).drop('_rank'))
    for column, parent, parent_key in FOREIGN_KEYS.get(entity, []):
        if parents is None or parent not in parents:
            raise ValueError('Required parent table: ' + parent)
        marker = '_exists_' + column
        lookup = parents[parent].select(F.col(parent_key).alias(column)).distinct().withColumn(marker, F.lit(True))
        candidates = candidates.join(lookup, column, 'left')
        candidates = candidates.withColumn('_rejection_reasons', F.concat('_rejection_reasons',
            F.when(F.col(marker).isNull(), F.array(F.lit('missing_parent:' + parent + '.' + column))).otherwise(F.array().cast('array<string>')))).drop(marker)
    orphaned = candidates.filter(F.size('_rejection_reasons') > 0)
    rejected = rejected.unionByName(orphaned, allowMissingColumns=True)
    clean = candidates.filter(F.size('_rejection_reasons') == 0)
    internal = [c for c in rejected.columns if c.startswith('_bad_')] + ['_conflict', '_record_hash']
    rejected = rejected.drop(*internal)
    clean = clean.drop(*internal, '_raw_payload', '_rejection_reasons')
    return clean, rejected


def write_snapshot(df, name):
    # Replace project-owned snapshots. Incremental MERGE is a future extension.
    (df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(name))
