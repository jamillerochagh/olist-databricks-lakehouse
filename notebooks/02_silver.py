# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: typed, validated and traceable data
# MAGIC Validate numeric values, timestamps, keys, duplicates, conflicts and references.
# MAGIC Rejected rows are retained in `olist_quality.<entity>_rejected` with reasons and the original payload.
# MAGIC A rejected row falls outside the analytical policy; it does not prove that the transaction never occurred.
# MAGIC Review quarantine coverage before interpreting metrics.

# COMMAND ----------
# MAGIC %run ./_shared

# COMMAND ----------
cfg = config(spark, dbutils)
parents = {}
report = []
for entity in BUILD_ORDER:
    source = spark.table(table_name(cfg['catalog'], 'bronze', entity))
    source_count = source.count()
    clean, rejected = curate(entity, source, parents)
    silver = table_name(cfg['catalog'], 'silver', entity)
    quarantine = table_name(cfg['catalog'], 'quality', entity + '_rejected')
    write_snapshot(clean, silver)
    write_snapshot(rejected, quarantine)
    # Re-read persisted tables to avoid retaining all five dataset lineages.
    parents[entity] = spark.table(silver)
    clean_count = parents[entity].count()
    rejected_count = spark.table(quarantine).count()
    duplicates = source_count - clean_count - rejected_count
    if duplicates < 0:
        raise AssertionError('Invalid row balance: ' + entity)
    assert parents[entity].select(*DATASETS[entity]['key']).distinct().count() == clean_count
    report.append((entity, source_count, clean_count, rejected_count, duplicates))

# COMMAND ----------
summary = spark.createDataFrame(report, 'entity string, bronze_rows long, silver_rows long, rejected_rows long, duplicate_rows_collapsed long')
write_snapshot(summary, table_name(cfg['catalog'], 'quality', 'load_summary'))
display(summary)

# COMMAND ----------
for entity in BUILD_ORDER:
    reasons = (spark.table(table_name(cfg['catalog'], 'quality', entity + '_rejected'))
               .select(F.explode('_rejection_reasons').alias('reason')).groupBy('reason').count())
    print('Rejection reasons: ' + entity)
    display(reasons)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Review checklist
# MAGIC 1. Bronze = Silver + rejected rows + collapsed exact duplicates.
# MAGIC 2. Canceled or in-transit orders can legitimately lack a delivery date.
# MAGIC 3. Delivered orders without delivery dates are quarantined.
# MAGIC 4. `customer_unique_id` identifies a person; `customer_id` identifies an order-associated customer record.
# MAGIC 5. Products without a category are retained.
# MAGIC Continue with exploration and Gold. Incremental MERGE and orchestration are future extensions.
