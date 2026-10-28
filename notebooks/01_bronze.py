# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: preserve the source
# MAGIC Read five original Olist CSVs and retain their values as strings in Delta tables.
# MAGIC Add the source file, batch identifier and ingestion timestamp.
# MAGIC This is a full snapshot: rerunning replaces the project Bronze tables.

# COMMAND ----------
# MAGIC %run ./_shared

# COMMAND ----------
cfg = config(spark, dbutils)
files = {item.name.rstrip('/') for item in dbutils.fs.ls(cfg['raw_path'])}
missing = [spec['file'] for spec in DATASETS.values() if spec['file'] not in files]
if missing:
    raise FileNotFoundError('Upload these files to the volume: ' + ', '.join(missing))

# COMMAND ----------
# Preflight: validate every source file before starting writes.
counts = {}
for entity, spec in DATASETS.items():
    df = read_raw(spark, cfg['raw_path'] + '/' + spec['file'], entity)
    count = df.count()
    if count == 0:
        raise ValueError('Empty file: ' + spec['file'])
    counts[entity] = count

# COMMAND ----------
for entity, spec in DATASETS.items():
    source = read_raw(spark, cfg['raw_path'] + '/' + spec['file'], entity)
    name = table_name(cfg['catalog'], 'bronze', entity)
    write_snapshot(source, name)
    assert spark.table(name).count() == counts[entity]
    print(entity + ': ' + str(counts[entity]) + ' rows written to ' + name)

# COMMAND ----------
display(spark.table(table_name(cfg['catalog'], 'bronze', 'orders')).limit(10))
