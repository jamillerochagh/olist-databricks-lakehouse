# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Set up the Olist project
# MAGIC Run this notebook first to create project schemas and a managed landing volume
# MAGIC in an existing Unity Catalog catalog. Names beginning with `olist_` are reserved for this project.

# COMMAND ----------
# MAGIC %run ./_shared

# COMMAND ----------
cfg = config(spark, dbutils)
for layer in ['landing', 'bronze', 'silver', 'quality']:
    spark.sql('CREATE SCHEMA IF NOT EXISTS ' + quote_identifier(cfg['catalog']) + '.' + quote_identifier('olist_' + layer))
spark.sql('CREATE VOLUME IF NOT EXISTS ' + quote_identifier(cfg['catalog']) + '.`olist_landing`.`raw`')
print('Catalog:', cfg['catalog'])
print('CSV destination:', cfg['raw_path'])
print('Upload the five original CSVs directly to this volume, without subfolders.')
for spec in DATASETS.values():
    print(' -', spec['file'])

# COMMAND ----------
# MAGIC %md
# MAGIC **Before continuing:** open Catalog > selected catalog > olist_landing > raw
# MAGIC and upload the five original CSVs directly to the volume. Do not upload the code archive.
# MAGIC See `GETTING_STARTED.md` for the source files and execution order.
