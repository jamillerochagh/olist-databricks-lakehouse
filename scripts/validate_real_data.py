"""Executa a mesma transformacao dos notebooks sobre os CSVs reais, em Spark local.
Nao valida Unity Catalog, Delta Lake ou a execucao serverless do Databricks.
"""
import importlib.util
import json
import tempfile
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('shared', ROOT / 'notebooks/_shared.py')
S = importlib.util.module_from_spec(spec)
spec.loader.exec_module(S)
spark = (SparkSession.builder.master('local[2]').appName('olist-real-data-validation')
         .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1').config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '4')
         .config('spark.sql.ansi.enabled', 'true').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
report = []
with tempfile.TemporaryDirectory(prefix='olist-validation-') as staging:
    parents = {}
    for entity in S.BUILD_ORDER:
        path = ROOT / 'data/raw' / S.DATASETS[entity]['file']
        if not path.exists():
            raise FileNotFoundError('Baixe os CSVs originais da Olist: ' + str(path))
        raw = S.read_raw(spark, str(path), entity)
        raw_count = raw.count()
        clean, rejected = S.curate(entity, raw, parents)
        # Persistir e reler corta a linhagem exatamente como os notebooks fazem.
        clean_path = str(Path(staging) / entity)
        bad_path = str(Path(staging) / (entity + '_rejected'))
        clean.write.mode('overwrite').parquet(clean_path)
        rejected.write.mode('overwrite').parquet(bad_path)
        parents[entity] = spark.read.parquet(clean_path)
        bad = spark.read.parquet(bad_path)
        good_count = parents[entity].count()
        bad_count = bad.count()
        assert parents[entity].select(*S.DATASETS[entity]['key']).distinct().count() == good_count
        collapsed = raw_count-good_count-bad_count
        assert collapsed >= 0
        reasons = {r.reason:r['count'] for r in bad.select(F.explode('_rejection_reasons').alias('reason')).groupBy('reason').count().collect()}
        report.append({'entity':entity, 'source_rows':raw_count, 'silver_rows':good_count, 'rejected_rows':bad_count, 'duplicates_collapsed':collapsed, 'reasons':reasons})
        print(json.dumps(report[-1],ensure_ascii=False),flush=True)
    # Integridade apos a cascata de rejeicoes e conferencia de joins com multiplos pagamentos.
    assert parents['items'].join(parents['orders'].select('order_id'),'order_id','left_anti').count() == 0
    assert parents['payments'].join(parents['orders'].select('order_id'),'order_id','left_anti').count() == 0
    result = {'scope':'Apache Spark local; CSVs reais. Persistencia temporaria em Parquet, nao Databricks/Delta.', 'spark_version':spark.version, 'tables':report}
    output=ROOT/'docs/validation_real_data.json'
    output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print('Resultado:',output,flush=True)
spark.stop()
