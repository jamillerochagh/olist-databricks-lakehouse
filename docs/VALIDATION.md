# Validation evidence

## Local automated checks

- Source Python and standalone notebook code cells compile.
- Original CSV headers, row counts and SHA-256 hashes match `data/source_manifest.json`.
- Nine transformation tests passed in Apache Spark 4.0.1, covering numeric types,
  invalid dates, duplicates, key conflicts and missing references.
- Real-data validation completed on all five original CSVs.
- Gold checks reconcile grain, amounts, component counts, summaries and delivery
  denominators, including a repeat snapshot and meaningful edge cases.
- Dashboard SQL checks cover all data, DF/2018, DF+GO/2017 and an empty period.
  Cards, charts and the monthly table reconcile with the fact population.
- Structural checks verify model roundtrips, references, query bindings,
  percentage formatting and a non-overlapping dashboard layout.

Reports: `validation_real_data.json`, `validation_gold.json` and
`validation_dashboard_*.json`. Local tests use temporary Parquet files and views;
they do not emulate Unity Catalog access controls or run an automated cloud suite.

## Manual Databricks checks

The project owner ran the notebook workflow in Databricks Free Edition.
The captured Silver summary matched the counts below. On October 28, 2026,
the owner confirmed the English dashboard's reference totals and date/state
filter checklist, including the corrected on-time percentage and DF/2018 view.

This check covers the dashboard's consumption of the Gold fact. It does not
independently audit every table or permission. The manual record is
`validation_databricks_user_report.json`.

## Row balance

| Entity | Source | Silver | Rejected | Duplicates collapsed |
| --- | ---: | ---: | ---: | ---: |
| customers | 99,441 | 99,441 | 0 | 0 |
| products | 32,951 | 32,951 | 0 | 0 |
| orders | 99,441 | 99,433 | 8 | 0 |
| items | 112,650 | 112,642 | 8 | 0 |
| payments | 103,886 | 103,878 | 8 | 0 |

Eight delivered orders lack delivery dates. The current policy quarantines them
and eight associated item rows and payment components. The original records
remain preserved; analytical coverage differs from the complete source.

## Scope

This version uses full snapshots. Separate star-schema dimensions, incremental
MERGE and Lakeflow Jobs orchestration remain future extensions. Portfolio editing
and English translation do not add those capabilities or change transformation logic.
