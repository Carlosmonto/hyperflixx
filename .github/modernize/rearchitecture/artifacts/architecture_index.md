# Architecture Index

This index is not the full contract. Do not implement from this file alone; follow the
artifact paths below.

## Global artifacts

- `unit_graph.yaml`: all workers use it for entry points, exported signatures, dependencies,
  dynamic entry points and shared reference subsets. Filter by `units[].name`.
- `migration_boundary.yaml`: defines CI/CD integration scope. Use `must_rewrite`, not
  `source_anchors`, as the implementation boundary.
- `wire_contracts.yaml`: preserve Atlas, S3, BigQuery and Terraform contracts. Filter by
  `unit`, including `global`.
- `shared_modules.yaml`: use rows whose `used_by_units` contains the assigned unit.
  `shared_refs.used_fields` in `unit_graph.yaml` is the allowed subset.
- `cross_unit_state.yaml`: currently empty; re-check if a new workflow introduces state
  shared between jobs or scripts.
- `project-structure.md`, `tech-stack.md`, `data-model.md`: global context for module,
  runtime and data constraints.
- `seams.yaml`: not produced; no partial-migration seam was identified.

## Implementation Guide

Each unit must read its three local artifacts: `behavior.yaml` for side effects, branches,
errors and concurrency; `bindings.yaml` for CLI/framework/runtime configuration; and
`unit_decomposition.yaml` for advisory split candidates only (`commit: false`). Before
DONE, report local/global artifacts read, every `must_preserve`/`must_appear_in_target`
item implemented, unresolved/deferred contracts, and test/build/runtime evidence.

### Units

| Unit / external trigger | Local artifacts | Global rows to filter |
|---|---|---|
| `environment-setup` — `python iac_setup.py` | `units/environment-setup/{behavior,bindings,unit_decomposition}.yaml` | `wire_contracts` global; `migration_boundary.must_rewrite` |
| `create-structure` — `python scripts/crear_estructura.py` | `units/create-structure/{behavior,bindings,unit_decomposition}.yaml` | `shared_modules.used_by_units`; Mongo contract |
| `generate-data` — `python scripts/generar_datos_masivos.py` | `units/generate-data/{behavior,bindings,unit_decomposition}.yaml` | `shared_modules.used_by_units` |
| `transform-oltp-olap` — `python scripts/transformar_oltp_a_olap.py` | `units/transform-oltp-olap/{behavior,bindings,unit_decomposition}.yaml` | `shared_modules.used_by_units`; warehouse data model |
| `build-data-warehouse` — `python scripts/construir_data_warehouse.py` | `units/build-data-warehouse/{behavior,bindings,unit_decomposition}.yaml` | `shared_modules.used_by_units`; warehouse contract |
| `query-data-warehouse` — `python scripts/consultas_data_warehouse.py` | `units/query-data-warehouse/{behavior,bindings,unit_decomposition}.yaml` | `shared_modules.used_by_units` |
| `export-bigquery` — `python scripts/exportar_dwh_bigquery.py` | `units/export-bigquery/{behavior,bindings,unit_decomposition}.yaml` | `wire_contracts.unit=export-bigquery`; shared Mongo module |
| `pipeline-data-lake` — `python scripts/pipeline_data_lake.py` | `units/pipeline-data-lake/{behavior,bindings,unit_decomposition}.yaml` | S3 contract; both shared modules |
| `ingest-streaming` — MongoDB Change Stream | `units/ingest-streaming/{behavior,bindings,unit_decomposition}.yaml` | Mongo contract; no implicit state row currently |
| `spark-analytics` — `python scripts/spark_jobs.py` | `units/spark-analytics/{behavior,bindings,unit_decomposition}.yaml` | Data model and migration boundary |
| `generate-s3-volume` — `python scripts/generar_volumen_s3.py` | `units/generate-s3-volume/{behavior,bindings,unit_decomposition}.yaml` | S3 contract; shared storage fields |
| `produce-cdc` — `python scripts/productor_cdc.py` | `units/produce-cdc/{behavior,bindings,unit_decomposition}.yaml` | Mongo contract; shared connection fields |
| `benchmark-analytics` — `python scripts/benchmark_consultas_analiticas.py` | `units/benchmark-analytics/{behavior,bindings,unit_decomposition}.yaml` | warehouse data model; shared connection fields |
| `benchmark-oltp-olap` — `python scripts/benchmarking_consultas.py` | `units/benchmark-oltp-olap/{behavior,bindings,unit_decomposition}.yaml` | shared connection fields |
| `generate-dashboard` — `python scripts/dashboard_monitoreo.py` | `units/generate-dashboard/{behavior,bindings,unit_decomposition}.yaml` | benchmark and data-model contracts; shared connection fields |
| `generate-catalog` — `python scripts/generar_catalogo.py` | `units/generate-catalog/{behavior,bindings,unit_decomposition}.yaml` | data-model and shared connection fields |

## CI/CD integration boundary

The image's proposed `.github/workflows/deploy.yml` should be implemented as a new
automation unit, not as a replacement for the data pipelines. Its safe automatic path is:

1. checkout and set up Python 3.10+;
2. install requirements with a pinned/reproducible strategy;
3. run syntax/type/lint checks and tests that do not require live credentials;
4. run `terraform fmt -check`, `terraform init` and `terraform validate`;
5. run `terraform plan` using GitHub secrets and a protected network;
6. expose `terraform apply` and data publication as an environment-protected/manual job.

Do not invoke `ingest-streaming`, `generate-s3-volume`, `produce-cdc`, or
`build-data-warehouse --reconstruir` on every push. They are long-running, mutating or
destructive. The workflow must publish logs and reports as artifacts and must not commit
`.env`, Terraform state, GCP JSON keys, AWS profiles or Atlas credentials.

## Completion evidence

An implementation agent must provide: artifact paths read; all preserved behavior and
runtime configuration; any intentionally deferred live integrations; `terraform validate`
and Python validation output; and, for protected jobs, the exact secret/network assumptions.
