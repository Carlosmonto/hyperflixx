# Estructura del proyecto

## Clasificación

Backend de datos / CLI de Python con infraestructura como código. No hay servidor web,
frontend ni monorepo. Los entry points son scripts ejecutables que leen configuración
desde `.env`, MongoDB Atlas, S3 o BigQuery y escriben datos derivados o reportes.

## Módulos y capas

| Área | Ubicación | Responsabilidad |
|---|---|---|
| Conectividad compartida | `conexion.py`, `data_lake_storage.py` | Cliente MongoDB Atlas, selección de base, acceso S3 |
| Preparación local | `iac_setup.py` | Entorno virtual, dependencias, carpetas locales y health check |
| OLTP/Atlas | `scripts/crear_estructura.py`, `scripts/generar_datos_masivos.py`, `scripts/productor_cdc.py` | Índices, datos de prueba y eventos CDC |
| ETL/OLAP | `scripts/transformar_oltp_a_olap.py`, `scripts/construir_data_warehouse.py` | Colecciones analíticas y modelo dimensional |
| Data Lake | `scripts/pipeline_data_lake.py`, `scripts/ingesta_streaming.py`, `scripts/spark_jobs.py`, `scripts/generar_volumen_s3.py` | RAW/PROCESSED/CURATED, CDC y transformación Spark |
| Publicación analítica | `scripts/exportar_dwh_bigquery.py`, `scripts/consultas_data_warehouse.py` | Copia a BigQuery y consultas del warehouse |
| Calidad/observabilidad | `scripts/benchmark_consultas_analiticas.py`, `scripts/benchmarking_consultas.py`, `scripts/dashboard_monitoreo.py`, `scripts/generar_catalogo.py` | SLA, métricas, dashboard y catálogo |
| Infraestructura | `terraform/` | Proyecto, cluster, usuario y lista de acceso de MongoDB Atlas |
| Datos locales | `data_lake/`, `reportes/` | Particiones de datos y salidas generadas; no son código fuente |

## Entry points

- `python iac_setup.py`: preparación y validación local.
- `scripts/crear_estructura.py`: índices y colección OLAP.
- `scripts/generar_datos_masivos.py`: carga sintética.
- `scripts/productor_cdc.py`: productor de insert/update/delete.
- `scripts/ingesta_streaming.py`: consumidor continuo de Change Streams.
- `scripts/pipeline_data_lake.py`: extracción y transformación batch.
- `scripts/spark_jobs.py`: RAW a agregado Parquet con Spark local.
- `scripts/generar_volumen_s3.py`: volumen demostrativo en S3.
- `scripts/transformar_oltp_a_olap.py`: ETL de `movies` a `peliculas_analiticas`.
- `scripts/construir_data_warehouse.py`: dimensiones, hechos y cubos.
- `scripts/exportar_dwh_bigquery.py`: publicación por staging a BigQuery.
- `scripts/consultas_data_warehouse.py`: consultas analíticas.
- `scripts/benchmark_consultas_analiticas.py` y `scripts/benchmarking_consultas.py`: medición.
- `scripts/dashboard_monitoreo.py`: PNG de métricas.
- `scripts/generar_catalogo.py`: catálogo Excel.

## Flujo de dependencias

MongoDB Atlas `sample_mflix` alimenta OLAP, Data Lake y warehouse. El Data Lake batch
usa S3 cuando `DATA_LAKE_URI` es `s3://`; Spark conserva rutas locales. BigQuery se
carga únicamente después de que existan las colecciones del warehouse. Terraform
administra Atlas, no colecciones ni datos.

## Implicaciones para CI/CD

El workflow recomendado debe ejecutar primero formato/validación Terraform y pruebas
unitarias o sintácticas; después puede ejecutar `terraform plan` con secretos efímeros.
Los scripts que mutan datos, requieren credenciales reales, quedan como job de integración
manual/protegido. CDC y generación de volumen no son pasos adecuados para cada `push`.
