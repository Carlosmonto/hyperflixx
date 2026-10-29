# Arquitectura del proyecto

## Resumen

El proyecto es una plataforma de datos batch/streaming. MongoDB Atlas conserva la capa
transaccional OLTP y sus colecciones analíticas derivadas; BigQuery es el Data Warehouse
cloud para consultas OLAP. Un ETL Python copia el modelo dimensional desde Atlas a BigQuery.
No hay un servicio web: los procesos se ejecutan como scripts Python. El Data Lake batch
se escribe en Amazon S3.

## Flujo de datos

```text
CAPA DE INGESTA / OLTP
MongoDB Atlas / sample_mflix (movies, comments, users, theaters)
        |
        +--> Change Streams --> ingesta_streaming.py --> Data Lake RAW (JSONL)
        +--> pipeline_data_lake.py --> RAW --> PROCESSED --> CURATED (Parquet en S3)
        +--> transformar_oltp_a_olap.py --> peliculas_analiticas (MongoDB)
                                                   |
CAPA DE DATA WAREHOUSE / OLAP                    |
MongoDB: dim_*, fact_comentarios, agg_* <----------+
        |
        +--> exportar_dwh_bigquery.py (ETL Python, lotes y tablas staging)
                    |
                    v
             Google BigQuery
             dataset mongo_data_warehouse
             dim_*, fact_comentarios, agg_*
```

- `conexion.py` centraliza la URI y la selección de `sample_mflix`.
- `scripts/crear_estructura.py` crea índices operativos y la colección OLAP.
- `scripts/productor_cdc.py` genera cambios para probar Change Streams.
- `scripts/ingesta_streaming.py` persiste eventos CDC y conserva el resume token.
- `scripts/pipeline_data_lake.py` construye las zonas de archivos particionadas por fecha.
- `data_lake_storage.py` abstrae las operaciones batch de JSONL y Parquet para local/S3.
- `scripts/spark_jobs.py` genera una salida analítica adicional por género.
- `scripts/construir_data_warehouse.py` crea dimensiones, hechos y agregados materializados.
- `scripts/exportar_dwh_bigquery.py` publica dimensiones, hechos y agregados en BigQuery.

## Decisiones de infraestructura

Terraform administra únicamente recursos de Atlas: proyecto, cluster, usuario de base de
datos y lista de acceso por CIDR. Las colecciones, índices, transformaciones y archivos
locales permanecen bajo control de los scripts, porque son estado de datos y no recursos
de infraestructura.

El cluster se define como un replicaset M0 multi-tenant. Esto conserva la compatibilidad
con Change Streams y mantiene el coste inicial en el nivel gratuito. `M0` no es adecuado
para cargas de producción o el volumen actual del benchmark; el cambio a Flex o M10+ debe
ser una decisión explícita de capacidad y coste.

## Riesgos y mejoras pendientes

1. El acceso actual es público mediante una lista de IP. Para CI/CD o producción conviene
   usar PrivateLink/VPC peering y un backend remoto cifrado para el estado Terraform.
2. S3 cubre actualmente el pipeline batch; CDC sigue escribiendo un archivo
   mutable local y Spark sigue leyendo/escribiendo rutas locales.
3. MongoDB conserva el Warehouse derivado utilizado por los scripts locales; BigQuery
   mantiene una copia cloud separada para consultas OLAP.
4. El consumidor CDC escribe un único archivo por fecha y token local. Para alta disponibilidad
   necesita almacenamiento duradero y coordinación entre consumidores.
5. La credencial que estaba en `.env` debe rotarse y sustituirse por una credencial nueva.

## Orden operativo

1. Provisionar o adoptar Atlas con `terraform/` cuando corresponda.
2. Completar `.env` con la URI y credenciales del cluster creado.
3. Ejecutar `python iac_setup.py` para preparar el entorno local y validar Atlas.
4. Ejecutar `python scripts/crear_estructura.py`.
5. Ejecutar el pipeline, ETL, warehouse y benchmarks según las guías existentes.
