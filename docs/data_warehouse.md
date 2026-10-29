# Fase 3: Data Warehouse para sample_mflix

## Arquitectura destino

MongoDB Atlas es la capa de ingesta/OLTP: recibe y conserva `movies`, `comments`,
`users` y `theaters`. En el mismo Atlas se generan `peliculas_analiticas` y un modelo
dimensional derivado para mantener los benchmarks locales existentes. Para separar la
carga analítica en la nube, BigQuery actúa como Data Warehouse/OLAP independiente:

```text
MongoDB Atlas (Ingesta / OLTP)
  movies, comments, users, theaters
          |
          +--> ETL Python: transformar_oltp_a_olap.py
          |       peliculas_analiticas (colección derivada)
          |
          +--> ETL Python: construir_data_warehouse.py
          |       dim_tiempo, dim_usuario, dim_pelicula,
          |       fact_comentarios y tablas agg_*
          |                  |
          |                  +--> exportar_dwh_bigquery.py
          |                         extracción por lotes desde Atlas
          |                         carga tipada en staging y publicación
          |                         BigQuery: dataset.mongo_data_warehouse
          |
          +--> Data Lake: pipeline_data_lake.py --> S3 (RAW/PROCESSED/CURATED)
```

BigQuery contiene las tablas analíticas independientes; MongoDB conserva las colecciones
de origen y la copia derivada que alimenta las consultas/benchmarks locales. La exportación
es una instantánea completa de cada tabla por ejecución, no CDC continuo: vuelve a ejecutar
los ETL y luego `exportar_dwh_bigquery.py` para publicar cambios.

El documento de referencia construye un modelo estrella a partir de `ventas_analiticas`.
Este proyecto trabaja con peliculas y comentarios, así que el mismo diseño se adapta así:

El benchmark mantiene cinco cubos materializados para que las consultas analíticas
cumplan un SLA de hasta 499 ms: `agg_comentarios_diarios`, `agg_genero_decada`,
`agg_genero_top`, `agg_pais_rating` y `agg_genero_premios`.

| Documento (ventas)   | Proyecto (sample_mflix)                 | Grano / contenido                          |
|----------------------|-----------------------------------------|--------------------------------------------|
| `ventas_analiticas`  | `peliculas_analiticas` + `comments`     | Fuente enriquecida (OLTP -> OLAP)          |
| `dim_tiempo`         | `dim_tiempo`                            | Un día calendario con comentarios          |
| `dim_cliente`        | `dim_usuario`                           | Una persona que comenta (email)            |
| `dim_producto`       | `dim_pelicula`                          | Una película del catálogo                  |
| `fact_ventas`        | `fact_comentarios`                      | Un comentario (evento)                     |

```
                dim_tiempo (fecha_id)
                        |
dim_usuario ---- fact_comentarios ---- dim_pelicula
(usuario_id)     cantidad, longitud     (pelicula_id)
```

## Orden de ejecución

1. `python scripts/crear_estructura.py`
2. `python scripts/generar_datos_masivos.py` (opcional)
3. `python scripts/transformar_oltp_a_olap.py`
4. `python scripts/construir_data_warehouse.py` (`--reconstruir` para borrar y cargar desde cero)
5. `python scripts/consultas_data_warehouse.py`

## Publicar el modelo en BigQuery

1. Usa un proyecto de Google Cloud con BigQuery habilitado (BigQuery Sandbox permite
   experimentar sin registrar una tarjeta, sujeto a sus cuotas y límites vigentes).
2. Configura Application Default Credentials o una cuenta de servicio fuera del repositorio.
   La identidad de Google necesita crear el dataset y cargar tablas; el proceso Python
   también debe tener conectividad/credenciales de lectura para MongoDB Atlas.
3. Define `GCP_PROJECT_ID`; opcionalmente `BIGQUERY_DATASET` (por defecto
   `mongo_data_warehouse`), `BIGQUERY_LOCATION` (`US`) y `BIGQUERY_BATCH_ROWS` (`5000`).
4. Ejecuta la transformación y construcción del Warehouse indicadas arriba, y luego:

   ```powershell
   python scripts/exportar_dwh_bigquery.py
   ```

El script crea el dataset si no existe. Para cada colección, carga lotes en una tabla staging
con esquema explícito y reemplaza la tabla publicada solo cuando terminó de leer MongoDB.
Así también publica el esquema cuando la colección está vacía y no deja visible una tabla
parcial si falla la extracción. Las fechas se tipan como `DATE` o `TIMESTAMP`, los ObjectId
se convierten en `STRING` y las listas/campos anidados mantienen su estructura analítica.

Configuración local opcional en `.env` (sin incluir credenciales):

```dotenv
GCP_PROJECT_ID=mi-proyecto-gcp
BIGQUERY_DATASET=mongo_data_warehouse
BIGQUERY_LOCATION=US
BIGQUERY_BATCH_ROWS=5000
GOOGLE_APPLICATION_CREDENTIALS=C:\ruta\segura\gcp-bigquery.json
```

No guardes el JSON de credenciales en el repositorio. En producción, prefiere identidad
administrada/federada y concede los permisos mínimos requeridos.

## Cambios respecto al documento

- **Fin de semana:** el documento compara `dia == 1 or dia == 7`, que usa el día del mes.
  Aquí se usa `$dayOfWeek` (1 = domingo, 7 = sábado), que es lo correcto.
- **create_collection + drop:** se eliminó. La carga es idempotente con `$merge`
  y un índice único en cada llave; el borrado solo ocurre con `--reconstruir`.
- **$first determinista:** en `dim_usuario` se ordena por fecha antes de agrupar,
  así `$first` toma el nombre del comentario más reciente y no uno arbitrario.
- **Datos sucios:** `sample_mflix` tiene años y calificaciones con texto; se limpian
  con `$convert` (`onError`) en lugar de romper la carga.
- **Validación:** al final se cuentan hechos sin coincidencia en cada dimensión
  (integridad referencial), además del resumen con `count_documents`.
- **Zona horaria:** variable `DW_ZONA_HORARIA` en `.env` (por defecto `UTC`).

## Relación con los entregables

| Entregable            | Implementación                                   |
|-----------------------|--------------------------------------------------|
| Modelo estrella       | `dim_*` + `fact_comentarios`                     |
| Transformación ETL    | `aggregate()` en `construir_data_warehouse.py`   |
| Carga                 | `$merge` con índices únicos                      |
| Optimización          | `allowDiskUse=True`, índices en llaves foráneas y cubos materializados |
| Consultas analíticas  | `consultas_data_warehouse.py` (6 consultas BI)   |
