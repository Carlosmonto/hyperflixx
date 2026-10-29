# Documentación integral del proyecto Mongo Data Lake

## 1. Propósito

Este repositorio implementa una plataforma académica de datos basada en el dataset
`sample_mflix` de MongoDB. El proyecto demuestra:

- conexión de una aplicación Python con MongoDB Atlas;
- separación OLTP, OLAP y Data Warehouse;
- construcción de un Data Lake local por zonas;
- captura de cambios mediante MongoDB Change Streams;
- procesamiento batch con pandas, PyArrow y PySpark;
- consultas analíticas, benchmarking y dashboard de monitoreo;
- Infraestructura como Código con Terraform;
- preparación reproducible del entorno local con `iac_setup.py`.
- generación automatizada de catálogo arquitectónico y diccionario Excel.

El proyecto no contiene una aplicación web ni una API HTTP. Los procesos se ejecutan como
scripts Python desde Windows y escriben resultados en MongoDB Atlas y en el sistema de
archivos local.

## 2. Arquitectura general

```text
CAPA DE INGESTA / OLTP                       CAPA DATA WAREHOUSE / OLAP
MongoDB Atlas / sample_mflix                 BigQuery / mongo_data_warehouse
movies, comments, users, theaters            dim_*, fact_comentarios, agg_*
       |                                                ^
       +--> transformar_oltp_a_olap.py                  |
       |       peliculas_analiticas                     |
       +--> construir_data_warehouse.py                  |
               Warehouse derivado MongoDB               |
                       +--> exportar_dwh_bigquery.py ----+
                            ETL Python por lotes + staging

Rutas paralelas desde Atlas:
  Change Streams --> CDC JSONL / resume token
  pipeline_data_lake.py --> S3: RAW -> PROCESSED -> CURATED (Parquet)
  Spark --> peliculas_por_genero
```

### 2.1 Capas

| Capa | Tecnología | Responsabilidad |
|---|---|---|
| Fuente OLTP | MongoDB Atlas / `sample_mflix` | Catálogo de películas, comentarios, usuarios y cines |
| OLAP | Colección `peliculas_analiticas` | Películas enriquecidas y desnormalizadas para análisis |
| Streaming | Change Streams + JSONL | Captura de inserts, updates y deletes de `movies` |
| Data Lake RAW | JSONL/BSON extendido | Copia sin transformación y eventos CDC |
| Data Lake PROCESSED | Parquet | Datos limpios, tipados y aplanados |
| Data Lake CURATED | Parquet | Datos preparados para BI y agregaciones |
| Warehouse derivado/local | Colecciones MongoDB | Dimensiones, hechos y cubos para consultas locales y benchmarks |
| Data Warehouse cloud | Google BigQuery | Copia tipada de dimensiones, hechos y agregados para OLAP |
| Observabilidad | JSON + PNG | Benchmark, latencias, almacenamiento y SLA |
| Infraestructura | Terraform Atlas | Proyecto, cluster, usuario y acceso de red |
| Preparación local | `iac_setup.py` | Python, dependencias, carpetas y health check |

## 3. Dependencias y entorno

Las dependencias están declaradas en `requirements.txt`:

- `pymongo`: driver Python para MongoDB y Atlas.
- `dnspython`: resolución DNS utilizada por las URI `mongodb+srv://`.
- `certifi`: bundle de certificados disponible para TLS.
- `python-dotenv`: carga de variables desde `.env`.
- `boto3`: acceso opcional a Amazon S3 para el Data Lake batch.
- `pandas`: transformación tabular en el pipeline batch.
- `pyarrow`: escritura de Parquet.
- `pyspark`: procesamiento distribuido/local con Spark.
- `matplotlib`: generación del dashboard PNG.

El proyecto está orientado a Python 3.10 o superior. En Windows se utiliza `.venv`.

### 3.1 Configuración de aplicación

`.env` contiene configuración de ejecución y no debe versionarse. Las variables usadas son:

| Variable | Uso |
|---|---|
| `MONGO_URI` | URI `mongodb+srv://` para Atlas |
| `MONGO_DB` | Base seleccionada por defecto: `sample_mflix` |
| `DW_ZONA_HORARIA` | Zona horaria del Data Warehouse; por defecto `UTC` |
| `GCP_PROJECT_ID` | Proyecto de Google Cloud que contiene BigQuery |
| `BIGQUERY_DATASET` | Dataset cloud; por defecto `mongo_data_warehouse` |
| `BIGQUERY_LOCATION` | Región del dataset; por defecto `US` |
| `BIGQUERY_BATCH_ROWS` | Filas por lote de carga; por defecto `5000` |
| `BENCH_SLA_MS` | SLA del benchmark; por defecto `499` ms |
| `BENCH_REPETICIONES` | Repeticiones del benchmark; por defecto `3` |
| `MFLIX_NUM_PELICULAS` | Películas sintéticas; por defecto `5000` |
| `MFLIX_COMENTARIOS_MIN` | Mínimo de comentarios sintéticos |
| `MFLIX_COMENTARIOS_MAX` | Máximo de comentarios sintéticos |
| `MFLIX_BATCH_SIZE` | Tamaño de lote de inserción |
| `ATLAS_PLAN` | Nombre mostrado en dashboard |
| `ATLAS_COSTO_MENSUAL` | Costo mostrado en dashboard |
| `ATLAS_LIMITE_MB` | Límite de almacenamiento mostrado en dashboard |
| `DATA_LAKE_URI` | URI S3 obligatoria para el pipeline batch; por ejemplo `s3://mi-bucket/prefijo` |
| `AWS_REGION` | Región AWS usada por boto3 cuando `DATA_LAKE_BACKEND=s3` |
| `AWS_PROFILE` | Perfil opcional de AWS usado por boto3; por defecto usa la cadena estándar |
| `DATA_LAKE_READ_PREFERENCE` | Preferencia de lectura MongoDB del pipeline; por defecto `secondaryPreferred` |

Los valores reales de `.env`, `terraform.tfvars`, claves API y contraseñas nunca deben
incluirse en esta documentación.

## 4. Infraestructura como Código

El proyecto tiene dos mecanismos complementarios:

1. **Terraform** administra recursos remotos de MongoDB Atlas.
2. **`iac_setup.py`** prepara el entorno local y valida la conexión.

### 4.1 Terraform para Atlas

La configuración vive en `terraform/` y usa el provider oficial
`mongodb/mongodbatlas` versión `~> 2.19`, con Terraform `>= 1.11.0`.

Recursos definidos en `terraform/main.tf`:

- `mongodbatlas_project.data_lake`: proyecto Atlas.
- `mongodbatlas_advanced_cluster.data_lake`: cluster M0 multi-tenant, replicaset, AWS,
  región `US_EAST_1`.
- `mongodbatlas_project_ip_access_list.allowed`: entradas CIDR del proyecto.
- `mongodbatlas_database_user.application`: usuario SCRAM con rol `readWrite` sobre
  `sample_mflix` y alcance limitado al cluster.

El cluster tiene `prevent_destroy = true` para reducir el riesgo de borrar datos por
accidente. Terraform no crea colecciones MongoDB, índices ni documentos; esa responsabilidad
permanece en los scripts Python.

#### Archivos Terraform

| Archivo | Responsabilidad |
|---|---|
| `terraform/versions.tf` | Versión mínima de Terraform, provider y credenciales del provider |
| `terraform/variables.tf` | Variables, tipos, defaults y validaciones |
| `terraform/main.tf` | Proyecto, cluster, usuario, lista de acceso y migración de recurso legacy |
| `terraform/outputs.tf` | Project ID, nombre del cluster y plantilla de URI SRV |
| `terraform/terraform.tfvars` | Valores locales reales; está excluido de Git |
| `terraform/.terraform.lock.hcl` | Versiones verificadas del provider |
| `terraform/README.md` | Uso, importación, seguridad y operación |

`allowed_cidrs = ["0.0.0.0/0"]` permite acceso desde cualquier IP y solo es apropiado
para un laboratorio temporal. Para un entorno real debe utilizarse la IP pública con `/32`,
PrivateLink o VPC peering.

#### Flujo Terraform recomendado

Para un proyecto nuevo:

```powershell
cd C:\Users\ASUS\Desktop\mongo\terraform
terraform init
terraform fmt
terraform validate
terraform plan
terraform apply
```

Para el proyecto Atlas ya existente `bd_datos`, primero debe importarse el estado. El
Project ID actual es el identificador hexadecimal de 24 caracteres mostrado en Atlas. La
API key del provider debe tener acceso al proyecto; el usuario de MongoDB no sustituye una
API key de Atlas.

```powershell
terraform import mongodbatlas_project.data_lake PROJECT_ID
terraform import mongodbatlas_advanced_cluster.data_lake PROJECT_ID-Cluster0
terraform import mongodbatlas_database_user.application PROJECT_ID/USUARIO/admin
terraform import 'mongodbatlas_project_ip_access_list.allowed["0.0.0.0/0"]' PROJECT_ID-0.0.0.0/0
terraform plan
```

Si `terraform plan` continúa mostrando `Plan: 4 to add`, el estado no contiene los
recursos existentes o la importación no se completó.

#### Estado actual de Terraform

La configuración valida sintácticamente y genera un plan, pero la adopción del proyecto
existente está bloqueada por:

```text
HTTP 401 Unauthorized
USER_CANNOT_ACCESS_GROUP
```

La causa es que la API key configurada en `terraform.tfvars` no tiene permisos sobre el
proyecto `bd_datos`, pertenece a otra organización o está limitada por la lista de acceso
administrativa de Atlas. Debe corregirse en `Organization Access Manager -> API Keys`.

### 4.2 Orquestador local `iac_setup.py`

`iac_setup.py` no crea recursos remotos. Su función es reproducir el entorno local:

1. verifica Python 3.10 o superior;
2. crea `.venv` si no existe;
3. instala `requirements.txt` dentro de `.venv`;
4. crea las carpetas base del Data Lake;
5. valida la existencia de `.env` y el formato de `MONGO_URI`;
6. ejecuta un health check mediante `conexion.py`;
7. muestra el siguiente paso Terraform.

Se ejecuta desde la raíz:

```powershell
python iac_setup.py
```

El script no activa el entorno en la shell padre, porque un proceso hijo no puede modificar
la sesión actual de PowerShell. En su lugar, usa directamente el ejecutable de `.venv` para
instalar dependencias y realizar el health check.

## 5. Conexión con MongoDB Atlas

`conexion.py` es el punto único de conexión de todos los scripts:

- carga `.env`;
- obtiene `MONGO_URI` y `MONGO_DB`;
- crea `MongoClient` con `connectTimeoutMS=20000` y
  `serverSelectionTimeoutMS=30000`;
- ejecuta `admin.command("ping")`;
- devuelve `(cliente, base_de_datos)`;
- convierte fallos de conexión, configuración y autenticación en `RuntimeError`;
- cierra el cliente en los scripts consumidores.

El proyecto no configura un MongoDB local. MongoDB se usa como tecnología de base de datos
y Atlas como servicio administrado remoto.

### 5.1 Problema observado de conectividad

Durante la revisión, Atlas aparecía activo en la consola, pero PyMongo reportaba:

```text
SSL handshake failed
TLSV1_ALERT_INTERNAL_ERROR
ReplicaSetNoPrimary
```

El error ocurre durante la negociación TLS con los nodos Atlas, antes de autenticar. Las
causas pendientes de revisar son red, proxy, antivirus, inspección TLS, firewall o lista de
acceso administrativa de Atlas. No se debe solucionar desactivando la validación TLS.

## 6. Modelo de datos MongoDB

### 6.1 Colecciones de origen `sample_mflix`

- `movies`: catálogo de películas, rating IMDb, géneros, países, año, reparto y premios.
- `comments`: comentarios vinculados a películas mediante `movie_id`.
- `users`: usuarios registrados vinculados por `email`.
- `theaters`: cines y ubicación geoespacial.
- `sessions`: datos del dump original.
- `embedded_movies`: datos embebidos del dump original.

### 6.2 Colección OLAP

`peliculas_analiticas` es creada y poblada por los procesos OLAP. Contiene campos
renombrados y enriquecidos como:

- `movie_id`, `titulo`, `anio`, `fecha_estreno`;
- `tipo`, `clasificacion`, `generos`, `paises`, `idiomas`;
- `calificacion_imdb`, `votos_imdb`;
- `calificacion_audiencia`, `resenas_audiencia`;
- `premios_ganados`, `nominaciones`;
- `total_comentarios`, `ultimo_comentario`, `fecha_actualizacion`.

### 6.3 Data Warehouse

El modelo estrella usa:

- `dim_tiempo`: calendario por día con año, mes, trimestre, semestre, día de semana y
  marca de fin de semana.
- `dim_usuario`: perfil agregado por email, actividad, películas comentadas y segmento.
- `dim_pelicula`: catálogo analítico con década, género, país, rating y rango de calificación.
- `fact_comentarios`: hechos de comentarios con fecha, usuario, película, cantidad y longitud.
- `agg_comentarios_diarios`: cubo diario de comentarios.
- `agg_genero_decada`: cubo por género y década.
- `agg_genero_top`: top de películas por género.
- `agg_pais_rating`: rating promedio por país.
- `agg_genero_premios`: tasa de películas premiadas por género.

Las cargas usan `$merge` e índices únicos para ser incrementales e idempotentes. La opción
`--reconstruir` elimina las colecciones del warehouse antes de reconstruirlas y debe usarse
solo cuando se acepta perder esas colecciones derivadas.

## 7. Inventario de scripts Python

### 7.1 Preparación y conexión

| Archivo | Qué implementa |
|---|---|
| `conexion.py` | Cliente MongoDB Atlas, selección de base, ping y manejo de errores |
| `iac_setup.py` | Orquestación local reproducible, dependencias, carpetas y health check |
| `scripts/crear_estructura.py` | Índices OLTP, colección OLAP e índices de `peliculas_analiticas` |

### 7.2 Ingesta y Data Lake

| Archivo | Qué implementa |
|---|---|
| `data_lake_storage.py` | Backend `local`/`s3` para JSONL y Parquet del pipeline batch |
| `scripts/pipeline_data_lake.py` | Extrae `movies`, genera RAW JSONL, procesa con pandas y escribe Parquet mediante el backend configurado |
| `scripts/ingesta_streaming.py` | Consume Change Streams de `movies`, escribe eventos JSONL y guarda resume token |
| `scripts/productor_cdc.py` | Genera inserts, updates y deletes para probar el consumidor CDC |
| `scripts/spark_jobs.py` | Lee el RAW más reciente, limpia, explota géneros y genera Parquet analítico |
| `scripts/generar_datos_masivos.py` | Genera películas/comentarios sintéticos en lotes para pruebas de escala |

### 7.3 ETL, warehouse y BI

| Archivo | Qué implementa |
|---|---|
| `scripts/transformar_oltp_a_olap.py` | `$lookup` de comentarios y `$merge` hacia `peliculas_analiticas` |
| `scripts/construir_data_warehouse.py` | Dimensiones, hechos, cubos, índices, validación referencial y cargas `$merge` en MongoDB |
| `scripts/exportar_dwh_bigquery.py` | ETL desde las colecciones del Warehouse MongoDB hacia tablas de BigQuery |
| `scripts/consultas_data_warehouse.py` | Seis consultas BI sobre el modelo estrella |
| `scripts/benchmark_consultas_analiticas.py` | Diez consultas, índices selectivos, `explain`, mediana y SLA |
| `scripts/benchmarking_consultas.py` | Comparación de agregación OLTP frente a OLAP |
| `scripts/dashboard_monitoreo.py` | PNG de seis paneles con colecciones, latencia, storage, tendencia y SLA |
| `scripts/generar_catalogo.py` | Inspección de colecciones, flattening, tipos, claves, índices y exportación Excel |

### 7.4 Catálogo Excel

El script `scripts/generar_catalogo.py` implementa el entregable de gobierno y catálogo de
datos pedido para el proyecto. Usa pandas y openpyxl para generar
`Catalogo_Datos_MongoDataLake_YYYYMMDD.xlsx` en la raíz, con dos hojas:

- `Resumen Arquitectonico`: colecciones, zona OLTP/OLAP/Data Warehouse, cantidad de
  documentos, tamaño estimado, campos, índices y descripción.
- `Diccionario de Datos`: colección, campo aplanado, tipo de dato, Primary Key, Foreign
  Key, indicador de índice, muestra y descripción de negocio.

El catálogo inspecciona la base real, toma un documento de muestra por colección, aplana
objetos anidados como `imdb.rating`, conserva listas como `Array`, consulta los índices
internos de MongoDB y clasifica automáticamente las colecciones `dim_*`, `fact_*` y `agg_*`.

Ejecución:

```powershell
python scripts/generar_catalogo.py
```

La conexión debe estar funcionando antes de ejecutarlo. El archivo Excel generado es un
artefacto local y no debe versionarse si contiene muestras de datos sensibles.

## 8. Flujo de ejecución completo

### 8.1 Preparar o adoptar Atlas

1. Configurar API key de Atlas con permisos suficientes.
2. Ejecutar `terraform init`, `validate` y `plan`.
3. Si el proyecto/cluster ya existe, importarlos al estado.
4. Aplicar Terraform solo cuando el plan no intente crear recursos existentes.

### 8.2 Preparar entorno local

```powershell
cd C:\Users\ASUS\Desktop\mongo
python iac_setup.py
```

### 8.3 Preparar estructura MongoDB

```powershell
python scripts/crear_estructura.py
```

### 8.4 Cargar y transformar datos

```powershell
python scripts/pipeline_data_lake.py
python scripts/spark_jobs.py
python scripts/transformar_oltp_a_olap.py
python scripts/construir_data_warehouse.py
```

Para separar el Data Warehouse cloud de MongoDB, usa Google BigQuery Sandbox o un proyecto
de Google Cloud con BigQuery habilitado. Configura una cuenta de servicio con permisos para
crear datasets y cargar tablas, y define `GOOGLE_APPLICATION_CREDENTIALS` apuntando a su
JSON sin versionarlo:

```dotenv
GCP_PROJECT_ID=mi-proyecto-gcp
BIGQUERY_DATASET=mongo_data_warehouse
BIGQUERY_LOCATION=US
BIGQUERY_BATCH_ROWS=5000
GOOGLE_APPLICATION_CREDENTIALS=C:\ruta\segura\gcp-bigquery.json
```

Después de construir el Warehouse en MongoDB, ejecuta:

```powershell
python scripts/exportar_dwh_bigquery.py
```

El script exporta `dim_tiempo`, `dim_usuario`, `dim_pelicula`, `fact_comentarios` y los
cinco agregados `agg_*` como tablas independientes. MongoDB conserva la ingesta OLTP y el
Warehouse cloud queda disponible para consultas OLAP separadas.

El pipeline batch publica RAW, PROCESSED y CURATED directamente en Amazon S3. Configura
las credenciales mediante el mecanismo estándar de AWS (perfil, variables de entorno o rol
IAM) y añade al `.env`:

```dotenv
DATA_LAKE_URI=s3://hyperflixx
AWS_REGION=us-east-2
DATA_LAKE_READ_PREFERENCE=secondaryPreferred
```

Después ejecuta el mismo comando `python scripts/pipeline_data_lake.py`. Los objetos se
publican conservando las particiones existentes:

```text
s3://abp-data-lake-juanpablo/raw/movies/fecha_carga=YYYY-MM-DD/movies.jsonl
s3://abp-data-lake-juanpablo/processed/movies/fecha_carga=YYYY-MM-DD/movies.parquet
s3://abp-data-lake-juanpablo/curated/movies/fecha_carga=YYYY-MM-DD/movies.parquet
```

La integración S3 cubre el pipeline batch. El CDC y Spark continúan usando rutas locales,
porque su escritura incremental y su lectura `s3a://` requieren una adaptación independiente.
No guardes `AWS_ACCESS_KEY_ID` ni `AWS_SECRET_ACCESS_KEY` en el repositorio.

El pipeline batch usa `secondaryPreferred` porque solo lee `movies`; esto permite continuar
durante una incidencia temporal del nodo primario. Los procesos que escriben en MongoDB
mantienen la selección primaria predeterminada.

Para reconstruir solo los derivados del warehouse:

```powershell
python scripts/construir_data_warehouse.py --reconstruir
```

### 8.5 Probar CDC

Terminal 1:

```powershell
python scripts/ingesta_streaming.py
```

Terminal 2:

```powershell
python scripts/productor_cdc.py --op all --iterations 3 --delay 3
```

Los eventos quedan en `data_lake/raw/streaming/movies/fecha_carga=YYYY-MM-DD/eventos.jsonl`.

### 8.6 Ejecutar BI y monitoreo

```powershell
python scripts/consultas_data_warehouse.py
python scripts/benchmark_consultas_analiticas.py
python scripts/dashboard_monitoreo.py
```

El dashboard puede recalcular el benchmark:

```powershell
python scripts/dashboard_monitoreo.py --benchmark
```

## 9. Data Lake en S3

Las rutas lógicas se particionan por fecha de carga en el bucket S3:

```text
data_lake/
├── raw/
│   ├── movies/fecha_carga=YYYY-MM-DD/movies.jsonl
│   └── streaming/movies/
│       ├── fecha_carga=YYYY-MM-DD/eventos.jsonl
│       └── resume_token.json
├── processed/
│   └── movies/fecha_carga=YYYY-MM-DD/movies.parquet
├── curated/
│   ├── movies/fecha_carga=YYYY-MM-DD/movies.parquet
│   └── spark/peliculas_por_genero/fecha_carga=YYYY-MM-DD/*.parquet
└── sample_mflix_import/
    └── sample_mflix/*.bson y metadatos
```

RAW, PROCESSED y CURATED están ignorados por Git para evitar versionar grandes volúmenes
de datos. `data_lake/sample_mflix_import` contiene el dump BSON original y sus archivos de
metadatos para una posible importación manual.

No se crean carpetas locales para el pipeline batch: cada ruta lógica se publica como una
clave de objeto bajo `DATA_LAKE_URI`. boto3 usa la cadena de credenciales estándar de AWS y
el bucket debe existir antes de ejecutar el pipeline.

## 10. Spark y Hadoop en Windows

`scripts/spark_jobs.py` ejecuta Spark en modo `local[*]`. Antes de escribir configura
`HADOOP_HOME` y `hadoop.home.dir` para el proceso actual.

`tools/hadoop/bin/winutils.exe` es el binario requerido por Hadoop/Spark en Windows. Su
procedencia, rama y SHA-256 están documentados en `tools/hadoop/README.md`. Si falta, el job
termina con un mensaje que indica consultar esa guía.

## 11. Benchmark y observabilidad

El benchmark registra en `reportes/benchmark_ultimo.json`:

- latencia de red;
- latencia media, mediana y máxima;
- consultas dentro del SLA;
- consultas con índice;
- plan `IXSCAN`, `COLLSCAN` o `MIXTO`;
- número de registros analíticos;
- índices utilizados.

`dashboard_monitoreo.py` genera un PNG con:

1. documentos por colección y capa;
2. latencia frente al SLA;
3. datos e índices por colección;
4. tendencia mensual de comentarios;
5. uso del límite de almacenamiento;
6. resumen ejecutivo.

La latencia de red es un límite inferior real para cualquier consulta Atlas. Por eso el SLA
debe interpretarse junto con el ping y no solo con el tiempo de agregación.

## 12. Archivos y artefactos adicionales

- `.env`: secretos y configuración local, excluido de Git.
- `.gitignore`: excluye secretos, entorno virtual, cachés, estado Terraform y datos derivados.
- `pyspark_install.log`: registro de instalación de PySpark.
- `pyspark_install_error.log`: registro de errores de instalación, actualmente vacío según el inventario.
- `reportes/benchmark_ultimo.json`: último resultado persistido del benchmark.
- `data_lake/sample_mflix_import`: dump BSON y metadatos de `sample_mflix`.
- `terraform/.terraform`: plugins descargados del provider; no debe versionarse.
- `terraform/.terraform.lock.hcl`: lockfile que sí debe conservarse para reproducibilidad.
- `__pycache__`: caché de Python, excluida de Git.

## 13. Seguridad y operación

- No versionar `.env`, `terraform.tfvars`, contraseñas ni claves privadas de Atlas.
- Rotar cualquier credencial que haya sido pegada en chats, logs o documentación.
- `0.0.0.0/0` permite acceso desde cualquier IP; usarlo solo temporalmente en laboratorio.
- Para producción usar CIDR restringido, PrivateLink/VPC peering y backend remoto cifrado.
- No ejecutar `terraform apply` sobre recursos existentes hasta importarlos o confirmar que se
  desea crear una infraestructura nueva.
- No usar `tlsAllowInvalidCertificates`; oculta problemas y debilita la seguridad.
- El consumidor CDC usa un token en disco local; para alta disponibilidad se necesita
  almacenamiento duradero y coordinación de consumidores.
- El Data Lake local no tiene catálogo, retención ni almacenamiento compartido; una evolución
  productiva debería usar object storage.

## 14. Estado actual

Implementado:

- conexión centralizada a MongoDB Atlas;
- Data Lake local por zonas y particiones diarias;
- backend configurable del Data Lake batch para filesystem local o Amazon S3;
- ETL OLTP a OLAP;
- Data Warehouse estrella con cubos agregados;
- Change Streams con resume token;
- Spark local para salida analítica por género;
- benchmarking, `explain()` y dashboard;
- Terraform para Atlas;
- orquestador local `iac_setup.py`;
- documentación base en `docs/`.

Pendiente para considerar la infraestructura operativa:

- resolver el acceso de la API key de Terraform al proyecto Atlas (`USER_CANNOT_ACCESS_GROUP`);
- decidir si se adopta el proyecto existente mediante `terraform import` o se crea uno nuevo;
- resolver el handshake TLS de PyMongo en el entorno de ejecución;
- reemplazar el acceso global `0.0.0.0/0` por una política de red restringida;
- mover estado Terraform a backend remoto si participa más de una persona o CI/CD;
- adaptar CDC a objetos inmutables por lote y adaptar Spark a `s3a://` para una migración
  completa del Data Lake a object storage;
