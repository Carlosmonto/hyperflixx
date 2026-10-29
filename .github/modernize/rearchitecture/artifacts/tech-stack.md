# Stack tecnológico existente

## Lenguaje y runtime

- Python 3.10 o superior, comprobado por `iac_setup.py`.
- Terraform `>= 1.11.0`.
- Spark local mediante PySpark; en Windows requiere `tools/hadoop/bin/winutils.exe`.

## Dependencias declaradas

`requirements.txt` declara: `pymongo`, `boto3`, `google-cloud-bigquery`, `Faker`,
`certifi`, `python-dotenv`, `pandas`, `pyarrow`, `pyspark`, `matplotlib` y `openpyxl`.
No se fijan versiones en el archivo de requisitos.

## Servicios y proveedores

- MongoDB Atlas mediante `mongodb/mongodbatlas ~> 2.19`, con cluster M0 `REPLICASET`.
- Amazon S3 mediante `boto3` para RAW, PROCESSED, CURATED y objetos de demostración.
- Google BigQuery mediante `google-cloud-bigquery` para el warehouse publicado.
- GitHub Actions aún no está presente en el repositorio.

## Configuración

`python-dotenv` carga `.env`. Las variables relevantes incluyen `MONGO_URI`, `MONGO_DB`,
`DATA_LAKE_URI`, `AWS_PROFILE`, `AWS_REGION`, `GCP_PROJECT_ID`, `BIGQUERY_DATASET`,
`BIGQUERY_LOCATION`, `BIGQUERY_BATCH_ROWS`, `DW_ZONA_HORARIA`, parámetros de benchmark
y parámetros de generación sintética. `.env`, `terraform.tfvars`, estado Terraform,
credenciales GCP y datos generados están excluidos por `.gitignore`.

## Restricciones observadas

- `terraform.tfstate` y `terraform.tfstate.backup` están presentes localmente aunque
  `.gitignore` los excluye; no deben incorporarse al control de versiones.
- Terraform usa una lista pública de CIDR para acceso Atlas; CI necesita una estrategia
  explícita de red/allowlist.
- No hay framework de pruebas ni configuración de lint/format declarada.
- La falta de versiones fijadas hace que una ejecución limpia de CI no sea reproducible.
