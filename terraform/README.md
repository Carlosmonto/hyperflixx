# Terraform para MongoDB Atlas

Esta configuracion provisiona la infraestructura minima que necesita el proyecto:

- un proyecto Atlas;
- un cluster M0 tipo replicaset, suficiente para `sample_mflix` y Change Streams;
- un usuario SCRAM con `readWrite` solo sobre `sample_mflix`;
- una lista de acceso configurable por CIDR.

Terraform no crea colecciones ni indices. Esa responsabilidad permanece en
`scripts/crear_estructura.py`, `scripts/transformar_oltp_a_olap.py` y
`scripts/construir_data_warehouse.py`, que son idempotentes para los datos del proyecto.

## Uso

1. Crea una API key de Atlas con permisos suficientes para administrar el proyecto.
2. Edita `terraform.tfvars` y reemplaza los marcadores por tus valores reales.
3. Configura `allowed_cidrs`; `0.0.0.0/0` permite acceso desde cualquier IP y solo debe
	usarse en entornos de laboratorio.
4. Ejecuta desde esta carpeta:

```powershell
terraform init
terraform fmt -check
terraform validate
terraform plan
terraform apply
```

El estado local puede contener identificadores y metadatos de infraestructura. Para un
entorno compartido usa un backend remoto cifrado antes de aplicar en CI/CD.

## GitHub Actions

El workflow [`../.github/workflows/deploy.yml`](../.github/workflows/deploy.yml) ejecuta
la validación Python y Terraform en cada pull request. En cada `push` a `main` o `master`
ejecuta `terraform plan`; `terraform apply` queda en el environment protegido
`production` para requerir aprobación antes de modificar Atlas.

Configura estos secretos del repositorio antes de usar el workflow:

- `MONGODB_ATLAS_PUBLIC_KEY`
- `MONGODB_ATLAS_PRIVATE_KEY`
- `MONGODB_ATLAS_ORG_ID`
- `MONGODB_ATLAS_DATABASE_PASSWORD`
- `MONGODB_ATLAS_ALLOWED_CIDRS` como conjunto Terraform JSON, por ejemplo
  `["203.0.113.10/32"]`

El workflow no ejecuta automáticamente los scripts CDC, generación masiva, generación
de volumen S3 ni `construir_data_warehouse.py --reconstruir`, porque modifican datos o
son procesos de larga duración. Las credenciales se entregan mediante secretos de
GitHub; no se usa `.env` ni `terraform.tfvars`.

`terraform.tfvars` solo configura recursos de Atlas. El archivo `.env` pertenece a la
aplicacion Python y contiene la URI que usaran los scripts despues de crear el cluster.

Tras crear el cluster, actualiza los marcadores de `.env` con la URI SRV y las credenciales
del usuario provisionado:

```dotenv
MONGO_URI=mongodb+srv://data_lake_app:<PASSWORD>@<CLUSTER_HOST>/sample_mflix?retryWrites=true&w=majority
MONGO_DB=sample_mflix
```

Después ejecuta `python scripts/crear_estructura.py` para crear indices y las colecciones
analiticas del proyecto.

## Cluster Atlas existente

No ejecutes esta configuracion contra el cluster actual sin adaptar el estado. Para
adoptarlo, importa primero el proyecto y el cluster con sus IDs reales, y revisa el plan:

```powershell
terraform import mongodbatlas_project.data_lake <ATLAS_PROJECT_ID>
terraform import mongodbatlas_advanced_cluster.data_lake <ATLAS_PROJECT_ID>-<CLUSTER_NAME>
```

El usuario y las entradas CIDR existentes también deben importarse o declararse fuera de
Terraform para evitar duplicados.

## Seguridad

No guardes `terraform.tfvars`, `.tfstate`, API keys ni contraseñas en Git. La contraseña
se entrega mediante `password_wo`, por lo que Terraform 1.11 o posterior no la escribe en
el estado del recurso. Rota inmediatamente cualquier credencial que haya sido expuesta en
un archivo `.env` o en logs.