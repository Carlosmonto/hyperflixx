resource "mongodbatlas_project" "data_lake" {
  name   = var.project_name
  org_id = var.atlas_org_id
}

resource "mongodbatlas_advanced_cluster" "data_lake" {
  project_id   = mongodbatlas_project.data_lake.id
  name         = var.cluster_name
  cluster_type = "REPLICASET"

  replication_specs = [
    {
      region_configs = [
        {
          electable_specs = {
            instance_size = "M0"
          }
          provider_name         = "TENANT"
          backing_provider_name = "AWS"
          region_name           = var.atlas_region
          priority              = 7
        }
      ]
    }
  ]

  lifecycle {
    prevent_destroy = true
  }
}

resource "mongodbatlas_project_ip_access_list" "allowed" {
  for_each   = var.allowed_cidrs
  project_id = mongodbatlas_project.data_lake.id
  cidr_block = each.value
  comment    = "Acceso del entorno local al pipeline Mongo Data Lake"
}

resource "mongodbatlas_database_user" "application" {
  project_id          = mongodbatlas_project.data_lake.id
  username            = var.database_username
  auth_database_name  = "admin"
  password_wo         = var.database_password
  password_wo_version = 3

  roles {
    role_name     = "readWrite"
    database_name = var.database_name
  }

  scopes {
    name = mongodbatlas_advanced_cluster.data_lake.name
    type = "CLUSTER"
  }
}

moved {
  from = mongodbatlas_cluster.data_lake
  to   = mongodbatlas_advanced_cluster.data_lake
}