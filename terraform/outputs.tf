output "atlas_project_id" {
  description = "Atlas project ID."
  value       = mongodbatlas_project.data_lake.id
}

output "atlas_cluster_name" {
  description = "Atlas cluster name."
  value       = mongodbatlas_advanced_cluster.data_lake.name
}

output "mongo_uri_template" {
  description = "SRV URI template for the Python application; insert credentials outside Terraform."
  value = format(
    "mongodb+srv://%s:<PASSWORD>@%s/%s?retryWrites=true&w=majority",
    var.database_username,
    trimprefix(mongodbatlas_advanced_cluster.data_lake.connection_strings.standard_srv, "mongodb+srv://"),
    var.database_name,
  )
  sensitive = true
}