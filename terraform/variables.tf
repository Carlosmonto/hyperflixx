variable "atlas_public_key" {
  description = "Public API key for MongoDB Atlas."
  type        = string
  sensitive   = true
}

variable "atlas_private_key" {
  description = "Private API key for MongoDB Atlas."
  type        = string
  sensitive   = true
}

variable "atlas_org_id" {
  description = "MongoDB Atlas organization ID that owns the project."
  type        = string
}

variable "project_name" {
  description = "Atlas project name for the data platform."
  type        = string
  default     = "mongo-data-lake"
}

variable "cluster_name" {
  description = "Atlas cluster name."
  type        = string
  default     = "data-lake-cluster"
}

variable "atlas_region" {
  description = "AWS region used by the Atlas shared cluster."
  type        = string
  default     = "US_EAST_1"
}

variable "database_username" {
  description = "Application database username."
  type        = string
  default     = "data_lake_app"
}

variable "database_password" {
  description = "Application database password. Terraform passes it write-only to Atlas."
  type        = string
  sensitive   = true
}

variable "database_name" {
  description = "Database used by the Python pipelines."
  type        = string
  default     = "sample_mflix"
}

variable "allowed_cidrs" {
  description = "CIDR blocks allowed to connect to Atlas. Prefer one public IP with /32."
  type        = set(string)

  validation {
    condition     = length(var.allowed_cidrs) > 0
    error_message = "Define at least one CIDR for the Atlas access list."
  }
}