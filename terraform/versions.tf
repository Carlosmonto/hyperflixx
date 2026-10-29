terraform {
  required_version = ">= 1.11.0"

  required_providers {
    mongodbatlas = {
      source  = "mongodb/mongodbatlas"
      version = "~> 2.19"
    }
  }

  backend "s3" {
    key     = "mongo-data-lake/terraform.tfstate"
    region  = "us-east-2"
    encrypt = true
  }
}

provider "mongodbatlas" {
  public_key  = var.atlas_public_key
  private_key = var.atlas_private_key
}