terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

# Bronze Layer: Raw Dataset 
resource "google_bigquery_dataset" "raw_dataset" {
  dataset_id                 = var.bq_dataset_raw
  location                   = var.location
  delete_contents_on_destroy = true 
}

# Silver/Gold Layer: dbt Transformations Dataset 
resource "google_bigquery_dataset" "dbt_dataset" {
  dataset_id                 = var.bq_dataset_dbt
  location                   = var.location
  delete_contents_on_destroy = true
}

# GCS Bucket for Data Lake
resource "google_storage_bucket" "data_lake" {
  name                        = var.gcs_bucket_name
  location                    = var.location
  force_destroy               = true
  storage_class               = var.gcs_storage_class
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}