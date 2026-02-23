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
  dataset_id = var.bq_dataset_raw
  location   = var.location
}

# Silver/Gold Layer: dbt Transformations Dataset
resource "google_bigquery_dataset" "dbt_dataset" {
  dataset_id = var.bq_dataset_dbt
  location   = var.location
}

# GCS Bucket for Data Lake
resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
}