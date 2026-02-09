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
  # No 'credentials', it defaults to ADC (gcloud login)
}

# Create the GCS Bucket (Data Lake)
resource "google_storage_bucket" "gcs_bucket" {
  name                        = var.gcs_bucket_name
  location                    = var.location
  force_destroy               = true
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

# Create the BigQuery Dataset (Data Warehouse)
resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
  delete_contents_on_destroy = true
}