terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0.0"
    }
  }
}

# provider configuration
provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# --------------------------------------------------------------------------------
# 🥉 BRONZE LAYER: Raw Data (GCS Bucket)
# --------------------------------------------------------------------------------
resource "google_storage_bucket" "bronze_bucket" {
  name          = "${var.project_id}-bronze-lake"
  location      = var.region
  force_destroy = true # allows deletion even if bucket contains data

  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  # lifecycle rule to clean up failed/interrupted uploads
  lifecycle_rule {
    condition {
      age = 1 # days
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }

  labels = {
    env  = "dev"
    tier = "bronze"
  }
}

# --------------------------------------------------------------------------------
# 🥈 SILVER LAYER: Cleaned Data (BigQuery Dataset)
# --------------------------------------------------------------------------------
resource "google_bigquery_dataset" "silver_dataset" {
  dataset_id  = "${var.dataset_prefix}_silver"
  project     = var.project_id
  location    = var.region
  description = "Silver Layer: Cleaned and normalized taxi trip data."

  labels = {
    env  = "dev"
    tier = "silver"
  }
}

# --------------------------------------------------------------------------------
# 🥇 GOLD LAYER: Analytics Data (BigQuery Dataset)
# --------------------------------------------------------------------------------
resource "google_bigquery_dataset" "gold_dataset" {
  dataset_id  = "${var.dataset_prefix}_gold"
  project     = var.project_id
  location    = var.region
  description = "Gold Layer: Aggregated and reporting-ready business metrics."

  labels = {
    env  = "dev"
    tier = "gold"
  }
}