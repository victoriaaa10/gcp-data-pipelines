terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# 🥉 BRONZE: Raw Data (GCS Bucket)
resource "google_storage_bucket" "bronze_bucket" {
  name          = "${var.project_id}-bronze-lake"
  location      = var.region
  force_destroy = true # allows deletion with data inside

  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  # auto-clean failed uploads
  lifecycle_rule {
    condition {
      age = 1
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

# 🥈 SILVER: Cleaned Data (BigQuery)
resource "google_bigquery_dataset" "silver_dataset" {
  dataset_id  = "trips_data_silver"
  project     = var.project_id
  location    = var.region
  description = "Silver Layer: Cleaned and normalized taxi trip data."

  labels = {
    env  = "dev"
    tier = "silver"
  }
}

# 🥇 GOLD: Analytics Data (BigQuery)
resource "google_bigquery_dataset" "gold_dataset" {
  dataset_id  = "trips_data_gold"
  project     = var.project_id
  location    = var.region
  description = "Gold Layer: Aggregated and reporting-ready business metrics."

  labels = {
    env  = "dev"
    tier = "gold"
  }
}