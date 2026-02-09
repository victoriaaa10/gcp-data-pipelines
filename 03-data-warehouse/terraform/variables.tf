variable "project" {
  description = "GCP Project ID"
}

variable "region" {
  default = "us-central1"
}

variable "gcs_bucket_name" {
  description = "GCS Bucket Name (Globally Unique)"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset for NYC Yellow Taxi 2024 (Jan-June)"
}

variable "location" {
  default = "US"
}