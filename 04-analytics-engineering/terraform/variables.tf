variable "project" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "Region for GCP resources"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  type        = string
  default     = "US"
}

variable "bq_dataset_raw" {
  description = "BigQuery Dataset for Raw Data (Bronze Layer)"
  type        = string
  default     = "nytaxi_trips_raw"
}

variable "bq_dataset_dbt" {
  description = "BigQuery dataset for dbt analytics models (Silver/Gold layer)"
  type        = string
  default     = "nytaxi_trips_dbt"
}

variable "gcs_bucket_name" {
  description = "Storage Bucket Name (globally unique)"
  type        = string
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  type        = string
  default     = "STANDARD"
}