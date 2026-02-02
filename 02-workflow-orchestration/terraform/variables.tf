variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "us-central1-a"
}

variable "prefix" {
  description = "Prefix for BigQuery datasets"
  type        = string
  default     = "trips_data"
}

variable "bucket_name" {
  description = "GCS Bucket name"
  type        = string
}