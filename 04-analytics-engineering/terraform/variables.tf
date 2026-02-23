variable "project" {
  description = "GCP Project ID"
  default     = "project-id-redacted" 
}

variable "region" {
  description = "Region for resources"
  default     = "US"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_raw" {
  description = "Raw Data Dataset (Bronze Layer)"
  default     = "trips_data_all"
}

variable "bq_dataset_dbt" {
  description = "dbt Transformations Dataset (Silver/Gold Layer)"
  default     = "nytaxi"
}

variable "gcs_bucket_name" {
  description = "Storage Bucket Name (Must be globally unique)"
  default     = "nytaxi-bucket-project-id-redacted"
}
