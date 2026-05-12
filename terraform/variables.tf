variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus"
}

variable "project" {
  description = "Project name used in resource naming"
  type        = string
  default     = "cms-medallion"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "databricks_pat" {
  description = "Databricks personal access token for dbt workflow task"
  type        = string
  sensitive   = true
}

variable "alert_email" {
  description = "Email address for workflow failure alerts"
  type        = string
  default     = "jasonwalker15@gmail.com"
}
