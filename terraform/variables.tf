variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "gamboge-etl"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-2"
}

# Totesys Database Credentials
variable "totesys_db_host" {
  description = "Totesys database host"
  type        = string
}

variable "totesys_db_name" {
  description = "Totesys database name"
  type        = string
  default     = "totesys"
}

variable "totesys_db_user" {
  description = "Totesys database username"
  type        = string
}

variable "totesys_db_password" {
  description = "Totesys database password"
  type        = string
  sensitive   = true
}

variable "totesys_db_port" {
  description = "Totesys database port"
  type        = number
  default     = 5432
}

# Alerting
variable "alert_email" {
  description = "Email for alerts"
  type        = string
}

# Lambda Configuration
variable "lambda_timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 300
}

variable "lambda_memory_size" {
  description = "Lambda memory in MB"
  type        = number
  default     = 512
}

variable "lambda_runtime" {
  description = "Lambda runtime"
  type        = string
  default     = "python3.11"
}

# Schedule
variable "ingestion_schedule" {
  description = "EventBridge schedule expression"
  type        = string
  default     = "rate(15 minutes)"
}