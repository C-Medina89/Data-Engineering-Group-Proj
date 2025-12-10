# S3 landing Zone 

variable "project_name" {

    description = "Name of project"
    type =string
    default = "gamboge-etl-pipeline"
}


variable "db_host" {
  description = "Database hostname for ingestion Lambda"
  type        = string
}

variable "db_name" {
  description = "Database name"
  type        = string
}

variable "db_user" {
  description = "Database username"
  type        = string
}

variable "db_password" {
  description = "Database password"
  type        = string
  sensitive   = true
}

variable "db_port" {
  description = "Database port"
  type        = number
  default     = 5432
}