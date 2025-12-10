resource "aws_secretsmanager_secret" "db_creds" {
  name        = "${var.project_name}-db-credentials"
  description = "RDS credentials for ${var.project_name} ingestion lambda"
}

# The actual secret value (JSON)
resource "aws_secretsmanager_secret_version" "db_creds_value" {
  secret_id = aws_secretsmanager_secret.db_creds.id

  secret_string = jsonencode({
    user     = var.db_user
    password = var.db_password
    host     = var.db_host
    database = var.db_name
    port     = var.db_port
  })
}