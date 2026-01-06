# Get existing VPC and subnet information
data "aws_vpc" "default" {
    default = true
}
data "aws_subnets" "default" {
    filter {
        name    = "vpc-id"
        values  = [data.aws_vpc.default.id]
    }
}
# security group for lambda functions
resource "aws_security_group" "lambda_sg" {
    name    = "${var.project_name}-lambda-sg"
    description  = "Security group for lambda functions in VPC"
    vpc_id       = data.aws_vpc.default.id
    # Allow all outbound traffic
    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }
    tags ={
        Name  = "${var.project_name}-lambda-sg"
        Stage = "Week3-Loading"
    }
}
# security group for RDS
resource "aws_security_group" "rds_sg" {
    name    = "${var.project_name}-rds-sg"
    description  = "Security group for RDS PostgresSQL"
    vpc_id       = data.aws_vpc.default.id
    # Allow PostgresSQL traffic from lambda security group only
    ingress {
        from_port       = 5432
        to_port         = 5432
        protocol        = "tcp"
        security_groups = [aws_security_group.lambda_sg.id]
    }
    # Allow all outbound traffic from RDS (for updates etc..)
    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }
    tags ={
        Name  = "${var.project_name}-rds-sg"
        Stage = "Week3-Loading"
    }
}
# RDS subnet group
resource "aws_db_subnet_group" "warehouse" {
    name       = "${var.project_name}-dw-subnet-group"
    subnet_ids = data.aws_subnets.default.ids
    tags ={
        Name  = "Data Warehouse Subnet Group"
        Stage = "Week3-Loading"
    }
}
# RDS PostgresSQL Instance
resource "aws_db_instance" "data_warehouse" {
    identifier         = "${var.project_name}-dw-${var.environment}"
    engine             = "postgres"
    engine_version     = var.rds_engine_version
    instance_class     = var.rds_instance_class
    allocated_storage  = var.rds_allocated_storage
    # Database credentials - store in Secrets Manager later
    db_name   = "totesys_warehouse"
    username  = var.dw_db_username
    password  = var.dw_db_password
    # VPC Configuration
    publicly_accessible      = false

    db_subnet_group_name = aws_db_subnet_group.warehouse.name

    vpc_security_group_ids   = [aws_security_group.rds_sg.id]
    # Backup and Maintanence
    backup_retention_period = var.rds_backup_retention
    backup_window           = "03:00-04:00"
    maintenance_window      = "sun:04:00-sun:05:00"
    # # Performance & Availabilty
    # multi_az          = var.rds_multi_az
    # storage_encrypted = true
    # storage_type      ="gp2"
    # Database settings
    port    = 5432
    # parameter_group_name = "default.postgres15"
    # Deletion protection ( false for dev, true for prod)
    deletion_protection  = false
    skip_final_snapshot  = true
    tags = {
      Name        = "Totesys Data Warehouse"
      Environment = var.environment
      Stage       = "Week3-Loading"
      Project     = var.project_name
  }
}

# Store RDS credentials in secret manager
resource "aws_secretsmanager_secret" "dw_creds" {
    name = "${var.project_name}/dw/${var.environment}"
    description = "Data warehouse RDS credentials"
    tags ={
        Stage       = "Week3-Loading"
        Environment = var.environment
        Database    = "warehouse"
    }
}


resource "aws_secretsmanager_secret_version" "dw_creds_value" {
    secret_id = aws_secretsmanager_secret.dw_creds.id
    secret_string = jsonencode({
        host      = aws_db_instance.data_warehouse.address
        port      = aws_db_instance.data_warehouse.port
        database  = aws_db_instance.data_warehouse.db_name
        username  = aws_db_instance.data_warehouse.username
        password  = aws_db_instance.data_warehouse.password
    })
}
