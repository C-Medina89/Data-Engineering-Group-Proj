terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
    backend "s3" {
    bucket   = "gamboge-state-bucket"
    key      = "terraform.tfstate"
    region = "eu-west-2" 
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName = "The ETL Data Pipeline Project"
      DeployedFrom = "Terraform"
      Repository = "Gamboge-project"
    }
  }
}