# S3 Landing Zone bucket
resource "aws_s3_bucket" "landing_zone" {
  bucket = "gamboge-etl-landing-zone" 
  force_destroy = True

  tags = {
    Name = "etl-landing-zone"
  }
}

# Version control stops files being overwritten
resource "aws_s3_bucket_versioning" "landing_zone_versioning" {
  bucket = aws_s3_bucket.landing_zone.id

    versioning_configuration {
    status = "Enabled"
  }
}    
