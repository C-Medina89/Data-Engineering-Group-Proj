data "archive_file" "loading_lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/../loading_lambda.zip"

  excludes = [
    "__pycache__",
    "*.pyc",
    "test_*.py",
  ]
}


resource "aws_lambda_function" "loading" {
  function_name = "${var.project_name}-loading-${var.environment}"
  role          = aws_iam_role.lambda_exec.arn
  runtime       = var.lambda_runtime


  handler = "loading.lambda_handler.lambda_handler"

  filename         = data.archive_file.loading_lambda.output_path
  source_code_hash = data.archive_file.loading_lambda.output_base64sha256

   architectures = ["x86_64"]

  layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:24"]

 


 vpc_config {
    subnet_ids         = slice(data.aws_subnets.default.ids, 0, 2)  # Use first 2 subnets
    security_group_ids = [aws_security_group.lambda_sg.id]
  }
  
  timeout     = var.lambda_timeout
  memory_size = var.lambda_memory_size

  environment {
    variables = {
      PROCESSED_BUCKET_NAME = aws_s3_bucket.processed_zone.bucket
      ENVIRONMENT           = var.environment
      LOG_LEVEL             = "INFO"
    }
  }

  tags = {
    Name        = "${var.project_name}-loading-lambda"
    Stage       = "Week3-Loading"
    Project = var.project_name
  }
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "loading_logs" {
  name              = "/aws/lambda/${aws_lambda_function.loading.function_name}"
  retention_in_days = 7

  tags = {
    Stage = "Week3-Loading"
  }
}
