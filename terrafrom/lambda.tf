data "archive_file" "etl_lambda" {
  type = "zip"

  # Updated path to Lambda handler inside src/ingestion
  source_file = "${path.module}/../src/ingestion/lambda_handler.py"

  
  output_path      = "${path.module}/../src/ingestion/lambda_handler.zip"
  output_file_mode = "0666"
}
# THIS WILL HAVE TO CHANGE !! 

