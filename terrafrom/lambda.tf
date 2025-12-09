data "archive_file" "etl_lambda" {
  type = "zip"

  # etl_handler.py lives in ../lambda relative to terrafrom/
  source_file = "${path.module}/../lambda/etl_handler.py"

  # Terraform will create this zip file for you
  output_path      = "${path.module}/../lambda/etl_handler.zip"
  output_file_mode = "0666"
}
# THIS WILL HAVE TO CHANGE !! 
 
# data "archive_file" "etl_layer" {
#   type             = "zip"
#   source_dir       = "${path.module}/lambda_layer"
#   output_path      = "${path.module}/lambda/etl_layer.zip"
#   output_file_mode = "0666"
# }

# resource "aws_lambda_layer_version" "etl_dependencies" {
#   layer_name          = "gamboge-etl-dependencies"
#   compatible_runtimes = ["python3.12"]

#   filename         = data.archive_file.etl_layer.output_path
#   source_code_hash = data.archive_file.etl_layer.output_base64sha256
# }
