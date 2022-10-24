resource "aws_s3_bucket" "test_data" {
  bucket = var.test_data_bucket_name
}

resource "aws_s3_bucket_versioning" "test_data" {
  bucket = aws_s3_bucket.test_data.id
  versioning_configuration {
    status = "Disabled"
  }
}
