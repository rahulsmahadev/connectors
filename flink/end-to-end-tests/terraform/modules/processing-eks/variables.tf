variable "run_id" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "region" {
  type = string
}

variable "subnet1_id" {
  type = string
}

variable "subnet2_id" {
  type = string
}

variable "test_data_bucket_name" {
  type = string
}

variable "eks_workers" {
  type = number
}

variable "tags" {
  type = map(string)
}
