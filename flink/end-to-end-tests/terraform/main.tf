resource "random_string" "run_id" {
  length  = 6
  special = false
  numeric = false
  lower   = true
  upper   = false
}

module "networking" {
  source = "./modules/networking"

  availability_zone1 = var.availability_zone1
  availability_zone2 = var.availability_zone2
}

module "storage" {
  source = "./modules/storage"

  test_data_bucket_name = var.test_data_bucket_name
}

module "flink_session_cluster" {
  source = "./modules/processing-eks"

  run_id = random_string.run_id.result

  vpc_id     = module.networking.vpc_id
  subnet1_id = module.networking.subnet1_id
  subnet2_id = module.networking.subnet2_id

  region                = var.region
  test_data_bucket_name = var.test_data_bucket_name
  eks_workers           = var.eks_workers

  tags = var.tags

  depends_on = [module.networking, module.storage]
}
