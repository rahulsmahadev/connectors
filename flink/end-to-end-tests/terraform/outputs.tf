output "eks_cluster_name" {
  value = module.flink_session_cluster.cluster_name
}

output "eks_cluster_endpoint" {
  value = module.flink_session_cluster.cluster_endpoint
}

output "account_id" {
  value = module.flink_session_cluster.account_id
}

output "region" {
  value = module.flink_session_cluster.region
}

output "secret_key" {
  value     = module.flink_session_cluster.secret_key
  sensitive = true
}

output "access_key" {
  value     = module.flink_session_cluster.access_key
  sensitive = true
}

output "test_data_bucket_name" {
  value = module.storage.test_data_bucket_name
}