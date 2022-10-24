output "region" {
  value = var.region
}

output "account_id" {
  value = data.aws_caller_identity.current.account_id
}

output "cluster_name" {
  value = aws_eks_cluster.flink-tests.name
}

output "cluster_endpoint" {
  value = aws_eks_cluster.flink-tests.endpoint
}

output "certificate_authority_data" {
  value     = aws_eks_cluster.flink-tests.certificate_authority[0].data
  sensitive = true
}

output "secret_key" {
  value = aws_iam_access_key.user.secret
  sensitive   = true
}

output "access_key" {
  value = aws_iam_access_key.user.id
}
