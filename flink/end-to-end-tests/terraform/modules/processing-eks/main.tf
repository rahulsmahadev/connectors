/* ========== EKS ========== */

resource "aws_eks_cluster" "flink-tests" {
  name     = "flink-tests-eks-cluster-${var.run_id}"
  role_arn = aws_iam_role.flink-tests.arn

  vpc_config {
    subnet_ids = [var.subnet1_id, var.subnet2_id]
  }

  # Ensure that IAM Role permissions are created before and deleted after EKS Cluster handling.
  # Otherwise, EKS will not be able to properly delete EKS managed EC2 infrastructure such as Security Groups.
  depends_on = [
    aws_iam_role.flink-tests,
    aws_iam_role_policy_attachment.flink-tests-AmazonEKSClusterPolicy,
    aws_iam_role_policy_attachment.flink-tests-AmazonEKSVPCResourceController,
  ]
}

resource "aws_eks_node_group" "flink-tests" {
  cluster_name    = aws_eks_cluster.flink-tests.name
  node_group_name = "flink-tests-node-group-${var.run_id}"
  subnet_ids      = [var.subnet1_id]
  instance_types  = []
  node_role_arn   = aws_iam_role.flink-tests_node_group.arn

  launch_template {
    name    = aws_launch_template.flink-tests.name
    version = "$Latest"
  }

  scaling_config {
    desired_size = var.eks_workers
    max_size     = var.eks_workers
    min_size     = var.eks_workers
  }

  # Ensure that IAM Role permissions are created before and deleted after EKS Node Group handling.
  # Otherwise, EKS will not be able to properly delete EC2 Instances and Elastic Network Interfaces.
  depends_on = [
    aws_iam_role_policy_attachment.flink-tests-AmazonEKSWorkerNodePolicy,
    aws_iam_role_policy_attachment.flink-tests-AmazonEKS_CNI_Policy,
    aws_iam_role_policy_attachment.flink-tests-AmazonEC2ContainerRegistryReadOnly,
  ]
}

resource "aws_launch_template" "flink-tests" {
  instance_type = "i3.2xlarge"
  name          = "flink-tests-lt-${var.run_id}"
  lifecycle {
    create_before_destroy = true
  }
  tag_specifications {
    resource_type = "instance"
    tags          = var.tags
  }
}

/* ========== IAM ========== */

# In order to provide fine-grained permissions, IAM roles for service accounts feature should be used:
# https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html
# Currently S3A connector is configured to use EnvironmentVariableCredentialsProvider, because
# WebIdentityTokenCredentialsProvider is supported by aws-java-sdk as of version 1.11.704
# Currently com.amazonaws:aws-java-sdk-bundle:1.11.271 is used (hadoop-aws:3.1.0 dependency),
# which does not support WebIdentityTokenCredentialsProvider.

resource "aws_iam_access_key" "user" {
  user    = aws_iam_user.user.name
}

resource "aws_iam_user" "user" {
  name = "flink-tests-user"
  path = "/"
}

resource "aws_iam_user_policy" "user_policy" {
  name = "flink-tests-user-policy"
  user = aws_iam_user.user.name

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::${var.test_data_bucket_name}"
            ],
            "Action": [
                "s3:ListBucket"
            ]
        },
        {
            "Effect": "Allow",
            "Resource": "arn:aws:s3:::${var.test_data_bucket_name}/*",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ]
        }
    ]
}
EOF
}


resource "aws_iam_role" "flink-tests" {
  name = "flink-tests-eks-cluster-role-${var.run_id}"

  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "eks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy_attachment" "flink-tests-AmazonEKSClusterPolicy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.flink-tests.name
}

resource "aws_iam_role_policy_attachment" "flink-tests-AmazonEKSVPCResourceController" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSVPCResourceController"
  role       = aws_iam_role.flink-tests.name
}

resource "aws_iam_role" "flink-tests_node_group" {
  name = "flink-tests-eks-node-group-role-${var.run_id}"

  assume_role_policy = jsonencode({
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
    Version = "2012-10-17"
  })
}

resource "aws_iam_role_policy_attachment" "flink-tests-AmazonEKSWorkerNodePolicy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.flink-tests_node_group.name
}

resource "aws_iam_role_policy_attachment" "flink-tests-AmazonEC2ContainerRegistryReadOnly" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.flink-tests_node_group.name
}

resource "aws_iam_role_policy_attachment" "flink-tests-AmazonEKS_CNI_Policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.flink-tests_node_group.name
}
