provider "aws" {
  region = var.aws_region
}

data "aws_availability_zones" "available" {}

# VPC
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "4.0.2"

  name = "sallma-vpc"
  cidr = var.vpc_cidr

  azs             = slice(data.aws_availability_zones.available.names, 0, 2)
  public_subnets  = var.public_subnets
  private_subnets = var.private_subnets

  enable_nat_gateway = true
  enable_vpn_gateway = false

  tags = var.tags
}

# EKS Cluster
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "20.8.4"

  cluster_name    = var.cluster_name
  cluster_version = var.cluster_version
  subnet_ids      = module.vpc.private_subnets
  vpc_id          = module.vpc.vpc_id

  enable_irsa = true

  eks_managed_node_groups = {
    ray-head = {
      desired_size   = 1
      max_size       = 2
      min_size       = 1
      instance_types = [var.eks_instance_type]
      labels         = { role = "ray-head" }
      tags           = { NodeGroup = "ray-head" }
    }
    ray-worker = {
      desired_size   = 2
      max_size       = 4
      min_size       = 2
      instance_types = [var.eks_instance_type]
      labels         = { role = "ray-worker" }
      tags           = { NodeGroup = "ray-worker" }
    }
  }

  tags = var.tags
}

# IAM Role for Jenkins (Least Privilege)
resource "aws_iam_role" "jenkins_role" {
  name = "sallma-jenkins-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "ec2.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "jenkins_ci_policy" {
  role       = aws_iam_role.jenkins_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "jenkins_ecr_policy" {
  role       = aws_iam_role.jenkins_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryFullAccess"
}

# IRSA Role
data "aws_iam_policy_document" "irsa_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:sub"
      values   = ["system:serviceaccount:default:s3-reader"]
    }

    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }

    effect = "Allow"
  }
}

resource "aws_iam_role" "irsa_role" {
  name               = "sallma-irsa-role"
  assume_role_policy = data.aws_iam_policy_document.irsa_assume_role_policy.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "irsa_policy" {
  role       = aws_iam_role.irsa_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}
