output "eks_cluster_name" {
  value = module.eks.cluster_name
}

output "ray_head_node_group" {
  value = module.eks.eks_managed_node_groups["ray_head"].node_group_name
}

output "ray_worker_node_group" {
  value = module.eks.eks_managed_node_groups["ray_worker"].node_group_name
}

output "kubeconfig" {
  value       = module.eks.kubeconfig_filename
  description = "Path to generated kubeconfig file"
}
