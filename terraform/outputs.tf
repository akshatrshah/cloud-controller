output "namespace" {
  value = kubernetes_namespace.orbitcp.metadata[0].name
}

output "port_forward_commands" {
  value = <<-EOT
    kubectl -n ${kubernetes_namespace.orbitcp.metadata[0].name} port-forward svc/orbitcp 8080:8080
    kubectl -n ${kubernetes_namespace.orbitcp.metadata[0].name} port-forward svc/prometheus 9090:9090
    kubectl -n ${kubernetes_namespace.orbitcp.metadata[0].name} port-forward svc/grafana 3000:3000
  EOT
}

output "cluster_addrs" {
  description = "The CLUSTER env value computed for the configured replica count."
  value       = local.cluster_addrs
}
