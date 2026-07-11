variable "kubeconfig_path" {
  description = "Path to the kubeconfig file kind writes to on cluster creation."
  type        = string
  default     = "~/.kube/config"
}

variable "kube_context" {
  description = "kubectl context name for the kind cluster (kind sets this automatically to kind-<cluster-name>)."
  type        = string
  default     = "kind-orbitcp-demo"
}

variable "namespace" {
  description = "Kubernetes namespace all resources are created in."
  type        = string
  default     = "orbitcp"
}

variable "replicas" {
  description = "Number of control-plane (Raft) nodes. Must be odd for a clean quorum majority."
  type        = number
  default     = 5

  validation {
    condition     = var.replicas % 2 == 1 && var.replicas >= 3
    error_message = "replicas must be an odd number >= 3 so quorum (majority) is well-defined."
  }
}

variable "image" {
  description = "Container image for the control-plane app (must already be loaded into kind, e.g. via 'kind load image-archive')."
  type        = string
  default     = "orbitcp:latest"
}

variable "postgres_password" {
  description = "Password for the Postgres instance backing Raft's durable storage."
  type        = string
  default     = "orbitcp-demo-password"
  sensitive   = true
}

variable "grafana_admin_password" {
  description = "Admin password for the Grafana instance."
  type        = string
  default     = "orbitcp-demo"
  sensitive   = true
}

variable "enable_monitoring" {
  description = "Whether to deploy Prometheus + Grafana alongside the control plane."
  type        = bool
  default     = true
}
