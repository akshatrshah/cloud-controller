terraform {
  required_version = ">= 1.5"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.31"
    }
  }
}

# Points at whatever kubeconfig context is currently active (set by
# `kind create cluster`, which automatically switches your context to the
# new cluster). No cloud credentials, no remote backend -- this targets
# the same local kind cluster kubectl already talks to.
provider "kubernetes" {
  config_path    = var.kubeconfig_path
  config_context = var.kube_context
}
