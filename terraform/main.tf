resource "kubernetes_namespace" "orbitcp" {
  metadata {
    name = var.namespace
  }
}

resource "kubernetes_secret" "postgres_credentials" {
  metadata {
    name      = "postgres-credentials"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
  }
  data = {
    POSTGRES_PASSWORD = var.postgres_password
  }
  type = "Opaque"
}

resource "kubernetes_service" "postgres" {
  metadata {
    name      = "postgres"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
  }
  spec {
    selector = { app = "postgres" }
    port {
      port        = 5432
      target_port = 5432
    }
  }
}

resource "kubernetes_stateful_set" "postgres" {
  metadata {
    name      = "postgres"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
  }
  spec {
    service_name = kubernetes_service.postgres.metadata[0].name
    replicas     = 1
    selector {
      match_labels = { app = "postgres" }
    }
    template {
      metadata {
        labels = { app = "postgres" }
      }
      spec {
        container {
          name  = "postgres"
          image = "postgres:16-alpine"
          port {
            container_port = 5432
          }
          env {
            name  = "POSTGRES_DB"
            value = "orbitcp"
          }
          env {
            name  = "POSTGRES_USER"
            value = "orbitcp"
          }
          env {
            name = "POSTGRES_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.postgres_credentials.metadata[0].name
                key  = "POSTGRES_PASSWORD"
              }
            }
          }
          volume_mount {
            name       = "data"
            mount_path = "/var/lib/postgresql/data"
          }
          resources {
            requests = { cpu = "50m", memory = "64Mi" }
            limits   = { cpu = "200m", memory = "256Mi" }
          }
          readiness_probe {
            exec {
              command = ["pg_isready", "-U", "orbitcp"]
            }
            initial_delay_seconds = 3
            period_seconds         = 5
          }
        }
      }
    }
    volume_claim_template {
      metadata {
        name = "data"
      }
      spec {
        access_modes = ["ReadWriteOnce"]
        resources {
          requests = { storage = "1Gi" }
        }
      }
    }
  }
}

# Headless service: stable peer DNS, exposing BOTH the HTTP API port and
# hashicorp/raft's own TCP consensus transport (a separate binary
# protocol, derived as HTTP port + 10000).
resource "kubernetes_service" "raft_headless" {
  metadata {
    name      = "raft-headless"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
    labels    = { app = "orbitcp" }
  }
  spec {
    cluster_ip = "None"
    selector   = { app = "orbitcp" }
    port {
      name        = "http"
      port        = 8080
      target_port = 8080
    }
    port {
      name        = "raft-transport"
      port        = 18080
      target_port = 18080
    }
  }
}

resource "kubernetes_service" "orbitcp" {
  metadata {
    name      = "orbitcp"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
    labels    = { app = "orbitcp" }
  }
  spec {
    type     = "ClusterIP"
    selector = { app = "orbitcp" }
    port {
      name        = "http"
      port        = 8080
      target_port = 8080
    }
  }
}

resource "kubernetes_pod_disruption_budget_v1" "orbitcp" {
  metadata {
    name      = "orbitcp-pdb"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
  }
  spec {
    min_available = var.replicas - 1
    selector {
      match_labels = { app = "orbitcp" }
    }
  }
}

locals {
  # Builds "raft-0.raft-headless:8080,raft-1.raft-headless:8080,..." for
  # however many replicas are configured.
  cluster_addrs = join(",", [
    for i in range(var.replicas) : "raft-${i}.raft-headless:8080"
  ])
}

resource "kubernetes_stateful_set" "raft" {
  metadata {
    name      = "raft"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
  }
  spec {
    service_name           = kubernetes_service.raft_headless.metadata[0].name
    replicas                = var.replicas
    pod_management_policy = "Parallel"
    selector {
      match_labels = { app = "orbitcp" }
    }
    template {
      metadata {
        labels = { app = "orbitcp" }
      }
      spec {
        affinity {
          pod_anti_affinity {
            preferred_during_scheduling_ignored_during_execution {
              weight = 100
              pod_affinity_term {
                topology_key = "kubernetes.io/hostname"
                label_selector {
                  match_labels = { app = "orbitcp" }
                }
              }
            }
          }
        }
        container {
          name               = "orbitcp"
          image              = var.image
          image_pull_policy = "IfNotPresent"
          port {
            container_port = 8080
            name           = "http"
          }
          port {
            container_port = 18080
            name           = "raft-transport"
          }
          env {
            name = "NODE_ID"
            value_from {
              field_ref {
                field_path = "metadata.name"
              }
            }
          }
          env {
            name  = "PORT"
            value = "8080"
          }
          env {
            name  = "CLUSTER"
            value = local.cluster_addrs
          }
          env {
            name = "POSTGRES_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.postgres_credentials.metadata[0].name
                key  = "POSTGRES_PASSWORD"
              }
            }
          }
          env {
            name  = "PG_DSN"
            value = "postgres://orbitcp:$(POSTGRES_PASSWORD)@postgres:5432/orbitcp?sslmode=disable"
          }
          resources {
            requests = { cpu = "25m", memory = "24Mi" }
            limits   = { cpu = "100m", memory = "64Mi" }
          }
          readiness_probe {
            http_get {
              path = "/healthz"
              port = 8080
            }
            initial_delay_seconds = 2
            period_seconds         = 5
          }
          liveness_probe {
            http_get {
              path = "/healthz"
              port = 8080
            }
            initial_delay_seconds = 5
            period_seconds         = 10
          }
        }
      }
    }
  }

  depends_on = [kubernetes_stateful_set.postgres]
}
