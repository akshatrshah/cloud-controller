locals {
  prometheus_scrape_targets = join("\n", [
    for i in range(var.replicas) : "              - 'raft-${i}.raft-headless:8080'"
  ])

  prometheus_config = <<-YAML
    global:
      scrape_interval: 5s
    scrape_configs:
      - job_name: 'orbitcp'
        static_configs:
          - targets:
    ${local.prometheus_scrape_targets}
        metrics_path: /metrics
  YAML
}

resource "kubernetes_config_map" "prometheus_config" {
  count = var.enable_monitoring ? 1 : 0
  metadata {
    name      = "prometheus-config"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
  }
  data = {
    "prometheus.yml" = local.prometheus_config
  }
}

resource "kubernetes_service" "prometheus" {
  count = var.enable_monitoring ? 1 : 0
  metadata {
    name      = "prometheus"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
  }
  spec {
    selector = { app = "prometheus" }
    port {
      port        = 9090
      target_port = 9090
    }
  }
}

resource "kubernetes_deployment" "prometheus" {
  count = var.enable_monitoring ? 1 : 0
  metadata {
    name      = "prometheus"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
  }
  spec {
    replicas = 1
    selector {
      match_labels = { app = "prometheus" }
    }
    template {
      metadata {
        labels = { app = "prometheus" }
      }
      spec {
        container {
          name  = "prometheus"
          image = "prom/prometheus:v2.53.0"
          args = [
            "--config.file=/etc/prometheus/prometheus.yml",
            "--storage.tsdb.retention.time=6h",
          ]
          port {
            container_port = 9090
          }
          volume_mount {
            name       = "config"
            mount_path = "/etc/prometheus"
          }
          resources {
            requests = { cpu = "25m", memory = "64Mi" }
            limits   = { cpu = "200m", memory = "256Mi" }
          }
        }
        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.prometheus_config[0].metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_secret" "grafana_admin" {
  count = var.enable_monitoring ? 1 : 0
  metadata {
    name      = "grafana-admin"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
  }
  data = {
    GF_SECURITY_ADMIN_PASSWORD = var.grafana_admin_password
  }
  type = "Opaque"
}

resource "kubernetes_config_map" "grafana_datasources" {
  count = var.enable_monitoring ? 1 : 0
  metadata {
    name      = "grafana-datasources"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
  }
  data = {
    "datasource.yml" = yamlencode({
      apiVersion = 1
      datasources = [{
        name      = "Prometheus"
        type      = "prometheus"
        access    = "proxy"
        url       = "http://prometheus:9090"
        isDefault = true
      }]
    })
  }
}

resource "kubernetes_config_map" "grafana_dashboard_provisioning" {
  count = var.enable_monitoring ? 1 : 0
  metadata {
    name      = "grafana-dashboard-provisioning"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
  }
  data = {
    "dashboards.yml" = yamlencode({
      apiVersion = 1
      providers = [{
        name    = "orbitcp"
        type    = "file"
        options = { path = "/var/lib/grafana/dashboards" }
      }]
    })
  }
}

resource "kubernetes_config_map" "grafana_dashboard_orbitcp" {
  count = var.enable_monitoring ? 1 : 0
  metadata {
    name      = "grafana-dashboard-orbitcp"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
  }
  data = {
    "orbitcp.json" = jsonencode({
      title    = "Orbit Control Plane"
      timezone = "browser"
      refresh  = "5s"
      time     = { from = "now-15m", to = "now" }
      panels = [
        {
          id = 1, title = "Current Leader (1 = leader, 0 = follower, per pod)"
          type = "timeseries", gridPos = { h = 8, w = 24, x = 0, y = 0 }
          targets = [{ expr = "orbitcp_raft_is_leader", legendFormat = "{{instance}}" }]
        },
        {
          id = 2, title = "Raft Term (per pod)"
          type = "timeseries", gridPos = { h = 8, w = 12, x = 0, y = 8 }
          targets = [{ expr = "orbitcp_raft_term", legendFormat = "{{instance}}" }]
        },
        {
          id = 3, title = "Commit Index (per pod)"
          type = "timeseries", gridPos = { h = 8, w = 12, x = 12, y = 8 }
          targets = [{ expr = "orbitcp_raft_commit_index", legendFormat = "{{instance}}" }]
        },
        {
          id = 4, title = "Resources by Lifecycle State"
          type = "timeseries", gridPos = { h = 8, w = 12, x = 0, y = 16 }
          targets = [{ expr = "max by (state) (orbitcp_resources_by_state)", legendFormat = "{{state}}" }]
        },
        {
          id = 5, title = "Elections Started vs Won (rate, per pod)"
          type = "timeseries", gridPos = { h = 8, w = 12, x = 12, y = 16 }
          targets = [
            { expr = "rate(orbitcp_elections_started_total[5m])", legendFormat = "started {{instance}}" },
            { expr = "rate(orbitcp_elections_won_total[5m])", legendFormat = "won {{instance}}" },
          ]
        },
      ]
    })
  }
}

resource "kubernetes_service" "grafana" {
  count = var.enable_monitoring ? 1 : 0
  metadata {
    name      = "grafana"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
  }
  spec {
    selector = { app = "grafana" }
    port {
      port        = 3000
      target_port = 3000
    }
  }
}

resource "kubernetes_deployment" "grafana" {
  count = var.enable_monitoring ? 1 : 0
  metadata {
    name      = "grafana"
    namespace = kubernetes_namespace.orbitcp.metadata[0].name
  }
  spec {
    replicas = 1
    selector {
      match_labels = { app = "grafana" }
    }
    template {
      metadata {
        labels = { app = "grafana" }
      }
      spec {
        container {
          name  = "grafana"
          image = "grafana/grafana:11.1.0"
          port {
            container_port = 3000
          }
          env {
            name  = "GF_SECURITY_ADMIN_USER"
            value = "admin"
          }
          env {
            name = "GF_SECURITY_ADMIN_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.grafana_admin[0].metadata[0].name
                key  = "GF_SECURITY_ADMIN_PASSWORD"
              }
            }
          }
          volume_mount {
            name       = "datasources"
            mount_path = "/etc/grafana/provisioning/datasources"
          }
          volume_mount {
            name       = "dashboard-provisioning"
            mount_path = "/etc/grafana/provisioning/dashboards"
          }
          volume_mount {
            name       = "dashboard-json"
            mount_path = "/var/lib/grafana/dashboards"
          }
          resources {
            requests = { cpu = "25m", memory = "64Mi" }
            limits   = { cpu = "200m", memory = "256Mi" }
          }
        }
        volume {
          name = "datasources"
          config_map {
            name = kubernetes_config_map.grafana_datasources[0].metadata[0].name
          }
        }
        volume {
          name = "dashboard-provisioning"
          config_map {
            name = kubernetes_config_map.grafana_dashboard_provisioning[0].metadata[0].name
          }
        }
        volume {
          name = "dashboard-json"
          config_map {
            name = kubernetes_config_map.grafana_dashboard_orbitcp[0].metadata[0].name
          }
        }
      }
    }
  }
}
