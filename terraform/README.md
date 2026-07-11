# Terraform deployment (alternative to `kubectl apply -f k8s.yaml`)

This is a Terraform equivalent of `../k8s.yaml` + `../k8s-monitoring.yaml`,
using the [`hashicorp/kubernetes`](https://registry.terraform.io/providers/hashicorp/kubernetes/latest)
provider. Same resources, same result -- declared and applied through
Terraform instead of raw `kubectl apply`, which gets you a plan/diff before
changes land, tracked state, and clean `terraform destroy` teardown.

Kubernetes itself is unchanged either way: Terraform's `kubernetes` provider
just talks to the same Kubernetes API server `kubectl` does. This doesn't
replace Kubernetes -- it's a different way of telling it what you want.

## Prerequisites

Same as the rest of the project (`kind`, `kubectl`, `docker`), plus:
- [`terraform`](https://developer.hashicorp.com/terraform/install) >= 1.5
- A running `kind` cluster with the `orbitcp:latest` image already loaded
  (run `./setup.sh` once first, or manually: `kind create cluster --name orbitcp-demo`
  then `docker save orbitcp:latest -o /tmp/img.tar && kind load image-archive /tmp/img.tar --name orbitcp-demo`)

## Usage

```bash
cd terraform
terraform init
terraform plan      # review what will be created
terraform apply
```

Then port-forward (commands also printed as a Terraform output):

```bash
kubectl -n orbitcp port-forward svc/orbitcp 8080:8080 &
kubectl -n orbitcp port-forward svc/grafana 3000:3000 &
```

Tear down:

```bash
terraform destroy
```

## Configuration

All tunable via `-var` flags or a `terraform.tfvars` file:

| Variable | Default | Notes |
|---|---|---|
| `replicas` | `5` | Must be odd and >= 3 (enforced by a validation block) |
| `image` | `orbitcp:latest` | Must already be loaded into the kind cluster |
| `enable_monitoring` | `true` | Set `false` to skip Prometheus/Grafana entirely |
| `postgres_password` | (demo default) | Override for anything beyond local demo use |
| `grafana_admin_password` | (demo default) | Same |

Example: deploy a 3-node cluster with monitoring disabled:

```bash
terraform apply -var="replicas=3" -var="enable_monitoring=false"
```

Try that and compare against `../k8s.yaml`, which hardcodes 5 replicas and a
literal comma-joined `CLUSTER` string -- the Terraform version computes both
from `var.replicas` via a `for` expression (see `locals.cluster_addrs` in
`main.tf`), so changing cluster size is a one-line change instead of hand-editing
a peer list. That's the actual practical case for IaC here, not just "it's
the same YAML in a different syntax."
