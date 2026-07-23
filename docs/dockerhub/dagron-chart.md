# dagron Helm chart (`oci://registry-1.docker.io/mancube/dagron`)

**One-command deploy of the dagron stack to Kubernetes — engine + API + console + a throwaway in-cluster Postgres.**

A Helm chart published as an **OCI artifact**. It wires the three dagron images together (`dagron-engine`, `dagron-api`, `dagron-frontend`, all `linux/amd64` + `linux/arm64`) plus an optional test Postgres, with ingress and secrets. Runs on kind / k3d / k3s / EKS.

- **Artifact:** `oci://registry-1.docker.io/mancube/dagron`
- **Deploys:** engine · dagron-api · frontend · (optional) Postgres
- **Website:** dagron.dev · **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## Versions

| Version | Notes |
|---|---|
| `0.4.3` | pulls the `0.4.3` images |

## Install

```bash
helm install dagron oci://registry-1.docker.io/mancube/dagron --version 0.4.3 \
  -n dagron --create-namespace \
  --set ingress.enabled=true --set-string ingress.host='dagron.your-host.example.com' \
  --set ingress.tls.enabled=true --set-string ingress.tls.secretName='dagron-tls' \
  --set-string dagronApi.jwtSecret='replace-with-a-32+char-signing-key' \
  --set-string dagronApi.admin.password='replace-with-a-strong-password'
```

Inspect first:

```bash
helm show values oci://registry-1.docker.io/mancube/dagron --version 0.4.3
helm template dagron oci://registry-1.docker.io/mancube/dagron --version 0.4.3   # render without installing
```

## Common values

| Value | Meaning |
|---|---|
| `engine.image` / `dagronApi.image` / `frontend.image` | image refs (default the matching `0.4.3` tags). |
| `dagronApi.jwtSecret` | **required** — session-cookie signing key (≥32 chars). |
| `dagronApi.admin.{email,password,name}` | bootstrap admin user. |
| `ingress.*` | host + TLS for the console. |
| `postgres.enabled` | `true` deploys a **throwaway** in-cluster Postgres (testing only). |
| `externalDatabaseUrl` | set `postgres.enabled=false` and point at a managed DB for anything real. |

> **Production:** disable the in-cluster Postgres and use a managed database; set a strong `jwtSecret` and admin password; enable ingress TLS.
>
> The GitOps **operator** is a separate, optional component and is **not** part of this chart.
