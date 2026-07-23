# dagron Helm chart

DAG workflow engine for Kubernetes. This chart deploys the OSS stack — the
engine/API, the `dagron-api` UI gateway, the Next.js frontend, and a throwaway
in-cluster Postgres for testing. Runs on kind / k3d / k3s / EKS.

- **Source:** https://github.com/lucheeseng827/dagron
- **Images:** `mancube/dagron-engine`, `mancube/dagron-api`, `mancube/dagron-frontend` (multi-arch: `linux/amd64`, `linux/arm64`)
- **License:** Apache-2.0

## Install (OCI)

The chart is published to Docker Hub as an OCI artifact (Helm ≥ 3.8):

```console
helm install dagron oci://registry-1.docker.io/mancube/dagron --version 0.4.0
```

Inspect values first:

```console
helm show values oci://registry-1.docker.io/mancube/dagron --version 0.4.0
```

## Values

| Key | Description | Default |
| --- | --- | --- |
| `dagron.engine.image` | Engine/API image | `mancube/dagron-engine:<chart version>` |
| `dagron.dagronApi.image` | UI gateway image | `mancube/dagron-api:<chart version>` |
| `dagron.frontend.image` | Frontend image | `mancube/dagron-frontend:<chart version>` |
| `global.imageRegistry` | Relocate every image to a private mirror | `""` |

The bundled Postgres is for testing only — point the engine at a managed
Postgres for anything real. See [`values.yaml`](./values.yaml) for the full set.

## Artifact Hub

Chart metadata (screenshots, images, links, category) is carried in
[`Chart.yaml`](./Chart.yaml) via `artifacthub.io/*` annotations. To claim the
repository as a **Verified Publisher**, register the OCI repo on Artifact Hub
(`oci://registry-1.docker.io/mancube/dagron`), then push the ownership metadata
once with the repository ID Artifact Hub assigns:

```console
# after filling repositoryID and owners.email locally in artifacthub-repo.yml
oras push \
  registry-1.docker.io/mancube/dagron:artifacthub.io \
  artifacthub-repo.yml:application/vnd.cncf.artifacthub.repository-metadata.layer.v1.yaml
```
