# dagron Helm chart

DAG workflow engine for Kubernetes. This chart deploys the OSS stack — the
engine/API, the `dagron-api` UI gateway, the Next.js frontend, and a throwaway
in-cluster Postgres for testing. Runs on kind / k3d / k3s / EKS.

- **Source:** https://github.com/lucheeseng827/dagron
- **Images:** `mancube/dagron-engine`, `mancube/dagron-api`, and when enabled `mancube/dagron-gitops` and `mancube/dagron-ci` (multi-arch: `linux/amd64`, `linux/arm64`)
- **License:** Apache-2.0

## Install (OCI)

The chart is published to Docker Hub as an OCI artifact (Helm ≥ 3.8):

```console
helm install dagron oci://registry-1.docker.io/mancube/dagron --version 0.5.0
```

Inspect values first:

```console
helm show values oci://registry-1.docker.io/mancube/dagron --version 0.5.0
```

## Values

| Key | Description | Default |
| --- | --- | --- |
| `dagron.engine.image` | Engine/API image | `mancube/dagron-engine:<chart version>` |
| `dagron.dagronApi.image` | UI gateway image | `mancube/dagron-api:<chart version>` |
| `dagron.frontend.image` | Frontend image — off by default; the console ships in `dagron-api` | `mancube/dagron-frontend:0.8.1` (pinned; discontinued after 0.8.1) |
| `ci.enabled` | Deploy dagron-ci, the CI service: a container in the engine's pod and a Service, ready once the database answers. What it then does waits on the Secrets named under `ci` (a dagron-api token, a webhook secret, a GitHub App or token) | `false` |
| `ci.placement` | Where dagron-ci runs. `engine`: in the engine's pod, where the engine asks it for a private repository's checkout credential on the pod's own address. `deployment`: a Deployment of its own, which this chart's engine is not told to ask, so it serves public repositories. `values.yaml` says what sharing the pod means | `engine` |
| `ci.checkoutTokenSecret.name` | The Secret holding the secret the engine asks dagron-ci for a checkout's credential with. Without it a private repository's checkout fails | `""` |
| `ci.fleet.labels` | Fleet mode's label map: each label a job of GitHub Actions' own may ask for in `runs-on`, with the `image` its runner is run from and, where said, `with`, `runner_class`, `pool`, `priority`, `timeout_secs`, `command`, `scope`, `runner_group`, `max_runners`, `max_per_repository`, `untrusted`, `untrusted_runner_class`, `docker`, `retry`, `warm` and `warm_for_secs`. Written to a ConfigMap that dagron-ci's container mounts; a map that changed restarts its pod. Needs `ci.cells` with `ci.apiTokenSecret`, `ci.githubApp` and `ci.webhookSecret`, and the render fails without them or on a map dagron-ci would not start on | `{}` |
| `ci.fleet.prepull.enabled` | Pull each label's image (and `ci.fleet.prepull.images`) onto each node a fleet runner may start on, before one does: a DaemonSet of an init container an image, which runs its `sh -c true`, and a pause. Fails to render with no image to pull | `false` |
| `ci.fleet.prepull.images` | Images pulled beside the labels' | `[]` |
| `ci.fleet.prepull.nodeSelector`, `ci.fleet.prepull.tolerations` | Where the runners' pods are started | `{}`, `[]` |
| `ci.fleet.prepull.pauseImage` | The DaemonSet's one container, which does nothing | `registry.k8s.io/pause:3.10` |
| `ci.image` | dagron-ci image | `mancube/dagron-ci:<chart version>` |
| `global.imageRegistry` | Relocate every image to a private mirror | `""` |
| `backup.enabled` / `backup.to` | A nightly CronJob: `pg_dump` of the engine's database, sent with `dagron backup` to `s3://`, `gs://`, `az://` or `file://` (with `backup.claimName`), the newest `backup.keep` kept. Cloud credentials through `backup.serviceAccount.annotations` (workload identity) or `backup.extraEnv` — see [docs/BACKUP_RECOVERY.md](../../../docs/BACKUP_RECOVERY.md) | `false` / `""` |
| `global.openshift` | OpenShift's restricted SCCs: no pod names a uid or an fsGroup, each runs as the one the SCC gives, in group 0, and the engine's task pods likewise. `auto` finds OpenShift by its `security.openshift.io/v1` API; `true` for a render with no cluster behind it — see [docs/OPENSHIFT.md](../../../docs/OPENSHIFT.md) | `auto` |
| `networkPolicy.enabled` | Limit ingress to the engine's unauthenticated ops API and the bundled Postgres (needs a CNI that enforces NetworkPolicy; ingress only, no egress policy) — see [docs/HARDENING.md](../../../docs/HARDENING.md) | `true` |
| `engine.taskNetworkPolicy` | Let a task ask for no network (`isolation: { network: none }`): the NetworkPolicy `dagron-task-no-network`, the engine's leave to read the namespace's policies, and its word that the cluster enforces one. Needs a CNI that does, executor `k8s`, and `DAGRON_CI_EXEC_IMAGE` in `engine.extraEnv`. `engine.taskNetworkPolicyCreate: false` leaves the policy to whoever applies the namespace's — see [docs/FAMILIES.md](../../../docs/FAMILIES.md) | `false` |
| `engine.taskNetworkAllowlists` | Allowlists a task may name (`isolation: { network: allow:<name> }`): for each, a NetworkPolicy `dagron-task-allow-<name>` with the egress rules given, and its name told to the engine. Needs `engine.taskNetworkPolicy`. A rule with no `to` is refused, and a list must not allow the cluster's API server. A list's `proxy` (`http://host:port`) is written to its policy as the annotation `dagron.dev/proxy`, and told to its tasks as `HTTP_PROXY` and the rest: a list by hostname is a proxy of yours that the list's rules allow | `{}` |
| `networkPolicy.engine.monitoringNamespace` / `.allowFrom` | Who may reach the engine: the Prometheus namespace, plus extra peers | `monitoring` / `[]` |

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
