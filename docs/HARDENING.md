# Hardening a dagron install

> What the chart does for you, what it deliberately does not, and how to check.
> Reproduced by [`loadtest/deploy/security-e2e/netpol.sh`](../loadtest/deploy/security-e2e/netpol.sh).

## The engine's API has no authentication — the network is its boundary

The engine's ops API (`API_ADDR`, `:8080`: `POST /runs`, `/runs/{id}/cancel`, `/rerun`, approvals…) is
unauthenticated by design; it logs a warning at startup saying so ("put a network boundary in front of
it"). `dagron-api` is the authenticated edge, and it does **not** call the engine over HTTP.

Before the chart shipped a NetworkPolicy, **any pod in the cluster could submit an arbitrary run**: a
throwaway `alpine` pod running `wget --post-data=<workflow YAML> http://dagron-engine:8080/runs` got a
`run_id` back, and the workflow's `command` then ran in a task pod. Task pods run workflow-author code and
sit in the same namespace, so a compromised task could do the same, and could also reach Postgres with
nothing but its password.

## What the chart now does (`networkPolicy.enabled`, on by default)

| Policy | Selects | Allows ingress on | From |
|---|---|---|---|
| `<release>-engine` | engine pods | the engine port | the Prometheus namespace (`networkPolicy.engine.monitoringNamespace`, default `monitoring`) and `networkPolicy.engine.allowFrom` |
| the same policy, with dagron-ci in the engine's pod (`ci.enabled`, `ci.placement: engine`) | engine pods | dagron-ci's port | anywhere, as that port is where dagron-ci has a pod of its own: each of its doors asks for a signature, a credential or a secret |
| `<release>-postgres` (only with the bundled Postgres) | postgres pods | 5432 | the release's engine, dagron-api, gitops and operator pods, plus `networkPolicy.postgres.allowFrom` |

Everything else is dropped: task pods, the gitops worker (for the engine), other pods in the namespace,
other namespaces. With no peers configured the engine policy renders as `ingress: []` (deny all), never as an
empty `from`, which Kubernetes reads as "allow everyone".

Verified on kind (`netpol.sh`, 14/14): with the policy off, a throwaway pod in the same namespace **and** one in
another namespace could `POST /runs` and connect to Postgres; with it on, neither can, while `dagron-api` login,
a run submitted through `kubectl port-forward`, and the API from other namespaces all keep working, and a task
pod cannot reach the engine or the datastore. The script first proves the cluster's CNI enforces policies
(reachable before a deny-all, blocked after) and stops as *inconclusive* if it does not.

```bash
loadtest/deploy/security-e2e/netpol.sh           # needs kubectl, helm; uses the kind-dagron-spark context
loadtest/deploy/security-e2e/netpol.sh --cleanup
```

### Values

```yaml
networkPolicy:
  enabled: true                      # false renders neither of the two policies above
  engine:
    monitoringNamespace: monitoring  # "" = nobody may scrape
    allowFrom: []                    # extra raw NetworkPolicy peers that may reach the engine API
  postgres:
    allowFrom: []
```

If something legitimate talks to the engine (a custom scheduler, an ops namespace), add it to
`networkPolicy.engine.allowFrom`. `kubectl port-forward` and `kubectl exec` are not subject to NetworkPolicy,
so operator access keeps working — gate it with RBAC.

## What this does NOT cover

- **It only works on a CNI that enforces NetworkPolicy** (Calico, Cilium, kindnet, the AWS VPC CNI with network
  policy enabled…). On one that does not, the objects are accepted and protect nothing. Check with the control
  test in `netpol.sh`.
- **Ingress only. These two policies say nothing of egress.** A task pod can still reach anything the namespace's
  egress allows — the internet, the metadata service, other namespaces, the Kubernetes API if its service account
  allows it. One task at a time can give that up: a task that declares `isolation: { network: none }` runs in
  a pod with no network, in or out, under a third policy the chart makes with `engine.taskNetworkPolicy`
  (`dagron-task-no-network`; it is separate from `networkPolicy.enabled`, and off by default). See
  "No network, on Kubernetes" in `docs/FAMILIES.md`. If you add an egress policy of your own to this namespace,
  keep it from those pods (`{key: dagron.dev/network, operator: DoesNotExist}` in its `podSelector`): a pod
  has what any policy that selects it allows, and the engine refuses to start beside one that gives a labelled pod
  something back.
- **The API is still unauthenticated for the peers you allow.** A NetworkPolicy filters by address and port, not
  by path: the monitoring namespace can reach `/metrics` **and** `POST /runs` on the same port, so treat that
  namespace as trusted, or set `monitoringNamespace: ""` and scrape another way.
- **dagron-ci in the engine's pod reaches the engine's API.** With `ci.enabled` the chart runs dagron-ci as a
  container of the engine's pod (`ci.placement: engine`), so that the engine can ask it for a private repository's
  checkout credential on the pod's own address. The two then share that address, and a NetworkPolicy does not reach
  inside a pod: dagron-ci, which takes a forge's webhooks, can call the engine's unauthenticated API there. It is
  not given the engine's service-account token (the pod mounts none, and the engine's container mounts its own).
  `ci.placement: deployment` keeps the two in separate pods, with the policy above between them, and a private
  repository's checkout is then not served.
- **Task pods share the namespace with the engine** and are not isolated from each other.
- **An external Postgres** (`externalDatabaseUrl`) is not covered: the policy only selects the chart's own pod.
- **`dagron-api` ingress is not restricted** (it serves users); front it with your ingress and its own controls.
- **Secrets are a separate topic**: see the environment-secrets notes in [HOWTO.md](HOWTO.md).
