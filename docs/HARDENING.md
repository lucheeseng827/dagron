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
  enabled: true                      # false renders nothing
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
- **Ingress only. There is no egress policy.** A task pod can still reach anything the namespace's egress
  allows — the internet, the metadata service, other namespaces, the Kubernetes API if its service account allows
  it. Egress control is the next step and needs to know what your tasks legitimately talk to.
- **The API is still unauthenticated for the peers you allow.** A NetworkPolicy filters by address and port, not
  by path: the monitoring namespace can reach `/metrics` **and** `POST /runs` on the same port, so treat that
  namespace as trusted, or set `monitoringNamespace: ""` and scrape another way.
- **Task pods share the namespace with the engine** and are not isolated from each other.
- **An external Postgres** (`externalDatabaseUrl`) is not covered: the policy only selects the chart's own pod.
- **`dagron-api` ingress is not restricted** (it serves users); front it with your ingress and its own controls.
- **Secrets are a separate topic**: see the environment-secrets notes in [HOWTO.md](HOWTO.md).
