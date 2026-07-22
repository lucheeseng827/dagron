# dagron on Argo CD — runnable GitOps demo

Deploy the **dagron platform** and a fleet of **dagron workflows** onto a
Kubernetes cluster with Argo CD, where `git push` is the only mutation. Workflows
fan out through a single **ApplicationSet** — one Argo CD Application per team
folder, each syncing that team's `Workflow` / `CronWorkflow` CRs.

The narrative walkthrough (architecture, the rolling rollout, multi-cluster,
secrets, troubleshooting) is in [`../../docs/GITOPS_ARGOCD.md`](../../docs/GITOPS_ARGOCD.md).
This file is the copy-paste runbook.

> Grounded in the **as-built** chart (`terraform/charts/dagron`, v0.3.0) and the
> **as-built** CRDs (`dagron.io/v1`: `Workflow`, `CronWorkflow`). The design/roadmap
> doc [`../../docs/GITOPS.md`](../../docs/GITOPS.md) describes a richer future schema —
> what runs here is what ships today.

## Layout

```text
examples/gitops/
├── bootstrap/root-app.yaml            # app-of-apps: apply this one file
├── projects/dagron.yaml               # AppProject (trust boundary)
├── platform/dagron-platform.yaml      # LAYER 1  Application → Helm chart (wave 0)
├── appsets/
│   ├── dagron-workflows.yaml          # LAYER 2  ApplicationSet: one app per team (wave 1)
│   └── dagron-workflows-matrix.yaml   # advanced: clusters × teams (not applied by default)
└── workflows/                         # the workflows themselves — plain CRs in Git
    ├── team-data/                     #   → Application dagron-wf-team-data      → ns team-data
    │   ├── etl-diamond.workflow.yaml
    │   └── nightly-rollup.cronworkflow.yaml
    └── team-payments/                 #   → Application dagron-wf-team-payments  → ns team-payments
        ├── reconcile.workflow.yaml
        └── hourly-settle.cronworkflow.yaml
```

## Prerequisites

- A cluster. A throwaway `kind` cluster is enough:
  ```bash
  kind create cluster --name dagron-gitops
  ```
- `kubectl` pointed at it, and Argo CD installed:
  ```bash
  kubectl create namespace argocd
  kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
  kubectl -n argocd rollout status deploy/argocd-applicationset-controller
  ```
- The engine/operator images must be reachable from the cluster. The chart
  defaults to `mancube/dagron-engine:0.3.0` and `mancube/dagron-operator:0.3.0`
  (public Docker Hub). The engine image must carry the `postgres` + `ops` cargo
  features (the published `0.3.0` does; a sqlite-only local build does not).

> **Point the manifests at your repo.** Every manifest defaults `repoURL` to
> `https://github.com/lucheeseng827/nick-rust-project.git` and `targetRevision` to
> `main`. If you forked, or you're trying this on a branch before merge, set both
> to match — e.g. `targetRevision: claude/gitops-argocd-appsets-guide-7u9kki`:
> ```bash
> grep -rl 'targetRevision: main\|revision: main' . | \
>   xargs sed -i 's#\(targetRevision\|revision\): main#\1: YOUR_BRANCH#'
> ```

## Deploy — one command

```bash
kubectl apply -f rust_modules/lab/module_54/examples/gitops/bootstrap/root-app.yaml
```

That app-of-apps root pulls in the AppProject, the platform Application (wave 0),
and the workflows ApplicationSet (wave 1). Watch it converge:

```bash
kubectl -n argocd get applications -w
```

Expected, after a minute or two (CRDs + operator land first, then the workflows):

```text
NAME                       SYNC STATUS   HEALTH STATUS
dagron-gitops-root         Synced        Healthy
dagron-platform            Synced        Healthy
dagron-wf-team-data        Synced        Healthy
dagron-wf-team-payments    Synced        Healthy
```

## Verify the workflows reached the cluster and ran

The CRs are in Git → Argo synced them → the operator turned each into an engine
run.

```bash
# 1. The CRs exist, one namespace per team folder:
kubectl get workflows,cronworkflows -A
```
```text
NAMESPACE       NAME                             AGE
team-data       workflow.dagron.io/etl-diamond   90s
team-payments   workflow.dagron.io/settlement-reconcile   90s

NAMESPACE       NAME                                     SCHEDULE      AGE
team-data       cronworkflow.dagron.io/nightly-rollup    0 0 2 * * *   90s
team-payments   cronworkflow.dagron.io/hourly-settle     0 0 * * * *   90s
```

```bash
# 2. The operator picked them up and created runs (watch its log):
kubectl -n dagron logs deploy/dagron-operator | grep -i "workflow\|create_run\|reconcile"
```
```text
INFO dagron_operator: reconcile Workflow team-data/etl-diamond -> create_run run_id=…
INFO dagron_operator: reconcile Workflow team-payments/settlement-reconcile -> create_run run_id=…
```

```bash
# 3. See the runs in the engine (port-forward the engine API):
kubectl -n dagron port-forward svc/dagron-engine 8080:8080 &
curl -s localhost:8080/api/runs | head
```

## The GitOps loop — change a workflow with a commit

Edit a DAG and push — no `kubectl apply`:

```bash
# add a task, or change a schedule, then:
git add rust_modules/lab/module_54/examples/gitops/workflows/team-data/etl-diamond.workflow.yaml
git commit -m "etl-diamond: add validate task"
git push
```

Argo CD detects the drift (or hit **Refresh**), re-syncs `dagron-wf-team-data`,
the operator reconciles the updated CR, and the next run uses the new DAG. Add a
whole new team by creating `workflows/<team>/` with CRs in it — the ApplicationSet
generates a new Application on its own.

## Teardown

```bash
kubectl -n argocd delete -f rust_modules/lab/module_54/examples/gitops/bootstrap/root-app.yaml
# or drop the whole cluster:
kind delete cluster --name dagron-gitops
```

## Notes

- **Executor.** The demo sets `engine.executor=local` so tasks run in-process (no
  task-pod RBAC, no task images) and converge on kind. For real fan-out set
  `executor: k8s` and give each task a `docker_image` — see
  `../../loadtest/workflows/dagron/`.
- **UI off by default.** `dagronApi` and `frontend` are disabled in the demo
  values. Enable them (and set `dagronApi.jwtSecret` + `dagronApi.admin.*`) to get
  the dashboard.
- **Postgres is throwaway.** `postgres.enabled=true` is a single in-cluster pod
  for testing only. Point at a managed DB for anything real (see the chart
  `values.yaml` `externalDatabaseUrl`).
- **Secrets in Git.** This demo uses chart-managed defaults. For real use, keep
  the DB/JWT secrets out of Git with Sealed Secrets / External Secrets / SOPS —
  see [`../../docs/GITOPS_ARGOCD.md`](../../docs/GITOPS_ARGOCD.md) §Secrets.
