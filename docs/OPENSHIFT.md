# dagron on OpenShift

The chart installs on OpenShift as an ordinary user would install it, into an
ordinary project, under the SCC every such project's pods get:
`restricted-v2`. Nothing needs `anyuid`, and nothing asks for a custom SCC.

## What restricted-v2 asks, and what the chart does

OpenShift's restricted SCCs differ from a stock cluster in three ways that
matter here:

1. **A pod may not name its own uid or fsGroup.** The SCC gives each pod a uid
   and an fsGroup from the project's range (its
   `openshift.io/sa.scc.uid-range` annotation) and refuses a pod that names
   another. The chart's pods otherwise name their images' users (65532,
   10001), so with `global.openshift` on, none of them names a uid, a group or
   an fsGroup. Each is still `runAsNonRoot`, under the `RuntimeDefault`
   seccomp profile, and each container escalates nothing and drops every
   capability, which is what restricted-v2 holds a pod to anyway.
2. **The process runs as a uid no passwd names, in group 0.** Every directory
   an image writes to is group 0's with the owner's rights (`chmod g=u`):
   the engine's `/workflows` and `/artifacts`, dagron-api's `/artifacts`, the
   runner's `/var/lib/dagron-runner`. dagron-gitops execs `ssh` for an
   `ssh://` repository, and OpenSSH refuses a uid it cannot look up, so its
   entrypoint gives such a uid a passwd entry through `nss_wrapper`, in a file
   under `/tmp`.
3. **Volumes are the fsGroup's.** A pod's volumes are given the fsGroup the
   SCC assigns, so the bundled Postgres's data and a task's own workspace
   volume are writable by the uid that runs them.

`global.openshift` says whether to do this:

| Value | Meaning |
|---|---|
| `auto` (default) | when the cluster serves `security.openshift.io/v1`, which OpenShift does and nothing else does |
| `true` | always: for `helm template` with no cluster behind it, or a GitOps tool that renders offline |
| `false` | never: each pod runs as its image's user, as on any other cluster |

`helm install` against an OpenShift cluster finds the API itself, so nothing
needs setting. A render made without a cluster (Argo CD with a custom render,
`helm template`) does not, so pass `--set global.openshift=true` or
`--api-versions security.openshift.io/v1`.

## Install

```sh
oc new-project dagron
helm install dagron oci://registry-1.docker.io/mancube/dagron -n dagron
```

To reach the console, expose dagron-api with a Route:

```sh
oc create route edge dagron --service dagron-api -n dagron
```

or turn on the chart's `ingress`: OpenShift turns an Ingress into a Route
itself.

The enterprise chart (`dagron-ee`) does the same for its own pods, with the
same switch (`global.openshift`, shared with the base chart): the control
plane, the router, the runner pools and the pull-secret Job.

## Task pods

The engine runs each task as a pod in its namespace, and the same SCC admits
it. In OpenShift mode the chart sets on the engine:

| Variable | Value | Why |
|---|---|---|
| `DAGRON_TASK_FS_GROUP` | `none` | a workspace that is its own volume names no fsGroup; the SCC gives it one |
| `DAGRON_TASK_RUN_AS_NON_ROOT` | `1` | non-root, without naming a uid |
| `DAGRON_TASK_DROP_ALL_CAPABILITIES` | `1` | no capabilities, no privilege escalation |
| `DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT` | `1` | the runtime's seccomp profile |

A task therefore runs as the project's uid in group 0, whatever its image's
`USER` says. An image that writes outside its workspace and `/tmp` has to make
those paths group 0's, as OpenShift's own image guidelines ask. An image that
must run as root does not run under restricted-v2 at all; it needs an SCC
that admits it, granted to the engine's service account, and
`global.openshift=false` for the pods that then name their user.

## What does not run under restricted-v2

- **Image builds** (`dagron-ee`'s build pools): BuildKit runs with an
  unconfined seccomp profile, rootless, or privileged in a user namespace.
  Neither passes restricted-v2. A build pool on OpenShift needs an SCC of its
  own for the build namespace, as BuildKit's own OpenShift guidance has it, or
  builds off.
- **The Docker executor** and a task's `docker_daemon`: each needs a daemon,
  which restricted-v2 does not admit.

## How it is checked

`scripts/openshift-k3s.sh` holds the chart to restricted-v2 without an
OpenShift cluster. It reads what the chart renders: in OpenShift mode no pod
names a uid or an fsGroup and each meets Pod Security `restricted`, found
from the API as well as set; off it, each pod is as it was. Then it installs
the chart on a k3s in Docker into a namespace that enforces Pod Security
`restricted`, through a post-renderer that does what the SCC does (gives each
pod a uid and an fsGroup from a range like OpenShift's, and fails on a pod
that names its own), with images built from the repository's own Dockerfiles:
Postgres, the engine and dagron-api start as that uid in group 0 and make
their schemas, and a workflow's task pod runs as a non-root uid in group 0 to
success.

What it does not prove: OpenShift itself. Its SCC admission, its SELinux
labels and its Route admission are not k3s's. A fresh install on OpenShift
passing the dogfood pipeline is the plan's acceptance line, and wants a
cluster.
