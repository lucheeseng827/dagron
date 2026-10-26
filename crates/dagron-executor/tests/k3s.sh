#!/usr/bin/env bash
# Workspace tasks on a real cluster: k3s in Docker, the dagron-ci-exec image
# built from crates/dagron-ci-exec/Dockerfile and loaded into it, then
# tests/kube_workspace.rs against it: steps that share the workspace with the
# binary installed beside them, an install that fails, a workspace evicted for
# outgrowing its size, one that is a volume claim of its own, and one that
# starts with its checkout, fetched by an init container from a git server the
# tests run on this host. And tests/kube_network.rs: a task that asked for no
# network reaches nothing, held by a NetworkPolicy that k3s's own controller
# enforces. That policy is the chart's: rendered here with
# engine.taskNetworkPolicy and applied to the namespace before the tests, so
# that what the engine reads as it starts, and what holds the task's pod, is
# the object a user of the chart gets. And an allowlist of the chart's
# (engine.taskNetworkAllowlists), which a task held to it runs under: it
# reaches the pod the list names, and neither another pod nor the API server.
# And tests/kube_services.rs: a task's services are sidecars of its pod, each
# ready before the task starts, and the task has ended when its own container
# has. And tests/kube_watch.rs: tasks that run together are each told of their
# pod by the executor's one watch, and ask the API server for a pod only until
# the watch has seen it. And tests/kube_follow.rs: a task's log is followed as
# it writes it, a task that runs out of time keeps what it wrote, and a line
# the runtime writes in parts is whole.
#
#   crates/dagron-executor/tests/k3s.sh            # from the repository root
#   KEEP=1 crates/dagron-executor/tests/k3s.sh     # leave the cluster running
#   EXEC_IMAGE=dagron-ci-exec:dev crates/dagron-executor/tests/k3s.sh
#                                                  # an image already built
#
# Needs docker, helm and cargo. Docker pulls every image a pod runs and the
# node is given them, so the node itself reaches no registry.
set -euo pipefail

K3S_IMAGE=${K3S_IMAGE:-rancher/k3s:v1.31.14-k3s1}
TASK_IMAGE=alpine:3.20       # the tests' task image
CHECKOUT_IMAGE=alpine/git:v2.47.2 # what a checkout is fetched with
N=${K3S_NAME:-dagron-workspace-k3s}
PORT=${K3S_PORT:-16443}
NAMESPACE=default

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
# The chart, where this repository keeps it.
chart=
for at in "$root/charts/dagron" "$root/terraform/charts/dagron"; do
  if [ -f "$at/Chart.yaml" ]; then chart=$at; break; fi
done
dir=$root/target/k3s-workspace
mkdir -p "$dir"
kubeconfig=$dir/kubeconfig

log() { echo "k3s.sh: $*" >&2; }
fail() { echo "k3s.sh: FAIL $*" >&2; exit 1; }
k() { docker exec -i "$N" kubectl "$@"; }

built=
diagnose() {
  echo "::group::what the cluster held when it failed" >&2
  k get pods -A -o wide >&2 || true
  k get pvc,pv -A >&2 || true
  k get events -A --sort-by=.lastTimestamp >&2 || true
  echo "::endgroup::" >&2
}
cleanup() {
  local status=$?
  [ "$status" -ne 0 ] && diagnose
  if [ "${KEEP:-0}" = 1 ]; then
    log "KEEP=1: left running; KUBECONFIG=$kubeconfig; remove with docker rm -f -v $N"
  else
    # -v: k3s's image declares volumes, which would otherwise outlive it.
    docker rm -f -v "$N" >/dev/null 2>&1 || true
    if [ -n "$built" ]; then docker image rm -f "$built" >/dev/null 2>&1 || true; fi
  fi
  exit "$status"
}

wait_for() { # what seconds command...
  local what=$1 seconds=$2
  shift 2
  for _ in $(seq 1 "$seconds"); do
    "$@" >/dev/null 2>&1 && return 0
    sleep 1
  done
  fail "$what: not within ${seconds}s"
}

# ---------------------------------------------------------------- the image --
if [ -n "${EXEC_IMAGE:-}" ]; then
  log "using $EXEC_IMAGE as the dagron-ci-exec image"
else
  EXEC_IMAGE=dagron-ci-exec:k3s-test
  log "building $EXEC_IMAGE from crates/dagron-ci-exec/Dockerfile"
  docker build -q -t "$EXEC_IMAGE" -f "$root/crates/dagron-ci-exec/Dockerfile" "$root" >/dev/null
  built=$EXEC_IMAGE
fi

# -------------------------------------------------------------- the cluster --
log "starting $K3S_IMAGE"
docker rm -f -v "$N" >/dev/null 2>&1 || true
trap cleanup EXIT
# Its local-path StorageClass stays (the claim test's); nothing else the tests
# use runs. Eviction for node pressure only below 200 MiB free: a runner's
# disk may be mostly full, and a node evicting every pod proves nothing. (A
# share would misfire where a large disk has little free: 1% of 250 GB is more
# than some sandboxes have left.) A workspace past its own size is evicted
# all the same. Image garbage collection is off, for the same disk: the node
# is given every image the tests run, and reaches no registry to get one back.
docker run -d --name "$N" --privileged --tmpfs /run --tmpfs /var/run \
  -p "127.0.0.1:$PORT:6443" \
  "$K3S_IMAGE" server \
  --disable=traefik,metrics-server,servicelb,coredns \
  '--kubelet-arg=eviction-hard=memory.available<50Mi,nodefs.available<200Mi,imagefs.available<200Mi' \
  --kubelet-arg=image-gc-high-threshold=100 >/dev/null
node_ready() { k get nodes --no-headers 2>/dev/null | grep -q ' Ready '; }
wait_for "the node to be Ready" 120 node_ready
# Every pod's sandbox asks for oom_score_adj -998. Where the kernel refuses
# lowering it even to root, as on some sandboxed hosts, no pod starts;
# containerd's restrict_oom_score_adj keeps each at or above its own.
if ! docker exec "$N" sh -c 'echo -998 > /proc/self/oom_score_adj' 2>/dev/null; then
  log "this kernel refuses a negative oom_score_adj: restricting containerd's"
  docker exec "$N" sh -c 'cd /var/lib/rancher/k3s/agent/etc/containerd &&
    sed -e "/DO NOT EDIT/d" \
        -e "s/^\[plugins.'"'"'io.containerd.cri.v1.runtime'"'"'\]$/&\n  restrict_oom_score_adj = true/" \
        config.toml > config-v3.toml.tmpl'
  docker restart "$N" >/dev/null
  sleep 5
  wait_for "the node to be Ready again" 120 node_ready
  docker exec "$N" grep -q 'restrict_oom_score_adj = true' \
    /var/lib/rancher/k3s/agent/etc/containerd/config.toml \
    || fail "containerd's oom_score_adj was not restricted"
fi
docker exec "$N" cat /etc/rancher/k3s/k3s.yaml \
  | sed "s#https://127.0.0.1:6443#https://127.0.0.1:$PORT#" > "$kubeconfig"
chmod 600 "$kubeconfig"

# ------------------------------------------------------------- its images --
# The pod sandbox's, local-path's and its volume helper's, as this k3s names
# them, and the tests' own.
pause=$(docker exec "$N" sh -c 'grep -hoE "sandbox(_image)? = \"[^\"]+\"" /var/lib/rancher/k3s/agent/etc/containerd/config.toml' \
  | sed -E 's/.*"([^"]+)"/\1/' | head -1)
[ -n "$pause" ] || fail "k3s's sandbox image: not in its containerd config"
provisioner_image() { k get deploy -n kube-system local-path-provisioner -o jsonpath='{.spec.template.spec.containers[0].image}'; }
wait_for "local-path's deployment" 120 provisioner_image
provisioner=$(provisioner_image)
helper=$(k get configmap -n kube-system local-path-config -o jsonpath='{.data.helperPod\.yaml}' \
  | awk '$1 == "image:" { print $2; exit }' | tr -d "\"'")
[ -n "$helper" ] || fail "local-path's helper image: not in its config"
for image in "$pause" "$provisioner" "$helper" "$TASK_IMAGE" "$CHECKOUT_IMAGE"; do
  docker image inspect "$image" >/dev/null 2>&1 || docker pull -q "$image" >/dev/null
done
for image in "$pause" "$provisioner" "$helper" "$TASK_IMAGE" "$CHECKOUT_IMAGE" "$EXEC_IMAGE"; do
  docker save "$image" | docker exec -i "$N" ctr -n k8s.io images import - >/dev/null
done
# A provisioner pod already backing off from a pull starts over, now that
# its image is here.
k delete pod -n kube-system -l app=local-path-provisioner --wait=false >/dev/null
provisioner_ready() {
  [ "$(k get deploy -n kube-system local-path-provisioner -o jsonpath='{.status.availableReplicas}')" = 1 ]
}
wait_for "local-path to be available" 180 provisioner_ready

# --------------------------------------------------------------- the tests --
# The node's pods reach this host where its own container does: at the
# gateway of Docker's default network, where the tests' git server listens.
host=$(docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}')
[ -n "$host" ] || fail "the default Docker network's gateway: not found"
# The policy that takes a task's network, as the chart makes it. The tests
# apply one of their own only where none is there, so with this in place the
# executor is started against the chart's, and a chart whose policy the
# engine would not take fails them.
[ -n "$chart" ] || fail "the chart: not found under $root"
command -v helm >/dev/null || fail "helm: not found (it renders the chart's NetworkPolicy)"
# And an allowlist, named for where it runs: the pods the tests label
# dagron-test/allowed=yes, on the port their listeners answer on.
ALLOWLIST=k3s
helm template dagron "$chart" \
  --set engine.taskNetworkPolicy=true \
  --set 'engine.extraEnv[0].name=DAGRON_CI_EXEC_IMAGE' \
  --set "engine.extraEnv[0].value=$EXEC_IMAGE" \
  --set-json "engine.taskNetworkAllowlists={\"$ALLOWLIST\":{\"egress\":[{\"to\":[{\"podSelector\":{\"matchLabels\":{\"dagron-test/allowed\":\"yes\"}}}],\"ports\":[{\"protocol\":\"TCP\",\"port\":8080}]}]}}" \
  --show-only templates/task-network-policy.yaml > "$dir/task-network-policy.yaml" \
  || fail "the chart does not render with engine.taskNetworkPolicy and an allowlist"
[ "$(grep -c '^kind: NetworkPolicy$' "$dir/task-network-policy.yaml")" = 2 ] \
  || fail "the chart did not render the two NetworkPolicies: the one that takes a task's network, and the allowlist's"
k apply -n "$NAMESPACE" -f - < "$dir/task-network-policy.yaml" >/dev/null \
  || fail "the chart's NetworkPolicy was not applied"
log "applied the chart's $(k get networkpolicy -n "$NAMESPACE" -o name | tr '\n' ' ')"

log "running tests/kube_workspace.rs, tests/kube_network.rs, tests/kube_services.rs, tests/kube_watch.rs and tests/kube_follow.rs"
cd "$root"
# Built first, with this environment as it is; then run without HTTPS_PROXY,
# which the kube client would send even 127.0.0.1 through (it reads no
# NO_PROXY).
cargo test --locked -p dagron-executor --features kubernetes,dagron-core/sqlite \
  --test kube_workspace --test kube_network --test kube_services --test kube_watch --test kube_follow --no-run
env -u HTTPS_PROXY -u https_proxy \
  KUBECONFIG="$kubeconfig" \
  DAGRON_KUBE_LIVE="$NAMESPACE" \
  DAGRON_KUBE_LIVE_EXEC_IMAGE="$EXEC_IMAGE" \
  DAGRON_KUBE_LIVE_ALLOWLIST="$ALLOWLIST" \
  DAGRON_KUBE_LIVE_STORAGE_CLASS=local-path \
  DAGRON_KUBE_LIVE_CHECKOUT_IMAGE="$CHECKOUT_IMAGE" \
  DAGRON_KUBE_LIVE_HOST="$host" \
  cargo test --locked -p dagron-executor --features kubernetes,dagron-core/sqlite \
  --test kube_workspace --test kube_network --test kube_services --test kube_watch --test kube_follow
log "ok"
