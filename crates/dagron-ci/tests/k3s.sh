#!/usr/bin/env bash
# dagron-ci installed by the chart, on a real cluster: k3s in Docker, the image
# built from crates/dagron-ci/Dockerfile and loaded into it, and the chart
# installed with `helm install` and `ci.enabled`. It checks that:
#   - the pod turns ready. /readyz answers once the database does, so the pod
#     reached the chart's Postgres under the chart's NetworkPolicy and made
#     its schema there, as the image's user on a read-only root;
#   - /readyz answers `ready` through the Service;
#   - a command of its own runs in the pod, and what it wrote is read back;
#   - without a webhook secret the receiver answers 404; with one named, the
#     Secret's key is mounted as a file and read: a delivery signed with it
#     is taken, and one that is not signed is refused;
#   - the chart makes up no cell: a token with no cell named is refused at
#     render, and so is a cell over plain http, which dagron-ci would not
#     start on; with a token and a cell over TLS it starts with both;
#   - by default it runs in the engine's pod (`ci.placement: engine`): the
#     chart makes it no Deployment, and its Service reaches it there under
#     the engine's NetworkPolicy, which admits its port and the engine's own
#     no further;
#     the engine's container holds the service-account token and dagron-ci's
#     holds none; and with a checkout secret named, the engine is told to ask
#     for a checkout's credential at the pod's own address, with the secret
#     in a file, and that door takes the secret and no other. What that
#     placement cannot use is refused at render;
#   - a fleet's label map (`ci.fleet.labels`) is written to a file that
#     dagron-ci itself reads as it starts, a whole number and a `&&` of it
#     as they were written, and a job is then served by the label it asks
#     for. A fleet with no cell, no App or no webhook secret is refused at
#     render, and so is a map dagron-ci would not start on.
#
#   crates/dagron-ci/tests/k3s.sh                         # from the repository root
#   KEEP=1 crates/dagron-ci/tests/k3s.sh                  # leave the cluster running
#   CI_IMAGE=dagron-ci:dev crates/dagron-ci/tests/k3s.sh  # an image already built
#   CI_BIN=…                                              # the command's name in it
#   ENGINE_IMAGE=dagron-engine:dev …                      # an engine's image, built
#                                                         # with the kubernetes feature
#
# Needs docker, helm and openssl. Docker pulls every image a pod runs and the
# node is given them, so the node itself reaches no registry. dagron-api is
# left out of the install: it is not what is checked here, and its image is a
# release's. So is the engine's. Where dagron-ci is in the engine's pod, the
# pod sandbox's image stands in for the engine, which only waits, and what is
# checked is the pod the chart makes. With ENGINE_IMAGE the engine itself is
# started in it, and has to reach the cluster and take the token service.
set -euo pipefail

K3S_IMAGE=${K3S_IMAGE:-rancher/k3s:v1.31.14-k3s1}
CURL_IMAGE=curlimages/curl:8.11.1
N=${K3S_NAME:-dagron-ci-k3s}
PORT=${K3S_PORT:-16445}
BIN=${CI_BIN:-dagron-ci}
RELEASE=dagron
WEBHOOK_SECRET=k3s-webhook-s3cret
CHECKOUT_SECRET=k3s-checkout-s3cret

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
# The chart, where this repository keeps it.
chart=
for dir in "$root/charts/dagron" "$root/terraform/charts/dagron"; do
  if [ -f "$dir/Chart.yaml" ]; then chart=$dir; break; fi
done
dir=$root/target/k3s-ci
mkdir -p "$dir"
kubeconfig=$dir/kubeconfig

log() { echo "k3s.sh: $*" >&2; }
ok() { echo "  ok   $*"; }
fail() { echo "k3s.sh: FAIL $*" >&2; exit 1; }
k() { docker exec -i "$N" kubectl "$@"; }
[ -n "$chart" ] || fail "the chart: not under charts/dagron or terraform/charts/dagron"
for tool in docker helm openssl; do
  command -v "$tool" >/dev/null || fail "needs $tool"
done

built=
diagnose() {
  echo "::group::what the cluster held when it failed" >&2
  k get pods -A -o wide >&2 || true
  k get events -A --sort-by=.lastTimestamp >&2 || true
  k logs "deploy/$RELEASE-ci" --tail 60 >&2 || true
  k logs "deploy/$RELEASE-engine" --all-containers --tail 60 >&2 || true
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

# -------------------------------------------------------- what is refused --
# dagron-ci sends its token for dagron-api to a cell, so it takes one over
# TLS and starts on no other. The chart's own dagron-api serves no TLS, so
# the chart makes no cell up, and refuses at render what dagron-ci would
# refuse at start. No cluster is needed to see it.
renders() { helm template "$RELEASE" "$chart" --set ci.enabled=true "$@" 2>&1; }
refuses() { # why, then helm arguments...
  local why=$1 said; shift
  said=$(renders "$@") && fail "the chart rendered: $*"
  printf '%s' "$said" | grep -q -- "$why" || fail "refused, and not for '$why': $(printf '%s' "$said" | tail -1)"
}
refuses 'ci.cells is not' --set ci.apiTokenSecret.name=t
refuses 'is not name=https://host' --set ci.apiTokenSecret.name=t --set-string ci.cells=default=http://dagron-api:8080
refuses 'is not name=https://host' --set ci.apiTokenSecret.name=t --set-string 'ci.cells=eu=https://eu.example\,us=http://us.internal'
# The URL's own host decides, as it does for dagron-ci: not how it begins.
refuses 'is not name=https://host' --set ci.apiTokenSecret.name=t --set-string ci.cells=default=http://localhost:8080@remote.example
for here in http://localhost:15001 http://127.0.0.2:15001 'http://[::1]:15001'; do
  renders --set ci.apiTokenSecret.name=t --set-string "ci.cells=default=$here" >/dev/null \
    || fail "a cell on the pod's own machine was refused: $here"
done
ok "a token needs a cell named, and a cell is https or the pod's own machine: the chart makes none up"

# ------------------------------------------------------------ where it runs --
# By default dagron-ci is a container of the engine's pod (ci.placement:
# engine), where the engine asks it for a checkout's credential at the pod's
# own address. There it has the engine's replicas, its place on a node and
# its address, so a setting of dagron-ci's own for one of those is refused
# and not left unused. No cluster is needed to see this either.
refuses 'ci.placement is' --set ci.placement=sidecar
refuses 'has the engine.s replicas' --set ci.replicas=2
refuses 'set engine.nodeSelector' --set ci.nodeSelector.disk=ssd
refuses 'set engine.priorityClassName' --set ci.priorityClassName=high
refuses 'share one address' --set ci.port=8080
renders --set ci.placement=deployment --set ci.replicas=2 --set ci.nodeSelector.disk=ssd >/dev/null \
  || fail "a Deployment of its own was refused its own replicas and node selector"

# shows TEMPLATE [helm arguments...]: that one template, as rendered
shows() {
  local template=$1; shift
  renders --show-only "templates/$template" "$@" || fail "templates/$template did not render: $*"
}
# holds TEXT PATTERN: whether a line of the text matches
holds() { grep -q -- "$2" <<<"$1"; }
engine=$(shows engine.yaml)
holds "$(shows ci.yaml)" '^kind: Deployment$' && fail "dagron-ci was given a Deployment of its own by default"
holds "$engine" '^        - name: dagron-ci$' || fail "by default the engine's pod has no dagron-ci container"
holds "$engine" 'automountServiceAccountToken: false' \
  || fail "the engine's pod mounts its service-account token in every container, dagron-ci's too"
holds "$engine" 'mountPath: /var/run/secrets/kubernetes.io/serviceaccount' \
  || fail "the engine's container does not mount the service-account token itself"
holds "$engine" 'DAGRON_CHECKOUT_TOKEN' && fail "with no checkout secret named, the engine is told to ask for a credential"
engine=$(shows engine.yaml --set ci.checkoutTokenSecret.name=ci-checkout)
holds "$engine" 'value: "http://127.0.0.1:8090/ci/v1/checkout-tokens"' \
  || fail "with a checkout secret named, the engine is not told to ask dagron-ci at the pod's own address"
holds "$engine" 'value: /etc/dagron/checkout-secret/value' \
  || fail "with a checkout secret named, the engine is not given it as a file"
# The chart's word on where the engine asks is the only one: a second, in
# engine.extraEnv, would be a second variable of the same name. Without a
# checkout secret the chart says nothing, and a token service of the
# operator's own may be named there.
own='engine.extraEnv[0].name=DAGRON_CHECKOUT_TOKEN_URL'
refuses 'engine.extraEnv sets DAGRON_CHECKOUT_TOKEN_URL' --set ci.checkoutTokenSecret.name=ci-checkout \
  --set "$own" --set 'engine.extraEnv[0].value=https://tokens.example.test/mint'
renders --set "$own" --set 'engine.extraEnv[0].value=https://tokens.example.test/mint' >/dev/null \
  || fail "with no checkout secret named, a token service of the operator's own was refused"
# A Deployment of its own, and no dagron-ci at all, leave the engine's pod as
# it was: neither is a reason to change what the engine mounts.
engine=$(shows engine.yaml --set ci.placement=deployment --set ci.checkoutTokenSecret.name=ci-checkout)
holds "$engine" 'dagron-ci\|DAGRON_CHECKOUT_TOKEN\|automountServiceAccountToken\|volumes:' \
  && fail "with a Deployment of its own, the engine's pod was changed for dagron-ci"
engine=$(helm template "$RELEASE" "$chart" --show-only templates/engine.yaml 2>&1) || fail "the chart did not render without dagron-ci"
holds "$engine" 'dagron-ci\|DAGRON_CHECKOUT_TOKEN\|automountServiceAccountToken\|volumes:' \
  && fail "without dagron-ci, the engine's pod was changed for it"
ok "by default it is a container of the engine's pod, which mounts the service-account token in the engine alone; what that placement cannot use is refused"

# --------------------------------------------------------------- fleet mode --
# The label map of the jobs GitHub Actions gives its own runner
# (ci.fleet.labels). The chart writes it to a file for dagron-ci, and refuses
# at render what dagron-ci would not start on: a fleet with no cell to start
# a runner in or no App to register one, and a map that is not one. It
# refuses a fleet with no webhook secret besides, which dagron-ci does start
# on: a job is heard of by its delivery alone, and a release's replicas are
# all set alike. No cluster is needed to see any of it.
cat > "$dir/fleet.yaml" <<'EOF'
ci:
  fleet:
    labels:
      dagron:
        image: ghcr.io/actions/actions-runner:2.321.0
      dagron-large:
        image: ghcr.io/actions/actions-runner:2.321.0
        runner_class: large
        pool: ci-large
        # A whole number and a `&&`: each has to reach dagron-ci as written.
        priority: 1000000
        timeout_secs: 7200
        command: [sh, -c, "cd /home/runner && exec ./run.sh"]
EOF
label=(--set-string ci.fleet.labels.dagron.image=runner:1)
cell=(--set ci.apiTokenSecret.name=t --set-string ci.cells=default=https://dagron-api.example.test)
app=(--set-string ci.githubApp.id=7 --set ci.githubApp.keySecret.name=k)
hook=(--set ci.webhookSecret.name=w)
refuses 'ci.fleet.labels needs ci.cells' "${label[@]}" "${app[@]}" "${hook[@]}"
refuses 'ci.fleet.labels needs ci.githubApp' "${label[@]}" "${cell[@]}" "${hook[@]}"
refuses 'ci.fleet.labels needs ci.webhookSecret.name' "${label[@]}" "${cell[@]}" "${app[@]}"
with=("${cell[@]}" "${app[@]}" "${hook[@]}")
refuses 'ci.extraEnv sets CI_GITHUB_FLEET_FILE' "${label[@]}" "${with[@]}" \
  --set 'ci.extraEnv[0].name=CI_GITHUB_FLEET_FILE' --set 'ci.extraEnv[0].value=/fleet.yaml'
# The map itself, by dagron-ci's own rules for the file.
refuses 'imge: is no key of a label.s' "${label[@]}" "${with[@]}" --set-string ci.fleet.labels.dagron.imge=runner:1
refuses 'labels.big.image: is the image' "${label[@]}" "${with[@]}" --set-string ci.fleet.labels.big.pool=large
refuses 'a label GitHub gives every such runner' "${with[@]}" --set-string ci.fleet.labels.Linux.image=runner:1
refuses 'timeout_secs: is a whole number' "${label[@]}" "${with[@]}" --set ci.fleet.labels.dagron.timeout_secs=30
refuses 'pool: is a name of' "${label[@]}" "${with[@]}" --set-string 'ci.fleet.labels.dagron.pool=a pool'
refuses 'named twice, in two cases' "${label[@]}" "${with[@]}" --set-string ci.fleet.labels.Dagron.image=runner:1
refuses 'with: names the label itself' "${label[@]}" "${with[@]}" --set 'ci.fleet.labels.dagron.with={self-hosted,dagron}'
refuses 'is a label of this map.s own' "${label[@]}" "${with[@]}" \
  --set-string ci.fleet.labels.big.image=runner:1 --set 'ci.fleet.labels.big.with={dagron}'
ok "a fleet with no cell, no App or no webhook secret is refused, and so is a label map dagron-ci would not start on"

fleet=(-f "$dir/fleet.yaml" "${with[@]}")
map=$(shows ci.yaml "${fleet[@]}")
holds "$map" "^  name: $RELEASE-ci-fleet\$" || fail "the label map was written to no ConfigMap"
holds "$map" '"priority": 1000000,$' || fail "a whole number of the map was not written as one"
# sum_of TEXT: the map's checksum, as a pod rendered there is annotated
sum_of() { grep -o 'checksum/ci-fleet: [0-9a-f]\{64\}' <<<"$1" || true; }
for placed in engine:engine.yaml deployment:ci.yaml; do
  rendered=$(shows "${placed#*:}" --set "ci.placement=${placed%%:*}" "${fleet[@]}")
  holds "$rendered" 'value: /etc/dagron-ci/fleet/fleet.json$' \
    || fail "ci.placement=${placed%%:*}: dagron-ci is not told where the map is"
  holds "$rendered" 'mountPath: /etc/dagron-ci/fleet$' \
    || fail "ci.placement=${placed%%:*}: dagron-ci's container does not mount the map"
  grep -A1 '^ *configMap:$' <<<"$rendered" | grep -q "name: $RELEASE-ci-fleet\$" \
    || fail "ci.placement=${placed%%:*}: the pod has no volume of the map's ConfigMap"
  [ -n "$(sum_of "$rendered")" ] || fail "ci.placement=${placed%%:*}: the pod is not annotated with the map's checksum"
done
# dagron-ci reads the map once, as it starts: a map that changed has to
# change the pod, or it would go on serving the old one.
before=$(sum_of "$(shows engine.yaml "${fleet[@]}")")
after=$(sum_of "$(shows engine.yaml "${fleet[@]}" --set ci.fleet.labels.dagron.priority=1)")
[ "$before" != "$after" ] || fail "a label map that changed leaves the pod as it was"
holds "$(renders "${with[@]}")" 'ci-fleet\|CI_GITHUB_FLEET_FILE\|fleet\.json' \
  && fail "with no label named, something of a fleet was rendered"
ok "a label map is a file in dagron-ci's container, in either place it runs, and a map that changed starts its pod again"

# ---------------------------------------------------------------- the image --
if [ -n "${CI_IMAGE:-}" ]; then
  log "using $CI_IMAGE as the dagron-ci image"
else
  CI_IMAGE=dagron-ci:k3s-test
  # With the features the published image is built with (docker.yml).
  features=$(bash "$root/scripts/published-features.sh" ci)
  log "building $CI_IMAGE from crates/dagron-ci/Dockerfile, with $features"
  docker build -q -t "$CI_IMAGE" --build-arg "CI_FEATURES=$features" \
    -f "$root/crates/dagron-ci/Dockerfile" "$root" >/dev/null
  built=$CI_IMAGE
fi

# -------------------------------------------------------------- the cluster --
log "starting $K3S_IMAGE"
docker rm -f -v "$N" >/dev/null 2>&1 || true
trap cleanup EXIT
# Its DNS stays, which dagron-ci finds its database by, and its local-path
# StorageClass, which the chart's Postgres claims its volume from. Its CNI
# enforces NetworkPolicy. Eviction for node pressure only below 200 MiB free:
# a runner's disk may be mostly full, and a node evicting every pod proves
# nothing. Image garbage collection is off, for the same disk: the node is
# given every image it runs, and reaches no registry to get one back.
docker run -d --name "$N" --privileged --tmpfs /run --tmpfs /var/run \
  -p "127.0.0.1:$PORT:6443" \
  "$K3S_IMAGE" server \
  --disable=traefik,metrics-server,servicelb --disable-helm-controller \
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
# The pod sandbox's, the cluster DNS's, local-path's and its volume helper's,
# as this k3s names them; Postgres as the chart names it; and the probe's.
pause=$(docker exec "$N" sh -c 'grep -hoE "sandbox(_image)? = \"[^\"]+\"" /var/lib/rancher/k3s/agent/etc/containerd/config.toml' \
  | sed -E 's/.*"([^"]+)"/\1/' | head -1)
[ -n "$pause" ] || fail "k3s's sandbox image: not in its containerd config"
image_of() { k get deploy -n kube-system "$1" -o jsonpath='{.spec.template.spec.containers[0].image}'; }
wait_for "the cluster DNS's deployment" 120 image_of coredns
wait_for "local-path's deployment" 120 image_of local-path-provisioner
dns=$(image_of coredns)
provisioner=$(image_of local-path-provisioner)
helper=$(k get configmap -n kube-system local-path-config -o jsonpath='{.data.helperPod\.yaml}' \
  | awk '$1 == "image:" { print $2; exit }' | tr -d "\"'")
[ -n "$helper" ] || fail "local-path's helper image: not in its config"
postgres=$(helm show values "$chart" | awk '$1 == "image:" && $2 ~ /^postgres:/ { print $2; exit }')
[ -n "$postgres" ] || fail "the chart's Postgres image: not in its values"
for image in "$pause" "$dns" "$provisioner" "$helper" "$postgres" "$CURL_IMAGE"; do
  docker image inspect "$image" >/dev/null 2>&1 || docker pull -q "$image" >/dev/null
done
for image in "$pause" "$dns" "$provisioner" "$helper" "$postgres" "$CURL_IMAGE" "$CI_IMAGE"; do
  docker save "$image" | docker exec -i "$N" ctr -n k8s.io images import - >/dev/null
done
# Pods already backing off from a pull start over, now that their images are
# here.
k delete pod -n kube-system -l k8s-app=kube-dns --wait=false >/dev/null
k delete pod -n kube-system -l app=local-path-provisioner --wait=false >/dev/null
available() { # namespace deployment
  [ "$(k get deploy -n "$1" "$2" -o jsonpath='{.status.availableReplicas}')" = 1 ]
}
wait_for "the cluster DNS to be available" 180 available kube-system coredns
wait_for "local-path to be available" 180 available kube-system local-path-provisioner

# ---------------------------------------------------------------- the chart --
# As a user installs it, with dagron-ci turned on and this image. In a
# Deployment of its own first, with no engine: the image, its commands and
# its Secrets are what is checked, and none of them needs one. --wait: the
# Deployment is Available only once /readyz answers, which is once dagron-ci
# has its schema in the chart's Postgres.
install() { # extra helm arguments...
  helm upgrade --install "$RELEASE" "$chart" --kubeconfig "$kubeconfig" \
    --set ci.enabled=true --set-string "ci.image=$CI_IMAGE" --set ci.imagePullPolicy=Never \
    --set ci.placement=deployment \
    --set dagronApi.enabled=false --set engine.replicas=0 \
    --wait --timeout 300s "$@" >/dev/null
}
log "installing the chart, with dagron-ci"
install || fail "dagron-ci never turned ready"
ok "helm install brings dagron-ci up: its pod is ready, so /readyz answered"

k run probe --image="$CURL_IMAGE" --image-pull-policy=Never --restart=Never \
  --command -- sleep 3600 >/dev/null
k wait --for=condition=Ready pod/probe --timeout=120s >/dev/null
ci=http://$RELEASE-ci:8090
[ "$(k exec probe -- curl -fsS -m 5 "$ci/readyz")" = ready ] || fail "/readyz did not answer ready through the Service"
ok "/readyz answers ready through the Service"

uid=$(k get pod -l app.kubernetes.io/component=dagron-ci -o jsonpath='{.items[0].spec.securityContext.runAsUser}')
image_uid=$(docker image inspect "$CI_IMAGE" --format '{{.Config.User}}' | cut -d: -f1)
[ "$uid" = "$image_uid" ] || fail "the pod runs as $uid, and the image is built for $image_uid"
read_only=$(k get pod -l app.kubernetes.io/component=dagron-ci \
  -o jsonpath='{.items[0].spec.containers[0].securityContext.readOnlyRootFilesystem}')
[ "$read_only" = true ] || fail "the pod's root filesystem is not read-only"
ok "the pod runs as the image's user ($uid), on a read-only root"

k exec "deploy/$RELEASE-ci" -- "$BIN" repo add github.com/acme/widgets >/dev/null \
  || fail "\`$BIN repo add\` did not run in the pod"
k exec "deploy/$RELEASE-ci" -- "$BIN" repo list | grep -q 'github.com/acme/widgets' \
  || fail "\`$BIN repo list\` does not show the repository just connected"
ok "its own commands run in the pod, against the same database"

# deliver [curl arguments...]: a ping, as GitHub sends one when a webhook is
# made, and what it is answered
deliver() {
  k exec probe -- curl -s -m 5 -X POST \
    -H 'content-type: application/json' -H 'x-github-event: ping' \
    -H 'x-github-delivery: k3s-ping' "$@" \
    -d '{"zen":"k3s"}' "$ci/hooks/github"
}
# answered [curl arguments...]: the status of that answer
answered() { deliver -o /dev/null -w '%{http_code}' "$@"; }
[ "$(answered)" = 404 ] || fail "with no webhook secret the receiver answered $(answered), not 404"
ok "with no webhook secret, the receiver answers 404"

log "naming a Secret for the webhook, a token and a cell over TLS, and upgrading"
k create secret generic ci-webhook "--from-literal=secret=$WEBHOOK_SECRET" >/dev/null
k create secret generic ci-api --from-literal=token=k3s-api-token >/dev/null
CELL=default=https://dagron-api.example.test
install --set ci.webhookSecret.name=ci-webhook --set ci.apiTokenSecret.name=ci-api --set-string "ci.cells=$CELL" \
  || fail "dagron-ci never turned ready with its webhook secret, its token and its cell"
given=$(k get deploy "$RELEASE-ci" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CI_CELLS")].value}')
[ "$given" = "$CELL" ] || fail "dagron-ci was given the cell '$given', not '$CELL'"
ok "with a token and a cell over TLS named, it starts with both"
signature=$(printf '%s' '{"zen":"k3s"}' | openssl dgst -sha256 -hmac "$WEBHOOK_SECRET" | sed 's/^.* //')
# The upgrade is over once the new pod is ready. The Service is pointed at it
# a moment after: asked at once, it may still reach the pod before it, which
# has no secret and answers 404, or nothing at all.
refused_unsigned() { [ "$(answered)" = 401 ]; }
wait_for "an unsigned delivery to be refused, through the Service" 30 refused_unsigned
pong=$(deliver -H "x-hub-signature-256: sha256=$signature")
[ "$pong" = '{"pong":true}' ] || fail "a delivery signed with the Secret's value was answered: $pong"
ok "with a Secret named, its key is the webhook's secret: a signed delivery is taken, an unsigned one refused"

# ------------------------------------------------------ in the engine's pod --
# Where the chart puts it by default. The engine's own image is a release's,
# unless ENGINE_IMAGE names one. In its place the pod sandbox's image, which
# only waits: what is checked is the pod the chart makes, and dagron-ci in it.
ENGINE=${ENGINE_IMAGE:-$pause}
log "moving dagron-ci into the engine's pod ($ENGINE as the engine), with a checkout secret"
if [ -n "${ENGINE_IMAGE:-}" ]; then
  docker save "$ENGINE_IMAGE" | docker exec -i "$N" ctr -n k8s.io images import - >/dev/null
fi
k create secret generic ci-checkout "--from-literal=secret=$CHECKOUT_SECRET" >/dev/null
install --set ci.placement=engine --set engine.replicas=1 \
  --set-string "engine.image=$ENGINE" --set engine.imagePullPolicy=Never \
  --set ci.webhookSecret.name=ci-webhook --set ci.apiTokenSecret.name=ci-api --set-string "ci.cells=$CELL" \
  --set ci.checkoutTokenSecret.name=ci-checkout \
  || fail "the engine's pod never turned ready with dagron-ci in it"
k get deploy "$RELEASE-ci" >/dev/null 2>&1 && fail "dagron-ci still has a Deployment of its own"
pod=$(k get pod -l app.kubernetes.io/component=engine -o jsonpath='{.items[0].metadata.name}')
held=$(k get pod "$pod" -o jsonpath='{.spec.containers[*].name}')
[ "$held" = "engine dagron-ci" ] || fail "the engine's pod holds '$held', not the engine and dagron-ci"

# The engine's NetworkPolicy selects this pod. It admits dagron-ci's port
# from anywhere, as that port is on a pod of dagron-ci's own, and the
# engine's own from no more than before. That is read from the policy the
# cluster holds: the stand-in listens on nothing, and to the probe a port
# that is closed and one nothing listens on are refused alike.
rules=$(k get networkpolicy "$RELEASE-engine" \
  -o jsonpath='{range .spec.ingress[*]}{.ports[0].port}{"="}{.from}{"\n"}{end}')
holds "$rules" '^8090=$' || fail "the engine's policy does not admit dagron-ci's port from anywhere: $rules"
[ "$(grep -c '^8080=' <<<"$rules")" = 1 ] \
  || fail "the engine's policy does not have the one rule for the engine's own port: $rules"
holds "$rules" '^8080=.*"kubernetes.io/metadata.name":"monitoring"' \
  || fail "the engine's own port is not admitted from the monitoring namespace alone: $rules"
# through: whether the Service reaches a dagron-ci that is ready. It selects
# other pods now, and is pointed at them a moment after they are ready.
through() { [ "$(k exec probe -- curl -fsS -m 5 "$ci/readyz" 2>/dev/null)" = ready ]; }
wait_for "/readyz to answer ready through the Service, with dagron-ci in the engine's pod" 30 through
pong=$(deliver -H "x-hub-signature-256: sha256=$signature")
[ "$pong" = '{"pong":true}' ] || fail "in the engine's pod, a delivery signed with the Secret's value was answered: $pong"
ok "it runs in the engine's pod, and its Service reaches it there, under a policy that admits its port and the engine's own no further"

# The service-account token, asked of the node's runtime, which is what a
# container is given: the pod mounts none, and the engine's container mounts
# its own.
mounts() { # container name: the paths the runtime mounted in it
  local id
  id=$(k get pod "$pod" -o jsonpath="{.status.containerStatuses[?(@.name==\"$1\")].containerID}")
  docker exec "$N" crictl inspect "${id#containerd://}" | grep -o '"containerPath": *"[^"]*"'
}
token=/var/run/secrets/kubernetes.io/serviceaccount
of_engine=$(mounts engine) || fail "the runtime did not say what the engine's container mounts"
of_ci=$(mounts dagron-ci) || fail "the runtime did not say what dagron-ci's container mounts"
holds "$of_ci" '"/etc/dagron-ci/checkout-secret"' || fail "dagron-ci's container does not mount its Secrets: $of_ci"
holds "$of_engine" "\"$token\"" || fail "the engine's container has no service-account token"
holds "$of_ci" 'serviceaccount' && fail "dagron-ci's container was given the service-account token"
ok "the engine's container holds the service-account token, and dagron-ci's holds none"

# A private repository's checkout: the engine is told to ask dagron-ci at the
# pod's own address, with the secret in a file that its container mounts.
env_of() { k get pod "$pod" -o jsonpath="{.spec.containers[?(@.name==\"engine\")].env[?(@.name==\"$1\")].value}"; }
door=http://127.0.0.1:8090/ci/v1/checkout-tokens
[ "$(env_of DAGRON_CHECKOUT_TOKEN_URL)" = "$door" ] || fail "the engine asks '$(env_of DAGRON_CHECKOUT_TOKEN_URL)', not $door"
file=$(env_of DAGRON_CHECKOUT_TOKEN_SECRET_FILE)
holds "$of_engine" "\"$(dirname "$file")\"" || fail "the engine's container does not mount $file"
# And that door takes the Secret's value and no other. Asked for a run that
# is not dagron-ci's, of a repository it does not serve, it has no credential
# to give and says so (204), once it has looked for the run.
asked() { # curl arguments...: the status of a request for a checkout's credential
  k exec probe -- curl -s -o /dev/null -w '%{http_code}' -m 15 -X POST -H 'content-type: application/json' "$@" \
    -d '{"repository":"https://github.com/nobody/nothing.git","commit":"4b4708e725f29891d22b4c96af9f0a403ff51c7c","run_id":"0d9c4f2e-58a1-4a67-9d3b-2f1e6c7a8b90","task_id":"build","attempt":0}' \
    "$ci/ci/v1/checkout-tokens"
}
[ "$(asked)" = 401 ] || fail "asked with no secret, the door answered $(asked), not 401"
[ "$(asked -H 'authorization: Bearer not-the-secret')" = 401 ] || fail "asked with another secret, the door did not answer 401"
answer=$(asked -H "authorization: Bearer $CHECKOUT_SECRET")
[ "$answer" = 204 ] || fail "asked with the Secret's value, the door answered $answer, not 204"
ok "with a checkout secret named, the engine asks at the pod's own address, and the door takes that secret alone"

if [ -n "${ENGINE_IMAGE:-}" ]; then
  # The engine itself. It stops at start on a token service it does not take
  # or a secret it cannot read, and its Kubernetes executor lists the
  # namespace's pods before it says it is in use: with the token its
  # container mounts, since the pod mounts none.
  said() { # pattern: whether a line of the engine's log matches
    local logged
    logged=$(k logs "$pod" -c engine 2>/dev/null) || return 1
    holds "$logged" "$1"
  }
  wait_for "the engine to reach the cluster with its own token" 90 said 'using KubeExecutor'
  wait_for "the engine to take the token service" 90 \
    said 'checkout credentials from the token service.*127\.0\.0\.1:8090\|127\.0\.0\.1:8090.*checkout credentials from the token service'
  # Its own port, now that the engine listens on it: closed to the probe, a
  # pod of the same namespace that the policy does not name. This cluster's
  # controller refuses such a connection (curl's 7); another drops it (28).
  wait_for "the engine to listen on its own port" 90 said 'management API listening'
  reached=0
  k exec probe -- curl -s -o /dev/null -m 5 "http://$RELEASE-engine:8080/healthz" 2>/dev/null || reached=$?
  case "$reached" in
    7 | 28) ;;
    *) fail "the probe reached the engine's own port (curl exit $reached): the policy that admits dagron-ci's port opened it" ;;
  esac
  restarts=$(k get pod "$pod" -o jsonpath='{.status.containerStatuses[?(@.name=="engine")].restartCount}')
  [ "$restarts" = 0 ] || fail "the engine restarted $restarts times in that pod"
  ok "the engine itself starts there: it lists the namespace's pods with the token it mounts, takes the token service, and its own port stays closed to the namespace"
fi

# --------------------------------------------------------------- fleet mode --
# The map as the chart wrote it, read by dagron-ci itself: it stops as it
# starts on a file it cannot read, so the pod is ready only if it took this
# one. The App is made up here, and registers nothing: dagron-ci asks GitHub
# for a runner once a job it serves waits, so where it would ask is set to
# the pod's own machine, where nothing answers, and what is made up here is
# shown to nobody.
log "naming a fleet's label map and an App, and upgrading"
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 2>/dev/null \
  | k create secret generic ci-app --from-file=private-key.pem=/dev/stdin >/dev/null
was=$pod
install --set ci.placement=engine --set engine.replicas=1 \
  --set-string "engine.image=$ENGINE" --set engine.imagePullPolicy=Never \
  --set ci.webhookSecret.name=ci-webhook --set ci.apiTokenSecret.name=ci-api --set-string "ci.cells=$CELL" \
  --set ci.checkoutTokenSecret.name=ci-checkout \
  --set-string ci.githubApp.id=7 --set ci.githubApp.keySecret.name=ci-app \
  --set 'ci.extraEnv[0].name=CI_GITHUB_API_URL' --set 'ci.extraEnv[0].value=http://127.0.0.1:9' \
  -f "$dir/fleet.yaml" \
  || fail "the engine's pod never turned ready with a fleet's label map"
# The upgrade is over once the new pod is ready, and the one before it may
# still be going: the pod asked about below is the one that read the map.
gone() { ! k get pod "$was" >/dev/null 2>&1; }
wait_for "the pod from before the label map to go" 180 gone
pod=$(k get pod -l app.kubernetes.io/component=engine -o jsonpath='{.items[0].metadata.name}')
file=$(k get pod "$pod" -o jsonpath='{.spec.containers[?(@.name=="dagron-ci")].env[?(@.name=="CI_GITHUB_FLEET_FILE")].value}')
[ "$file" = /etc/dagron-ci/fleet/fleet.json ] || fail "dagron-ci is told the label map is at '$file'"
serving() { # whether dagron-ci has said which labels it serves
  local logged
  logged=$(k logs "$pod" -c dagron-ci 2>/dev/null) || return 1
  holds "$logged" 'fleet mode: .*dagron, dagron-large\|dagron, dagron-large.*fleet mode: '
}
wait_for "dagron-ci to say which labels it serves" 60 serving
wait_for "the Service to reach the pod that read the label map" 30 through
# job_for LABELS ID: a job of GitHub Actions' own that waits and asks for
# those labels (the items of a JSON list), delivered as GitHub delivers one,
# and what it is answered
job_for() {
  local body signed
  body='{"action":"queued","workflow_job":{"id":'"$2"',"run_id":1,"run_attempt":1,"workflow_name":"CI","name":"build","head_sha":"4b4708e725f29891d22b4c96af9f0a403ff51c7c","labels":['"$1"']},"repository":{"id":7,"full_name":"acme/widgets","html_url":"https://github.com/acme/widgets"}}'
  signed=$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$WEBHOOK_SECRET" | sed 's/^.* //')
  k exec probe -- curl -s -m 5 -X POST -H 'content-type: application/json' \
    -H 'x-github-event: workflow_job' -H "x-github-delivery: k3s-job-$2" \
    -H "x-hub-signature-256: sha256=$signed" -d "$body" "$ci/hooks/github"
}
answer=$(job_for '"ubuntu-latest"' 1)
holds "$answer" 'asks for no label this fleet serves' || fail "a job for a runner of GitHub's own was answered: $answer"
answer=$(job_for '"self-hosted","Dagron-Large"' 2)
holds "$answer" '"class":"dagron-large"' || fail "a job that asks for a label of the map was answered: $answer"
ok "the label map the chart wrote is the one dagron-ci serves: a job is taken for the label it asks for, and another runner's is left alone"

echo "== dagron-ci is installed by the chart, and ready"
