# CI quickstart: a first green check

A GitHub repository, a pipeline in it, and a check on its next commit, on
one machine with Docker. About ten minutes, most of them on GitHub's pages.

What you end with: a push to the repository runs its `.dagron/ci`
pipelines, each job in a container of its own with the commit checked out,
and each job's outcome is a status on the commit.

It reaches github.com and nothing reaches back: dagron-ci finds a push by
looking, once a minute here. So it runs on a laptop, behind anything.

> This is `compose.ci.yaml`: a way to meet it, not a deployment. Its
> credentials are in the file and its engine holds the container runtime's
> socket. [On Kubernetes](#on-kubernetes-with-the-chart) is further down.

## What you need

- Docker with `docker compose`, or Podman with a compose provider.
- A GitHub repository you may install a GitHub App on. A private one is
  fine: that is what the App is for.

## 1. Make a GitHub App

dagron-ci acts on GitHub as an App of yours. It reads the repository as the
App, posts each job's status as the App, and gives each job's checkout a
token of the App's for that one repository, which it revokes after.

On GitHub: **Settings → Developer settings → GitHub Apps → New GitHub App**.

| Field | Set it to |
|---|---|
| GitHub App name | anything: `dagron-ci-<you>` |
| Homepage URL | anything: it is required and not used |
| Webhook → Active | **off**. Nothing is delivered to this machine |
| Repository permissions → Contents | **Read-only** |
| Repository permissions → Pull requests | **Read-only**. **Read and write** only for a comment on each pull request (`CI_GITHUB_PR_COMMENT=on`), which this quickstart does not turn on |
| Repository permissions → Commit statuses | **Read and write** |
| Repository permissions → Issues | leave at **No access**. **Read and write** only for an issue of a job that keeps flaking (`CI_FLAKE_ISSUES`), which this quickstart does not turn on |
| Repository permissions → Checks | leave at **No access**. **Read and write** only for check runs in place of statuses (`CI_GITHUB_CHECKS=on`), which this quickstart does not turn on |
| Where can this GitHub App be installed? | Only on this account |

Then, on the App's page:

1. Note the **App ID**, a number.
2. **Generate a private key.** A `.pem` file is downloaded: keep it where
   only you can read it.
3. **Install App**, on the one repository. An App reads only what it is
   installed on.

## 2. Start it

From a checkout of dagron (or with `compose.ci.yaml` alone, which is all it
needs):

```sh
export CI_GITHUB_APP_ID=123456                        # the App ID
export CI_GITHUB_APP_KEY=$HOME/keys/dagron-ci.private-key.pem
docker compose -f compose.ci.yaml up -d
```

It pulls the images, starts Postgres, the engine, dagron-api and dagron-ci,
and makes dagron-ci its own identity in dagron-api. dagron-ci's log warns
that it has no webhook secret: that is this setup, which looks instead. When
`docker compose -f compose.ci.yaml ps` shows `dagron-ci` running, it is
ready:

```sh
curl -s localhost:8090/readyz        # ready
```

The console is at <http://localhost:8080>: sign in with `admin@local` /
`dagron-admin`.

With Podman, name its socket first: `export DOCKER_SOCK=/run/user/1000/podman/podman.sock`
for a rootless one.

**From a checkout, before a release has the images.** `compose.ci.yaml`
names the images of 0.11.1. Until they are published, or to try a change,
build them from the tree, and name both files in every `docker compose`
command below:

```sh
docker compose -f compose.ci.yaml -f compose.ci.build.yaml up -d --build
```

The first build compiles four Rust binaries and takes a while.

## 3. Connect the repository

```sh
docker compose -f compose.ci.yaml exec dagron-ci \
  dagron-ci repo add github.com/OWNER/NAME
```

`dagron-ci repo list` shows it, and when it was last looked at. The first
look only notes where each branch is: nothing runs for commits that were
there before.

## 4. Add a pipeline, and push

In the repository, copy a starter to `.dagron/ci/ci.yaml`:
[`examples/ci`](../examples/ci) has one for Rust, Go, Node, Python and
Java, and one for a dbt project.
Or the smallest there is:

```yaml
# .dagron/ci/ci.yaml
name: ci
on:
  push: {}
  pull_request: {}
tasks:
  - name: hello
    docker_image: docker.io/library/alpine:3.20
    steps:
      - run: echo "checked out $CI_SHA of $CI_REPO"
      - run: ls
```

Check it before it is pushed. This needs no server:

```sh
docker run --rm -v "$PWD:/repo" -w /repo \
  docker.io/mancube/dagron-ci:0.11.1 validate
# .dagron/ci/ci.yaml: ok (ci: 1 task; on push, pull_request)
```

(With images built from a checkout, that image is `localhost/dagron-ci:dev`.)

Commit it and push.

## 5. See the check

Within a minute dagron-ci sees the commit, and the commit on GitHub has a
status for the job, `dagron-ci/ci/hello (push)`: pending while it runs, and
then how it ended. Its **Details** link is the run in the console, where
the job's log is, by step. **Repositories**, in the console's menu, has
the repository, and its branch with that run.

From a terminal, the same. A session of yours in dagron-api is the token:

```sh
TOKEN=$(curl -s localhost:8080/api/login -H 'content-type: application/json' \
  -d '{"email":"admin@local","password":"dagron-admin"}' \
  | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
ci() {
  docker compose -f compose.ci.yaml exec -e DAGRON_CI_URL=http://127.0.0.1:8090 \
    -e DAGRON_CI_TOKEN="$TOKEN" dagron-ci dagron-ci "$@"
}
ci logs RUN_ID            # the id in the Details link: each line under its job
ci rerun RUN_ID           # run its failed jobs again
ci rerun RUN_ID --all     # or every job of it, one that succeeded too
ci cancel RUN_ID          # stop it
ci run --remote --repo github.com/OWNER/NAME --ref refs/heads/main --wait --timeout 900
```

With `dagron-ci` on this machine too (a release's `dagron-ci-<version>-<target>`
archive, from 0.11.1), log in to it once instead. Paste an
API token of yours (`POST /api/tokens`, [HOWTO §5](HOWTO.md#5-api-tokens-for-ci-and-scripts):
a session's ends with the session); it is not shown as it is typed, and is
kept for that server once the server has said whose it is. The commands
then need neither variable:

```sh
dagron-ci login --server http://127.0.0.1:8090
dagron-ci logs RUN_ID
```

That is the whole loop. [`CI_PIPELINES.md`](CI_PIPELINES.md) is the
reference for what a pipeline may say, and [`CI.md`](CI.md) says what
dagron-ci refuses, and what this release does not do.

## When nothing happens

| What you see | Look at |
|---|---|
| No status on the commit | `docker compose -f compose.ci.yaml exec dagron-ci dagron-ci events`: each event, its state and why. `skipped … on: does not take push` is a pipeline whose `on:` does not name the event |
| dagron-ci's log says `GitHub will not mint an installation token` | the App ID or the key is not the App's, the App is not installed on the repository, or it lacks one of the three permissions. A public repository is then read as anyone reads it, and gets no status; a private one is not read, and `dagron-ci repo list` shows a `poll_error` |
| The event reads `has no pipelines in .dagron/ci` | the commit has none, or it is a private repository's and the App cannot read it |
| A status that reads `not run: …` | the pipeline was refused, and that is why: a key that is not one, a job without an image the runtime can pull |
| The job fails at its checkout | `docker compose -f compose.ci.yaml logs engine`, and the job's log in the console. A private repository needs the App installed on it |
| `dagron-ci` keeps restarting | its log names the setting it stopped on. `CI_GITHUB_APP_KEY` must be the path of the `.pem` file |

## A webhook instead of looking

Looking is enough and is a minute behind. Where GitHub can reach dagron-ci,
a webhook makes a push run at once, and the sweep stays as the net under
it: give dagron-ci a secret (`CI_GITHUB_WEBHOOK_SECRET_FILE`), turn the
App's webhook on with that secret, point it at `https://<dagron-ci>/hooks/github`,
and subscribe it to **Push** and **Pull request**. A commit seen both ways
runs once.

## On Kubernetes, with the chart

The Helm chart installs dagron-ci with `ci.enabled=true`, as a container in
the engine's pod, beside dagron-api. Three things differ from the machine
above, and each is a thing to have before starting:

1. **TLS before dagron-api.** dagron-ci sends its token for dagron-api to
   its cell, and starts on no cell over plain http unless it is its own
   machine. The chart's Services serve no TLS, so name what terminates it:
   with `ingress.tls.enabled`, the ingress host
   (`ci.cells=default=https://dagron.example.com`), which dagron-ci must
   resolve from inside the cluster and whose certificate it must trust.
2. **Secrets you make.** The chart makes none of them, so none lands in
   values: dagron-ci's API token for dagron-api (`ci.apiTokenSecret`, a
   token made in the console), the App's private key (`ci.githubApp.id`,
   `ci.githubApp.keySecret`), and, for private repositories, a secret of
   your own making that the engine asks dagron-ci with
   (`ci.checkoutTokenSecret`).
3. **The engine's two images.** The chart does not set them: give the
   engine `DAGRON_CI_EXEC_IMAGE` and `DAGRON_CHECKOUT_IMAGE` through
   `engine.extraEnv`.

```sh
kubectl create secret generic dagron-ci-api --from-literal=token='<a dagron-api token>'
kubectl create secret generic dagron-ci-app --from-file=private-key.pem=dagron-ci.private-key.pem
kubectl create secret generic dagron-ci-checkout --from-literal=secret="$(openssl rand -hex 32)"
helm upgrade --install dagron oci://registry-1.docker.io/mancube/dagron --version 0.11.1 \
  --set ingress.enabled=true --set ingress.host=dagron.example.com \
  --set ingress.tls.enabled=true \
  --set ci.enabled=true \
  --set-string ci.cells=default=https://dagron.example.com \
  --set ci.apiTokenSecret.name=dagron-ci-api \
  --set-string ci.githubApp.id=123456 \
  --set ci.githubApp.keySecret.name=dagron-ci-app \
  --set ci.checkoutTokenSecret.name=dagron-ci-checkout \
  --set 'engine.extraEnv[0].name=DAGRON_CI_EXEC_IMAGE' \
  --set 'engine.extraEnv[0].value=docker.io/mancube/dagron-ci-exec:0.11.1' \
  --set 'engine.extraEnv[1].name=DAGRON_CHECKOUT_IMAGE' \
  --set 'engine.extraEnv[1].value=docker.io/alpine/git:2.54.0'
kubectl exec deploy/dagron-engine -c dagron-ci -- dagron-ci repo add github.com/OWNER/NAME
```

**A private repository's checkout, and why dagron-ci shares the engine's
pod.** A job's checkout is given its credential by the engine asking
dagron-ci for one (`DAGRON_CHECKOUT_TOKEN_URL`), with a secret both hold in a
file. The engine takes no address for that over plain http but its own
machine's. In one pod the address is the pod's own, as it is in the stack
above, so there is no TLS to set up between the two: with
`ci.checkoutTokenSecret` named, the chart tells the engine where to ask and
mounts the secret in both containers.

Sharing the pod has its price. dagron-ci has the engine's replicas and the
engine's place on a node, the pod is ready when both are, and dagron-ci
reaches the engine's own API on the address they share. The chart's
`values.yaml` says each, under `ci.placement`.

`ci.placement=deployment` gives dagron-ci a Deployment of its own instead.
The chart's engine is then not told to ask it, so a public repository's
pipelines run and a private one's fail at their checkout. dagron-ci's own
reads and its statuses are the App's either way.
