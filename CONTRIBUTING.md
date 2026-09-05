# Contributing to dagron

Thanks for your interest in improving dagron!

## Developer Certificate of Origin (DCO)

We use the [DCO](https://developercertificate.org/) rather than a CLA. By signing
off on your commits you certify that you wrote the patch or otherwise have the
right to submit it under the project's Apache-2.0 license.

Add a sign-off line to every commit:

```bash
git commit -s -m "your message"
```

This appends `Signed-off-by: Your Name <you@example.com>` (using your `git`
identity). Patent grant comes from Apache-2.0 itself.

## Development

```bash
cargo build
cargo test
cargo fmt
cargo clippy --all-targets -- -D warnings
```

`make ci` runs all four the way CI does, which is not quite the way they read
above: this workspace cannot be built as one unit (`dagron-core` compiles exactly
one sqlx backend, the engine wants sqlite and `dagron-api`/`dagron-gitops` want
postgres), so each command covers the two feature worlds separately plus the
`mqtt` one that no default build compiles. A bare `cargo test` is still useful
while you work; `make ci` is what tells you whether the PR will be green.
`make` on its own lists every other target.

CI runs on every PR. **Build and test must pass** — those are the gate. `fmt` and
`clippy` also run but are advisory for now: there is no `rustfmt.toml` yet, so stock
rustfmt disagrees with this tree in about 960 places, and clippy has a small standing
backlog. Both will report things that have nothing to do with your change. Read them
for the files you touched and ignore the rest.

## Expectations

- Keep the public surface small and legible — the `Executor` and
  `WorkflowSource` traits are the extension points; prefer adding behind them.
- New source files carry the SPDX header: `// SPDX-License-Identifier: Apache-2.0`.
- Add tests for new behavior.
- Use clear, conventional commit messages (e.g. `feat:`, `fix:`, `docs:`).

## How your PR lands

`main` here is a **published snapshot** of the tree dagron is developed in, not a
branch that takes merges directly — every commit on it arrives from a sync.

So a maintainer will not click Merge on your PR. Instead:

1. CI runs here and review happens here. This is where the discussion lives.
2. Once it is accepted a maintainer labels it `ready-to-backport`, and a daily job
   carries your commits upstream with your authorship (`Co-authored-by`) and your
   `Signed-off-by` preserved. Accepted PRs travel together in a day's batch, so
   expect the label to sit for up to a day before anything else happens — that gap
   is the schedule, not a stalled review.
3. It returns to this repository in the next sync and ships in a tagged release.
4. Your PR is then **closed with a link to the release that carries it.**

A closed PR here is not a rejection — a rejection would be said plainly in the
thread. The detour is mechanical: the sync republishes this branch wholesale, so a
merge commit on this side would be overwritten by the next release and your change
would disappear. Landing it upstream is what makes it stick.

Two things make that go smoothly:

- **Sign off every commit** (`git commit -s`). The backport refuses commits without
  a `Signed-off-by`, so a PR missing one cannot be taken at all.
- **Do commit the `Cargo.lock` your change needs.** CI here builds `--locked`, so a
  manifest edit without a refreshed lockfile fails. `cargo fetch` after the edit
  updates the minimum required. Note that adding a dependency can legitimately move
  several unrelated pins, because the new crate's own constraints ripple — that is
  expected and not something to fight.

  Your lockfile is not carried upstream; it is regenerated there against a workspace
  slightly wider than this one, which may hold crates not yet published here. So do
  not be surprised if the released lockfile differs a little from yours. That is the
  mechanism working, not a rejection of your resolution.

## Code of Conduct

This project follows the [Contributor Covenant](https://www.contributor-covenant.org/).
Be respectful.
