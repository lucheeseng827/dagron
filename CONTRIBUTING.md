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

CI runs fmt, clippy, build, and test on every PR; please make sure they pass
locally first.

## Expectations

- Keep the public surface small and legible — the `Executor` and
  `WorkflowSource` traits are the extension points; prefer adding behind them.
- New source files carry the SPDX header: `// SPDX-License-Identifier: Apache-2.0`.
- Add tests for new behavior.
- Use clear, conventional commit messages (e.g. `feat:`, `fix:`, `docs:`).

## Code of Conduct

This project follows the [Contributor Covenant](https://www.contributor-covenant.org/).
Be respectful.
