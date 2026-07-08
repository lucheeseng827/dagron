# Releasing `dagron-sdk` to PyPI

> Module: `rust_modules/lab/module_54/sdks/python`
> Status: **manual runbook** — how the Python SDK is built and published to the
> Python Package Index (PyPI), the `pip` repository. Companion to the cargo-crate
> plan in [`../../docs/RELEASE.md`](../../docs/RELEASE.md).
> The package is build-ready today (PEP 621 metadata + `setuptools` backend); the
> steps below are the operator process until it is wired into CI (§6).

## 0. What ships

One pure-Python module, `dagron.py`, packaged as **`dagron-sdk`** (the import name
stays `dagron`). Standard-library only, so there are no runtime dependencies to
resolve — `pip install dagron-sdk` pulls just this package.

The version is single-sourced from **`dagron.__version__`**; `pyproject.toml`
reads it via `[tool.setuptools.dynamic]`. Bump it in **one** place (§2).

## 1. Prerequisites (one-time)

- **PyPI account** with access to the `dagron-sdk` project (and a **TestPyPI**
  account at <https://test.pypi.org> for dry-runs — separate login from PyPI).
- **API tokens**, not passwords:
  - PyPI: <https://pypi.org/manage/account/token/> → scope it to the
    `dagron-sdk` project once the project exists (use an account-wide token for the
    very first upload, then re-scope).
  - TestPyPI: <https://test.pypi.org/manage/account/token/>.
  - The token *is* the password; the username is the literal `__token__`.
- **Build tooling** in your environment (these are *build*-time only, never runtime
  deps of the SDK):

  ```bash
  python3 -m pip install --upgrade build twine
  ```

- Store tokens in `~/.pypirc` (mode `600`) so `twine` finds them without prompts:

  ```ini
  [distutils]
  index-servers = pypi testpypi

  [pypi]
  username = __token__
  password = pypi-AgEN...                # your PyPI token

  [testpypi]
  repository = https://test.pypi.org/legacy/
  username = __token__
  password = pypi-AgEN...                # your TestPyPI token
  ```

  Or pass `TWINE_USERNAME=__token__ TWINE_PASSWORD=<token>` as env vars instead.

## 2. Bump the version

[SemVer](https://semver.org/). Pre-1.0: minor = breaking, patch = additive/fix
(matches the crate convention in [`../../docs/RELEASE.md`](../../docs/RELEASE.md) §2).
Edit the single source of truth:

```python
# dagron.py
__version__ = "0.3.0"   # was 0.2.0
```

`pyproject.toml` picks this up automatically — do **not** hard-code a version there.
Update [`../../CHANGELOG.md`](../../CHANGELOG.md) and the coverage state in
[`ROADMAP.md`](ROADMAP.md) in the same commit. Land it on the branch and merge
before tagging (§5).

## 3. Build & check the artifacts

From this directory (`sdks/python`):

```bash
rm -rf dist build ./*.egg-info          # always build from a clean tree
python3 -m build                        # writes dist/dagron_sdk-X.Y.Z-py3-none-any.whl
                                        #    and  dist/dagron-sdk-X.Y.Z.tar.gz
python3 -m twine check dist/*           # validates the long-description / metadata
```

`twine check` must report `PASSED` for both files. Quick sanity that the wheel
imports and reports the right version, in a throwaway venv:

```bash
python3 -m venv /tmp/dagron-rel && . /tmp/dagron-rel/bin/activate
pip install dist/dagron_sdk-*.whl
python -c "import dagron; print(dagron.__version__)"   # must match §2
python -c "from dagron import Dag, Client, DagronError"  # public API imports
deactivate && rm -rf /tmp/dagron-rel
```

Run the test suite once more before publishing (`python3 -m unittest`).

## 4. Dry-run on TestPyPI (do this every release)

Publish to TestPyPI first and install from there to prove the package is well-formed
end-to-end:

```bash
python3 -m twine upload --repository testpypi dist/*
# Install from TestPyPI; fall back to real PyPI for any (here: zero) deps.
pip install --index-url https://test.pypi.org/simple/ \
            --extra-index-url https://pypi.org/simple/ dagron-sdk
```

TestPyPI is wiped periodically and a version number can't be reused, so a throwaway
`X.Y.Z.devN` is fine here if you need to re-upload.

## 5. Publish to PyPI

A PyPI version is **immutable and can never be reused** — you cannot overwrite or
re-upload `X.Y.Z`. Be sure §3–4 passed.

```bash
python3 -m twine upload dist/*          # uploads the wheel + sdist to PyPI
```

Then tag the release so the published artifact is traceable to a commit. This repo
uses prefixed monorepo tags — use a
distinct prefix for the SDK so it doesn't collide with engine releases:

```bash
git fetch origin
git tag dagron-sdk-vX.Y.Z origin/<release-branch>   # tag the fetched commit, not a stale local one
git push origin dagron-sdk-vX.Y.Z
```

Finally verify the real thing installs from PyPI in a clean container/venv:

```bash
pip install dagron-sdk==X.Y.Z
python -c "import dagron; print(dagron.__version__)"
```

## 6. Automating it later (recommended: Trusted Publishing)

The manual `twine` flow above is the bootstrap. Before this is routine, move to
**PyPI Trusted Publishing (OIDC)** so no long-lived token sits in CI:

1. On PyPI: *Manage project → Publishing → Add a trusted publisher* → GitHub Actions,
   pointing at this repo + the release workflow + environment.
2. Add a workflow gated on a `dagron-sdk-v*` tag (or a manual `workflow_dispatch`)
   that runs `python -m build` then `pypa/gh-action-pypi-publish` — it mints a
   short-lived OIDC token, so there is **no `PYPI_TOKEN` secret** to manage.
3. Keep the same gate as local: `python -m unittest` + `twine check` must pass
   before the publish step.

This mirrors the engine's `release-plz` → crates.io pipeline
([`../../docs/RELEASE.md`](../../docs/RELEASE.md) §4–5), one ecosystem over.

## 7. Gotchas (learned the hard way)

- **Versions are immutable on PyPI.** A typo'd or broken `X.Y.Z` is burned forever —
  yank it and ship `X.Y.(Z+1)`. That's why §4's TestPyPI dry-run is not optional.
- **Bump the version in `dagron.py` only.** `pyproject.toml` is `dynamic` — a stray
  hard-coded `version =` there would shadow the module and silently drift.
- **Always build from a clean `dist/`.** A leftover wheel from a prior version will
  get uploaded by `twine upload dist/*`. `rm -rf dist` first.
- **`__token__` is the username**, the API token is the password — for *both* PyPI
  and TestPyPI, and they are different tokens on different accounts.
- **System `pip wheel --no-build-isolation` can fail on Debian** with
  `AttributeError: install_layout` — that's a distro-patched-`distutils` quirk, not
  a package problem. Use `python -m build` (isolated) or a venv with upstream
  `setuptools`; the package builds cleanly there (verified: sdist + metadata
  resolve `Version: X.Y.Z` from `dagron.__version__`).
- **The import name is `dagron`, the distribution is `dagron-sdk`.** `pip install
  dagron-sdk`, then `import dagron`. Don't expect `import dagron_sdk`.
