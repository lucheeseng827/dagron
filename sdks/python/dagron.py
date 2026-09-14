"""dagron Python SDK — author DAGs in code and drive the full dagron control plane.

Two layers, both standard-library only (``json`` + ``urllib``):

* :class:`Dag` — a fluent builder for a dagron workflow spec. ``to_json()`` emits
  valid dagron input (dagron parses YAML, and JSON is a YAML subset) and the
  builder validates the graph (unique names, known deps, task kinds, trigger
  rules, template calls, acyclicity) client-side, mirroring the server's
  ``validate_graph``.

* :class:`Client` — a thin, typed wrapper over the **dagron-api** gateway
  (``/api/...``, JWT- or token-authenticated). It covers the same surface the web
  UI uses — login and access tokens, runs, workflows and their lifecycle,
  schedules, backfills, environments and secrets, datasets, archive, triage,
  dead-letters, GitOps repos, artifacts, settings and metrics — so an automation
  can do anything the UI can without hand-rolling REST calls.

    from dagron import Dag, Client

    dag = Dag("etl")
    extract = dag.task("extract", image="alpine", command=["echo", "hi"])
    dag.task("load", image="alpine", command=["true"], depends_on=[extract])

    api = Client("http://localhost:8080")
    api.login("admin@example.com", "hunter2222")   # stores the session token
    run_id = api.submit_run(dag)                    # trigger an ad-hoc run
    result = api.wait_run(run_id)                   # server-side long poll
    print(result["status"])

For automation, mint a personal access token once (``create_token``) and hand it
to :meth:`Client.from_env` via ``DAGRON_API_URL`` / ``DAGRON_TOKEN`` — a password
never has to be stored.

The gateway expects a DAG submitted as ``{"yaml": "<spec>"}``; the SDK wraps that
for you, so callers pass a :class:`Dag`, a spec ``dict``, or a YAML/JSON string.

Targeting the no-auth engine ops API instead (``/runs``, raw-body submit) is on the
roadmap (see ``ROADMAP.md``); today the client speaks the gateway dialect.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Union

__all__ = [
    "Dag",
    "Template",
    "Recipe",
    "RecipeFile",
    "BUILD_GENERATOR_VERSION",
    "Client",
    "DagronError",
    "SpecLike",
    "LOG_FILTER_PARAMS",
    "TRIGGER_RULES",
    "TERMINAL_RUN_STATUSES",
    "log_filter_params",
    "__version__",
]
#: Lined up with the dagron-api version this SDK covers (the convention set when
#: the SDKs were versioned to the 0.3.0 API), not with the SDK's own history —
#: so the number answers "which gateway does this speak to".
__version__ = "0.9.0"

#: Seconds of transport headroom added on top of a server-side wait budget, so
#: the client's own socket timeout can never abort a long poll a moment before
#: the gateway answers it.
WAIT_TRANSPORT_MARGIN_SECS = 5

#: The server's bounds on ``GET /api/runs/{id}/wait?timeout_secs=`` — it clamps
#: to this range and defaults to the low end, so the client can size its
#: transport timeout against the budget the server will actually honour.
WAIT_BUDGET_DEFAULT_SECS = 30
WAIT_BUDGET_MIN_SECS = 1
WAIT_BUDGET_MAX_SECS = 600

#: Run statuses the engine treats as terminal (no further transitions).
TERMINAL_RUN_STATUSES = frozenset({"succeeded", "failed", "cancelled"})

#: When a task runs relative to its dependencies' outcomes (engine
#: ``trigger_rule``). Unset means ``all_success``.
TRIGGER_RULES = frozenset(
    {"all_success", "all_done", "one_failed", "all_failed", "none_failed"}
)

#: The keys a ``repeat:`` block may carry, as the engine spells them. Checked at
#: build time because the alternative is a spec that validates locally and is
#: then rejected on the wire — a camelCased ``maxIterations``, say, would leave
#: the required field unset with nothing to say so.
REPEAT_KEYS = frozenset({"until", "max_iterations", "delay_secs"})

#: Task kinds that run no command: they park instead (a human gate, a child-run
#: trigger, a deferrable sensor). Each is refused a ``command`` / ``template`` /
#: ``workflow_ref``, mirroring the server.
COMMANDLESS_TASK_TYPES = frozenset({"approval", "workflow", "wait"})

#: Anything accepted where a DAG spec is expected: a builder, a spec mapping, or
#: an already-serialised YAML/JSON string.
SpecLike = Union["Dag", Mapping[str, Any], str]


# ── Builder ───────────────────────────────────────────────────────────────────



#: Identifies the recipe format *and* the Dockerfile the builder synthesises from
#: it. It is hashed with the recipe, so bumping it re-tags every image on
#: purpose: two images with the same tag must have been built the same way.
#: Must equal ``GENERATOR_VERSION`` in ``ee/dagron-build/src/recipe.rs``.
BUILD_GENERATOR_VERSION = "dagron-build/v2"

#: Fields of a recipe, in the order the builder serialises them. **Declaration
#: order, not alphabetical** — Rust structs serialise in declaration order, and
#: the canonical form is hashed, so reordering this list silently re-tags every
#: image. Same reason the recipe's own ``env`` is sorted: it is a ``BTreeMap``
#: there.
#:
#: It does not drive :meth:`Recipe.to_dict` — each field there has its own emit
#: condition (``workdir=""`` is emitted, ``apt=[]`` is not), which a list of
#: names cannot carry. So it is a *guard* instead: ``test_dagron`` asserts
#: ``to_dict()`` emits exactly these keys in exactly this order. Left unused it
#: was worse than absent, because it read like the source of truth it is not.
_RECIPE_FIELDS = (
    "name", "base", "apt", "pip", "env", "workdir", "files", "run", "user",
    "keep_entrypoint", "platform", "dockerfile",
)

#: `\Z`, not `$`. In Python `$` also matches *just before* a trailing newline, so
#: `^...$` accepts "etl\n" — a name the builder refuses and which would have made
#: a spec the engine dead-letters. `\Z` is the true end of string.
#: Exactly Rust's ``char::is_whitespace`` — the Unicode ``White_Space``
#: property, which is what the builder tests with.
#:
#: Not ``str.isspace()``. Python also counts U+001C-U+001F (the file, group,
#: record and unit separators) as space and Rust does not, so ``isspace()`` here
#: refused recipes the builder accepts — the opposite of the usual bug, and just
#: as wrong: an author blocked from something legal. Measured against the
#: builder rather than assumed.
_RUST_WHITESPACE = frozenset(
    "\t\n\x0b\x0c\r \x85\xa0\u1680\u2028\u2029\u202f\u205f\u3000"
) | frozenset(chr(c) for c in range(0x2000, 0x200B))


def _has_ws(value: str) -> bool:
    """Does ``value`` contain a character the builder considers whitespace?"""
    return any(c in _RUST_WHITESPACE for c in value)


def _blank(value: str) -> bool:
    """Is ``value`` empty or only builder-whitespace? (Rust's ``str::trim``.)"""
    return all(c in _RUST_WHITESPACE for c in value)


def _rstrip_ws(value: str) -> str:
    """``str::trim_end`` as the builder means it."""
    end = len(value)
    while end and value[end - 1] in _RUST_WHITESPACE:
        end -= 1
    return value[:end]


_RECIPE_NAME_RE = re.compile(r"^[a-z0-9]([a-z0-9._-]*[a-z0-9])?\Z")
#: The builder's ceiling on a recipe name. A name becomes a repository path
#: component, and registries have their own limits well below this.
_MAX_RECIPE_NAME_LEN = 128
_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\Z")  # `\Z`: see _RECIPE_NAME_RE


class RecipeFile:
    """A text file the recipe places in the image.

    Binary payloads do not belong in a recipe — they belong in a base image —
    so ``content`` is text.
    """

    __slots__ = ("path", "content", "executable")

    def __init__(self, path: str, content: str, *, executable: bool = False) -> None:
        self.path = path
        self.content = content
        self.executable = executable

    def to_dict(self) -> Dict[str, Any]:
        """The canonical form: ``executable`` is omitted when false, because the
        builder skips it when false and the two must serialise identically."""
        d: Dict[str, Any] = {"path": self.path, "content": self.content}
        if self.executable:
            d["executable"] = True
        return d

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        kind = "executable" if self.executable else "file"
        return f"RecipeFile({self.path!r}, {len(self.content)} chars, {kind})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RecipeFile):
            return NotImplemented
        return self.to_dict() == other.to_dict()


class Recipe:
    """What a task's image should contain, instead of a Dockerfile.

    Pass one straight to :meth:`Dag.task` as ``image=`` and the builder task is
    added for you::

        recipe = Recipe("etl", "python:3.12-slim", pip=["duckdb==1.1.3"])
        dag = Dag("nightly")
        dag.task("report", image=recipe, command=["python", "/app/report.py"])

    **The tag is a function of the recipe.** ``tag()`` is
    ``r-<first 16 hex of sha256>`` over the canonical form, so the image
    reference is known before the image exists: downstream tasks can name it at
    author time, and re-submitting an unchanged recipe is a registry lookup
    rather than a build.

    That also makes this class a *contract*, not a convenience. The builder that
    actually produces the image is a separate program in a separate language
    (``ee/dagron-build``), and if the two disagree about the canonical form by
    one byte, an author pins their task to an image no build will ever push and
    nothing fails until the run does. ``sdks/recipe-vectors.json`` holds golden
    vectors generated from the builder; the test suite asserts against them.

    This describes the *image*, not the build: a ``run`` line is arbitrary code
    executed as root inside the builder's sandbox. Where that sandbox is and
    what it may reach is the deployment's business, not the recipe's.
    """

    __slots__ = ("name", "base", "apt", "pip", "env", "workdir", "files", "run",
                 "user", "keep_entrypoint", "platform", "dockerfile")

    def __init__(
        self,
        name: str,
        base: str,
        *,
        apt: Optional[Sequence[str]] = None,
        pip: Optional[Sequence[str]] = None,
        env: Optional[Mapping[str, str]] = None,
        workdir: Optional[str] = None,
        files: Optional[Sequence[Union["RecipeFile", Mapping[str, Any]]]] = None,
        run: Optional[Sequence[str]] = None,
        user: Optional[str] = None,
        keep_entrypoint: bool = False,
        platform: Optional[str] = None,
        dockerfile: Optional[str] = None,
    ) -> None:
        """Describe an image.

        ``name`` is the repository's last component (``etl``); the registry and
        workspace prefix are supplied where the build runs, not here.
        ``base`` is any image reference. ``apt`` and ``pip`` install packages;
        ``env`` becomes ``ENV`` lines; ``files`` are written into the image;
        ``run`` is the escape hatch for anything the fields above do not say.

        ``dockerfile`` overrides synthesis entirely: the text is used verbatim
        and every field except ``name``, ``files`` and ``platform`` is ignored,
        though all of them still hash.

        ``env`` values are **baked into the image**, visible to anyone who can
        pull it. A private index is reached by network or by a credential in the
        base image, never by a URL carrying ``user:password@`` — :meth:`validate`
        refuses one.
        """
        self.name = name
        self.base = base
        self.apt: List[str] = list(apt or [])
        self.pip: List[str] = list(pip or [])
        self.env: Dict[str, str] = dict(env or {})
        self.workdir = workdir
        self.files: List[RecipeFile] = [_as_recipe_file(f) for f in (files or [])]
        self.run: List[str] = list(run or [])
        self.user = user
        self.keep_entrypoint = keep_entrypoint
        self.platform = platform
        self.dockerfile = dockerfile

    # ── canonical form ───────────────────────────────────────────────────────

    def to_dict(self) -> Dict[str, Any]:
        """The recipe as the builder serialises it.

        Absent, empty and false fields are **omitted** rather than emitted as
        ``null`` / ``[]`` / ``false``, and ``env`` is sorted by key, because the
        builder's Rust types do both and this dict is what gets hashed. Insertion
        order here is the field order in the canonical JSON.
        """
        d: Dict[str, Any] = {"name": self.name, "base": self.base}
        if self.apt:
            d["apt"] = list(self.apt)
        if self.pip:
            d["pip"] = list(self.pip)
        if self.env:
            d["env"] = {k: self.env[k] for k in sorted(self.env)}
        if self.workdir is not None:
            d["workdir"] = self.workdir
        if self.files:
            d["files"] = [f.to_dict() for f in self.files]
        if self.run:
            d["run"] = list(self.run)
        if self.user is not None:
            d["user"] = self.user
        if self.keep_entrypoint:
            d["keep_entrypoint"] = True
        if self.platform is not None:
            d["platform"] = self.platform
        if self.dockerfile is not None:
            d["dockerfile"] = self.dockerfile
        return d

    def canonical_json(self) -> str:
        """The exact bytes that are hashed.

        Three details are load-bearing, and each one silently changes every tag
        if it is wrong:

        * ``separators`` — no spaces after ``:`` or ``,``.
        * ``ensure_ascii=False`` — non-ASCII goes in as raw UTF-8. Python's
          default would escape ``é`` to ``\\u00e9``; the builder does not.
        * ``sort_keys`` is **off** — fields are in declaration order, which is
          what a Rust struct serialises to. Sorting them would be alphabetical
          and wrong.
        """
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)

    def hash(self) -> str:
        """Lowercase hex SHA-256 over the generator version and the canonical form.

        The version is hashed too, so a change to how the Dockerfile is
        synthesised re-tags every image rather than silently reusing one built
        by the old rules.
        """
        h = hashlib.sha256()
        h.update(BUILD_GENERATOR_VERSION.encode("utf-8"))
        h.update(b"\n")
        h.update(self.canonical_json().encode("utf-8"))
        return h.hexdigest()

    def tag(self) -> str:
        """The content-addressed tag, ``r-<16 hex>``."""
        return "r-" + self.hash()[:16]

    def image_ref(self, repository_prefix: str = "") -> str:
        """``<prefix>/<name>:<tag>``, or ``<name>:<tag>`` with no prefix.

        No prefix means a daemon-local image: built and used on the same Docker
        or Podman socket, never pushed. That is the compose case.
        """
        prefix = repository_prefix.rstrip("/")
        return f"{prefix}/{self.name}:{self.tag()}" if prefix else f"{self.name}:{self.tag()}"

    # ── validation ───────────────────────────────────────────────────────────

    def _strings(self) -> List[tuple]:
        """Every author-supplied string, with something to call it."""
        out: List[tuple] = [("name", self.name or ""), ("base", self.base or "")]
        for label, v in (("workdir", self.workdir), ("user", self.user),
                         ("platform", self.platform), ("dockerfile", self.dockerfile)):
            if v is not None:
                out.append((label, v))
        out += [(f"apt entry {p!r}", p) for p in self.apt]
        out += [(f"pip entry {p!r}", p) for p in self.pip]
        out += [(f"run line {r!r}", r) for r in self.run]
        for k, v in self.env.items():
            out += [(f"env key {k!r}", k), (f"env {k!r}", v)]
        for f in self.files:
            out += [(f"file path {f.path!r}", f.path), (f"the contents of {f.path!r}", f.content)]
        return out

    def validate(self) -> None:
        """Refuse a recipe that could not produce a well-formed image.

        These are the builder's own rules, applied here so a mistake surfaces
        where it was written rather than in a build log twenty minutes later.
        The builder still runs them; being *laxer* here would only mean the
        error arrives late, and being *stricter* would block something legal, so
        this list tracks `Recipe::validate` in `ee/dagron-build/src/recipe.rs`
        rule for rule.
        """
        # A lone surrogate is representable in a Python str and not in UTF-8, so
        # `canonical_json().encode()` raises a UnicodeEncodeError naming a byte
        # offset — true, and useless. Name the field instead.
        for what, value in self._strings():
            if any(0xD800 <= ord(c) <= 0xDFFF for c in value):
                raise ValueError(
                    f"recipe {self.name!r}: {what} contains an unpaired UTF-16 surrogate, "
                    "which cannot be encoded as UTF-8 — the builder cannot parse a recipe "
                    "containing one"
                )
        if (
            not self.name
            or len(self.name) > _MAX_RECIPE_NAME_LEN
            or not _RECIPE_NAME_RE.match(self.name)
        ):
            raise ValueError(
                f"recipe name {self.name!r} must be 1-{_MAX_RECIPE_NAME_LEN} characters of "
                "[a-z0-9._-], starting and ending alphanumeric, with no '/' — the registry "
                "and workspace prefix are added where the build runs"
            )
        if not self.base or _blank(self.base) or _has_ws(self.base):
            raise ValueError(f"recipe {self.name!r}: base must be a single image reference")
        _no_continuation(self.name, "base", self.base)
        for k, v in self.env.items():
            if not _ENV_KEY_RE.match(k):
                raise ValueError(
                    f"recipe {self.name!r}: env key {k!r} must match [A-Za-z_][A-Za-z0-9_]*"
                )
            _no_line_breaks(self.name, "env", v)
            _no_continuation(self.name, "env", v)
            _no_userinfo(self.name, "env", v)
        if self.workdir is not None:
            _absolute_path(self.name, "workdir", self.workdir)
            _no_continuation(self.name, "workdir", self.workdir)
        seen: set[str] = set()
        for f in self.files:
            _absolute_path(self.name, "file", f.path)
            _no_continuation(self.name, "file", f.path)
            if f.path in seen:
                # Two entries for one path is a recipe that cannot say which
                # content it means; the builder refuses rather than pick.
                raise ValueError(f"recipe {self.name!r}: file {f.path!r} is listed twice")
            seen.add(f.path)
        for field, entries in (("apt", self.apt), ("pip", self.pip), ("run", self.run)):
            for line in entries:
                if _blank(line):
                    raise ValueError(f"recipe {self.name!r}: {field} has an empty entry")
                _no_line_breaks(self.name, field, line)
                _no_continuation(self.name, field, line)
                _no_userinfo(self.name, field, line)
                # A backslash in a package specifier is a shell habit that does
                # not mean anything here — `run` is the field for shell.
                if field != "run" and "\\" in line:
                    raise ValueError(
                        f"recipe {self.name!r}: {field} entry {line!r} contains a backslash; "
                        "entries are package specifiers"
                    )
                # An entry beginning with `-` is not a package, it is an OPTION
                # to the command that installs them, and both installers have
                # options that run code or move where packages come from:
                # `apt-get -o DPkg::Pre-Invoke::=<shell>` executes that shell as
                # root during the build, and `pip --index-url <url>` fetches from
                # somewhere else. Quoting does not help — quoting is what makes
                # `-o` a clean separate argument.
                if field != "run" and line.startswith("-"):
                    raise ValueError(
                        f"recipe {self.name!r}: {field} entry {line!r} starts with '-', which is "
                        "an option to the installer rather than a package; use `run` if that is "
                        "what you meant"
                    )
        if self.user is not None and (not self.user or _has_ws(self.user)):
            raise ValueError(f"recipe {self.name!r}: user must be a single uid or name")
        if self.user is not None:
            _no_continuation(self.name, "user", self.user)
        if self.platform is not None and (not self.platform or _has_ws(self.platform)):
            raise ValueError(f"recipe {self.name!r}: platform must look like linux/arm64")

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"Recipe({self.name!r}, base={self.base!r}, tag={self.tag()!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Recipe):
            return NotImplemented
        return self.to_dict() == other.to_dict()


def _as_recipe_file(f: Union[RecipeFile, Mapping[str, Any]]) -> RecipeFile:
    """Accept a :class:`RecipeFile` or the plain mapping a YAML recipe uses."""
    if isinstance(f, RecipeFile):
        return f
    if isinstance(f, Mapping):
        missing = [k for k in ("path", "content") if k not in f]
        if missing:
            raise TypeError(f"recipe file mapping is missing {', '.join(missing)}")
        return RecipeFile(str(f["path"]), str(f["content"]), executable=bool(f.get("executable", False)))
    raise TypeError("files must be RecipeFile objects or {'path', 'content'} mappings")


def _no_line_breaks(recipe: str, field: str, value: str) -> None:
    # The builder refuses a NUL here too: it would truncate the value at the C
    # boundary somewhere downstream, so the image would not contain what the
    # recipe says. Checked with the same three characters it checks.
    if any(c in value for c in ("\n", "\r", "\0")):
        raise ValueError(
            f"recipe {recipe!r}: {field} must not contain a line break or a NUL"
        )


def _no_continuation(recipe: str, field: str, value: str) -> None:
    """A trailing backslash would continue the generated Dockerfile line onto the
    next one, splicing two directives into one.

    Strips trailing whitespace as the builder does — Rust's
    ``char::is_whitespace``, i.e. :data:`_RUST_WHITESPACE` — so a value ending
    ``\\`` followed by a no-break space is a continuation here too.
    """
    if _rstrip_ws(value).endswith("\\"):
        raise ValueError(f"recipe {recipe!r}: {field} must not end with a backslash")



def _no_userinfo(recipe: str, field: str, value: str) -> None:
    """``https://user:password@host`` in an ENV bakes a credential into the image
    and echoes it into the build log."""
    if re.search(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^/\s@]*:[^/\s@]*@", value):
        raise ValueError(
            f"recipe {recipe!r}: {field} carries a 'user:password@' URL — an ENV is baked "
            "into the image and visible to anyone who can pull it"
        )


def _absolute_path(recipe: str, field: str, value: str) -> None:
    if (
        not value.startswith("/")
        or _has_ws(value)
        or "\0" in value
        or any(seg in ("", ".", "..") for seg in value.split("/")[1:])
    ):
        raise ValueError(
            f"recipe {recipe!r}: {field} path {value!r} must be absolute, without "
            "whitespace, `.`/`..` or empty components"
        )


class _TaskSet:
    """The task-list half shared by a :class:`Dag` and each :class:`Template`.

    Both hold an ordered list of tasks under a name, add them through the same
    :meth:`task` signature, and run the same per-task structural checks — a
    template's sub-graph is validated exactly like the top-level graph
    server-side, so sharing the code here is what keeps the two from drifting.
    """

    def __init__(
        self,
        name: str,
        *,
        what: str,
        image_repository: str = "",
        build_runner_class: str = "build",
        build_timeout_secs: int = 900,
    ) -> None:
        if not name:
            raise ValueError(f"{what} requires a name")
        self.name = name
        self._what = what
        self._tasks: List[Dict[str, Any]] = []
        self._names: set = set()
        # Where a `Recipe` image is pushed and pulled from, and the builds
        # already injected for this task set, keyed by image tag. Both live
        # here rather than on `Dag` because `task()` is what triggers a build
        # and `task()` is shared -- a recipe used inside a template builds in
        # that template's namespace, exactly as its other tasks do.
        self.image_repository = image_repository
        self.build_runner_class = build_runner_class
        self.build_timeout_secs = build_timeout_secs
        self._builds: Dict[str, str] = {}

    # ── authoring ─────────────────────────────────────────────────────────────

    def task(
        self,
        name: str,
        *,
        image: Optional[str] = None,
        command: Optional[Sequence[str]] = None,
        depends_on: Optional[Sequence[str]] = None,
        workflow_ref: Optional[str] = None,
        template: Optional[str] = None,
        arguments: Optional[Mapping[str, str]] = None,
        task_type: Optional[str] = None,
        workflow: Optional[str] = None,
        wait: Optional[Mapping[str, Any]] = None,
        approval_timeout_secs: Optional[int] = None,
        approval_on_timeout: Optional[str] = None,
        input: Optional[Any] = None,
        when: Optional[str] = None,
        trigger_rule: Optional[str] = None,
        hook: Optional[str] = None,
        allow_failure: Optional[bool] = None,
        with_items: Optional[Sequence[Any]] = None,
        with_param: Optional[str] = None,
        instance_key: Optional[str] = None,
        max_attempts: Optional[int] = None,
        retry_delay_secs: Optional[int] = None,
        retry_max_delay_secs: Optional[int] = None,
        retry_on_timeout: Optional[bool] = None,
        retry_budgets: Optional[Mapping[str, int]] = None,
        timeout_secs: Optional[int] = None,
        env: Optional[Union[Mapping[str, str], Sequence[Mapping[str, Any]]]] = None,
        resources: Optional[Mapping[str, Any]] = None,
        service_account: Optional[str] = None,
        runner_class: Optional[str] = None,
        pool: Optional[str] = None,
        priority: Optional[int] = None,
        cache: Optional[Mapping[str, Any]] = None,
        repeat: Optional[Mapping[str, Any]] = None,
        produces: Optional[Sequence[str]] = None,
        gang: Optional[Union[int, Mapping[str, Any]]] = None,
        isolation: Optional[Mapping[str, Any]] = None,
    ) -> str:
        """Add a task; returns its name (pass it to a later task's ``depends_on``).

        A task is exactly one **kind**: a *leaf* (runs ``command``), a *call*
        (``template`` inlines a sub-DAG declared on this spec), a *chain*
        (``workflow_ref`` inlines another saved workflow), or one of the
        command-less kinds selected by ``task_type`` — ``approval`` (a human
        gate), ``workflow`` (trigger a registered workflow as a child run) or
        ``wait`` (a deferrable sensor). :meth:`Dag.to_spec` enforces that at
        build time, mirroring the server.

        The remaining keyword arguments map one-to-one onto the engine's
        ``TaskSpec`` and are omitted from the emitted spec when left unset so the
        JSON stays minimal:

        ``input``/``env``/``resources``/``service_account``
            what the task runs with. ``env`` takes a ``{name: value}`` mapping or
            a list of ``{"name", "value"}`` / ``{"name", "value_from"}`` entries,
            the latter resolving a secret at dispatch.
        ``when``/``trigger_rule``/``hook``/``allow_failure``
            when the task runs and what its failure means. A ``when`` that reads
            ``{{ tasks.<dep>.output }}`` must also depend on ``<dep>``.
        ``with_items``/``with_param``/``instance_key``
            fan-out: one instance per item, named from ``instance_key``.
        ``max_attempts``/``retry_delay_secs``/``retry_max_delay_secs``/
        ``retry_on_timeout``/``retry_budgets``/``timeout_secs``
            the retry policy, including per-fault-class attempt budgets.
        ``runner_class``/``pool``/``priority``/``gang``
            where and in what order it is claimed. ``gang`` accepts a member
            count or the full ``{"size": n}`` mapping.
        ``cache``/``repeat``/``produces``/``isolation``
            result memoization, the loop operator, the datasets it updates, and
            the trust envelope it runs under.
        """
        if not name:
            raise ValueError("task requires a name")
        # str/bytes are sequences, so list("echo") would silently split into
        # characters — reject the scalar instead of building a broken spec.
        if isinstance(command, (str, bytes)):
            raise TypeError("command must be a sequence of strings, not a single string")
        if isinstance(depends_on, (str, bytes)):
            raise TypeError("depends_on must be a sequence of task names, not a single string")
        if isinstance(produces, (str, bytes)):
            raise TypeError("produces must be a sequence of dataset URIs, not a single string")
        # A Recipe is validated BEFORE the name is reserved, so a bad recipe
        # leaves the task set untouched rather than half-mutated.
        if isinstance(image, Recipe):
            image.validate()
        if name in self._names:
            raise ValueError(f"duplicate task '{name}'")
        # Reserve the author's name BEFORE injecting the build. The other way
        # round, `task("build-etl", image=Recipe("etl", ...))` has the injected
        # task claim `build-etl` and the author's own call then fails as a
        # duplicate -- the author's name must win.
        self._names.add(name)

        if isinstance(image, Recipe):
            depends_on = list(depends_on or [])
            build_name = self._ensure_build(image)
            if build_name not in depends_on:
                depends_on.append(build_name)
            image = image.image_ref(self.image_repository)

        t: Dict[str, Any] = {"name": name}
        if image:
            t["docker_image"] = image
        if command:
            t["command"] = list(command)
        if depends_on:
            t["depends_on"] = list(depends_on)
        if workflow_ref:
            t["workflow_ref"] = workflow_ref
        if template:
            t["template"] = template
        if arguments:
            t["arguments"] = {str(k): str(v) for k, v in arguments.items()}
        if task_type:
            # The engine's field is `type`; `task_type` is only the Python
            # spelling, since `type` is a builtin.
            t["type"] = task_type
        if workflow:
            t["workflow"] = workflow
        if wait is not None:
            t["wait"] = _normalize_wait(wait)
        if approval_timeout_secs is not None:
            t["approval_timeout_secs"] = approval_timeout_secs
        if approval_on_timeout is not None:
            if approval_on_timeout not in ("approve", "reject"):
                raise ValueError("approval_on_timeout must be 'approve' or 'reject'")
            t["approval_on_timeout"] = approval_on_timeout
        if input is not None:
            t["input"] = input
        if when is not None:
            t["when"] = when
        if trigger_rule is not None:
            t["trigger_rule"] = trigger_rule
        if hook is not None:
            if hook not in ("on_exit", "on_failure"):
                raise ValueError("hook must be 'on_exit' or 'on_failure'")
            t["hook"] = hook
        if allow_failure:
            t["allow_failure"] = True
        if with_items is not None:
            t["with_items"] = list(with_items)
        if with_param is not None:
            t["with_param"] = with_param
        if instance_key is not None:
            t["instance_key"] = instance_key
        if max_attempts is not None:
            if max_attempts < 1:
                raise ValueError("max_attempts must be >= 1")
            t["max_attempts"] = max_attempts
        if retry_delay_secs is not None:
            t["retry_delay_secs"] = retry_delay_secs
        if retry_max_delay_secs is not None:
            t["retry_max_delay_secs"] = retry_max_delay_secs
        if retry_on_timeout is not None:
            t["retry_on_timeout"] = bool(retry_on_timeout)
        if retry_budgets:
            t["retry_budgets"] = {str(k): int(v) for k, v in retry_budgets.items()}
        if timeout_secs is not None:
            t["timeout_secs"] = timeout_secs
        if env is not None:
            t["env"] = _normalize_env(env)
        if resources is not None:
            t["resources"] = dict(resources)
        if service_account:
            t["service_account"] = service_account
        if runner_class:
            t["runner_class"] = runner_class
        if pool:
            t["pool"] = pool
        if priority:
            # 0 is the engine's default and means "fall back to task_defaults";
            # emitting it would pin the task to 0 and defeat that.
            t["priority"] = priority
        if cache is not None:
            t["cache"] = dict(cache)
        if repeat is not None:
            unknown = set(repeat) - REPEAT_KEYS
            if unknown:
                raise TypeError(
                    f"unknown repeat key(s): {', '.join(sorted(unknown))}; "
                    f"expected any of {', '.join(sorted(REPEAT_KEYS))}"
                )
            t["repeat"] = dict(repeat)
        if produces:
            t["produces"] = list(produces)
        if gang is not None:
            t["gang"] = {"size": int(gang)} if isinstance(gang, int) else dict(gang)
        if isolation is not None:
            t["isolation"] = dict(isolation)

        self._tasks.append(t)
        return name

    def approval(
        self,
        name: str,
        *,
        timeout_secs: Optional[int] = None,
        on_timeout: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        """Add a human approval gate (``type: approval``).

        The task parks in ``awaiting_approval`` when its dependencies are
        satisfied and waits for :meth:`Client.approve_task` /
        :meth:`Client.reject_task` — or, if ``timeout_secs`` is set, for the
        deadline to resolve it as ``on_timeout`` (``"reject"`` by default: absent
        a human decision, a gate fails safe).
        """
        return self.task(
            name,
            task_type="approval",
            approval_timeout_secs=timeout_secs,
            approval_on_timeout=on_timeout,
            **kwargs,
        )

    def sensor(
        self,
        name: str,
        *,
        duration: Optional[str] = None,
        until: Optional[str] = None,
        url: Optional[str] = None,
        dataset: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        """Add a deferrable sensor (``type: wait``) — it holds no worker slot.

        Exactly one of ``duration`` (a relative span like ``"5m"``, anchored when
        the task is reached), ``until`` (an absolute RFC3339 instant), ``url``
        (poll until it answers 2xx) or ``dataset`` (wait for a *fresh* update to
        that dataset).
        """
        spec = {"for": duration, "until": until, "url": url, "dataset": dataset}
        return self.task(name, task_type="wait", wait=spec, **kwargs)

    def trigger(
        self,
        name: str,
        workflow: str,
        *,
        arguments: Optional[Mapping[str, str]] = None,
        **kwargs: Any,
    ) -> str:
        """Add a sub-workflow trigger (``type: workflow``).

        The engine submits the named **registered** workflow as a child run and
        parks this task until that run is terminal, succeeding or failing with
        it. ``arguments`` become the child run's parameters, so a repeating
        trigger can hand each child different inputs.
        """
        return self.task(
            name, task_type="workflow", workflow=workflow, arguments=arguments, **kwargs
        )

    # ── validation ────────────────────────────────────────────────────────────

    def _validate(self, template_names: Iterable[str]) -> None:
        """Run the server's per-task and graph checks over this task set.

        Mirrors ``routes::control::validate_graph``, and deliberately stops where
        it does: a dependency that only resolves after expansion (a name inside a
        chained sub-workflow) is left for the engine, so a spec the server would
        accept is never rejected here.
        """
        declared = set(template_names)
        for t in self._tasks:
            name = t["name"]
            rule = t.get("trigger_rule")
            if rule is not None and rule not in TRIGGER_RULES:
                raise ValueError(
                    f"task '{name}' has invalid trigger_rule '{rule}' "
                    f"(expected one of {sorted(TRIGGER_RULES)})"
                )
            repeat = t.get("repeat")
            if repeat is not None:
                if not str(repeat.get("until", "")).strip():
                    raise ValueError(f"task '{name}' repeat.until is empty")
                if int(repeat.get("max_iterations", 0)) < 1:
                    raise ValueError(f"task '{name}' repeat.max_iterations must be >= 1")
                if t.get("type") not in (None, "task", "workflow"):
                    raise ValueError(
                        f"task '{name}' cannot combine `repeat` with `type: {t['type']}` — "
                        "`repeat` applies to command tasks and sub-workflow triggers"
                    )
            klass = t.get("runner_class")
            # A templated class (`{{ param }}`) is only a real name after the
            # server substitutes it, so checking its charset here would reject a
            # spec the engine accepts.
            if klass and "{{" not in klass:
                _validate_runner_class(klass, f"task '{name}'")
            for referenced in _when_output_refs(t.get("when") or ""):
                if referenced not in t.get("depends_on", []):
                    raise ValueError(
                        f"task '{name}' when references "
                        f"'{{{{ tasks.{referenced}.output }}}}' but does not depend on "
                        f"'{referenced}' — add it to depends_on"
                    )
            kinds = [bool(t.get("command")), "template" in t, "workflow_ref" in t]
            if t.get("type") in COMMANDLESS_TASK_TYPES:
                if any(kinds):
                    raise ValueError(
                        f"task '{name}' is a command-less kind (approval / workflow / wait) "
                        "and cannot set `command`, `template` or `workflow_ref`"
                    )
            elif kinds.count(True) == 0:
                raise ValueError(
                    f"task '{name}' needs a `command` (leaf), a `template` (sub-DAG call) "
                    "or a `workflow_ref` (chain)"
                )
            elif kinds.count(True) > 1:
                raise ValueError(
                    f"task '{name}' sets more than one of `command` / `template` / "
                    "`workflow_ref` — use exactly one"
                )
            if t.get("type") == "workflow":
                if not str(t.get("workflow", "")).strip():
                    raise ValueError(
                        f"task '{name}' is type: workflow but names no `workflow:` to trigger"
                    )
            elif "workflow" in t:
                raise ValueError(f"task '{name}' sets `workflow:` but is not `type: workflow`")
            if t.get("type") == "wait":
                set_keys = [k for k, v in t.get("wait", {}).items() if v is not None]
                if len(set_keys) != 1:
                    raise ValueError(
                        f"task '{name}' is type: wait and needs exactly one of "
                        "`for` / `until` / `url` / `dataset`"
                    )
                if "hook" in t:
                    raise ValueError(f"task '{name}' cannot be both a wait sensor and a hook")
            elif "wait" in t:
                raise ValueError(f"task '{name}' sets `wait:` but is not `type: wait`")
            called = t.get("template")
            if called is not None:
                if called not in declared:
                    raise ValueError(
                        f"task '{name}' calls unknown template '{called}' in "
                        f"{self._what} '{self.name}' — declare it with Dag.template()"
                    )
            elif t.get("arguments") and t.get("type") != "workflow":
                raise ValueError(
                    f"task '{name}' sets `arguments` with no `template` or "
                    "`type: workflow` to pass them to"
                )

        # Names a dependency may legitimately forward-reference: a task inside a
        # chained sub-workflow, namespaced only once the chain is inlined.
        chains = [t["name"] for t in self._tasks if "workflow_ref" in t]
        for t in self._tasks:
            for dep in t.get("depends_on", []):
                if dep in self._names:
                    continue
                if any(dep == c or dep.startswith(f"{c}.") for c in chains):
                    continue
                raise ValueError(f"task '{t['name']}' depends on unknown task '{dep}'")
        self._assert_acyclic()

    def _assert_acyclic(self) -> None:
        """DFS colouring; raise on the first back-edge (a dependency cycle)."""
        adjacency: Dict[str, List[str]] = {
            t["name"]: [d for d in t.get("depends_on", []) if d in self._names]
            for t in self._tasks
        }
        WHITE, GREY, BLACK = 0, 1, 2
        color: Dict[str, int] = {name: WHITE for name in adjacency}

        def visit(node: str) -> None:
            """Depth-first visit; a GREY neighbour is a back-edge, i.e. a cycle."""
            color[node] = GREY
            for dep in adjacency[node]:
                if color[dep] == GREY:
                    raise ValueError(
                        f"{self._what} '{self.name}' contains a cycle (through '{dep}')"
                    )
                if color[dep] == WHITE:
                    visit(dep)
            color[node] = BLACK

        for name in adjacency:
            if color[name] == WHITE:
                visit(name)

    def _ensure_build(self, recipe: "Recipe") -> str:
        """Add the build task for ``recipe`` once, and return its name.

        Keyed on the tag rather than the recipe's name: two recipes with the
        same name are two different images and need two builds, and the same
        recipe used by ten tasks needs one.
        """
        recipe.validate()
        tag = recipe.tag()
        existing = self._builds.get(tag)
        if existing is not None:
            return existing

        base = f"build-{recipe.name}"
        # A second recipe named the same thing, or an author's own task called
        # `build-x`, must not collide. The tag disambiguates and stays stable.
        build_name = base if base not in self._names else f"{base}-{tag[2:10]}"
        if build_name in self._names:
            raise ValueError(
                f"cannot add a build task for recipe {recipe.name!r}: both "
                f"'{base}' and '{build_name}' are taken"
            )

        # The recipe travels as canonical JSON, not as the YAML someone typed.
        # JSON is a YAML subset so the builder parses it either way, and the
        # canonical form has no whitespace left to disagree about — which is how
        # the tag computed here and the tag the builder computes stay the same
        # string. An earlier version of this feature embedded the YAML verbatim
        # and a stripped trailing newline moved the hash.
        build_env: List[Dict[str, str]] = [
            {"name": "DAGRON_BUILD_RECIPE", "value": recipe.canonical_json()}
        ]
        if self.image_repository:
            # Pin both halves in the spec. The pool has its own defaults for
            # these; a task's env wins over them, so the image this build pushes
            # is the image the tasks reference even against a pool configured
            # for a different registry.
            build_env.append({"name": "DAGRON_IMAGE_REPOSITORY", "value": self.image_repository})
            build_env.append({"name": "DAGRON_BUILD_PUSH", "value": "1"})

        image_ref = recipe.image_ref(self.image_repository)
        self._names.add(build_name)
        self._builds[tag] = build_name
        self._tasks.append({
            "name": build_name,
            "command": ["dagron-build"],
            "runner_class": self.build_runner_class,
            "env": build_env,
            "timeout_secs": self.build_timeout_secs,
            # Declarative lineage: the thing this task makes. Deliberately no
            # `cache:` — the engine's memo is keyed on the cache key alone and
            # never looks at a registry, so a memo outliving a pruned image
            # would skip the build and leave every task pulling a tag that is
            # no longer there. The builder's own registry lookup is the reuse
            # check that cannot go stale.
            "produces": [f"oci://{image_ref}"],
        })
        return build_name


class Template(_TaskSet):
    """A named, reusable sub-DAG declared on a :class:`Dag` and called by a task.

    Create one with :meth:`Dag.template`, fill it with :meth:`_TaskSet.task`, and
    call it from a task with ``template="<name>"`` plus ``arguments``. Its tasks
    live in their own namespace — ``depends_on`` inside a template names the
    template's own tasks, and the expander prefixes each produced task with the
    calling task's name (``run-etl.build``).
    """

    def __init__(
        self,
        name: str,
        *,
        parameters: Optional[Mapping[str, str]] = None,
        image_repository: str = "",
        build_runner_class: str = "build",
        build_timeout_secs: int = 900,
    ) -> None:
        """Declare a template named ``name`` with optional default ``parameters``.

        The three build settings are not for callers to pass: :meth:`Dag.template`
        forwards the DAG's own, so a ``Recipe`` used inside a template resolves to
        the same image reference, and pushes to the same registry, as one used
        directly on the DAG. Left at their defaults a template would build
        ``etl:r-<tag>`` with no registry prefix and never push it — one spec
        carrying two references for one recipe, the template's pointing at a
        daemon-local image a remote runner cannot pull.
        """
        super().__init__(
            name,
            what="template",
            image_repository=image_repository,
            build_runner_class=build_runner_class,
            build_timeout_secs=build_timeout_secs,
        )
        self.parameters: Dict[str, str] = (
            {str(k): str(v) for k, v in parameters.items()} if parameters else {}
        )

    def to_spec(self) -> Dict[str, Any]:
        """The template as it appears under a spec's ``templates:`` list.

        Deep-copied like :meth:`Dag.to_spec`: called through the DAG the outer
        copy would cover it, but a caller who builds a template and reads it
        directly would otherwise be handed the builder's own task list to mutate.
        """
        spec: Dict[str, Any] = {"name": self.name}
        if self.parameters:
            spec["parameters"] = dict(self.parameters)
        spec["tasks"] = copy.deepcopy(self._tasks)
        return spec


class Dag(_TaskSet):
    """Build a dagron workflow spec in code.

    Add tasks with :meth:`_TaskSet.task` (or the :meth:`_TaskSet.approval` /
    :meth:`_TaskSet.sensor` / :meth:`_TaskSet.trigger` shorthands), reusable
    sub-DAGs with :meth:`template`; ``to_spec()`` / ``to_json()`` validate the
    graph and emit the spec. Pass the builder straight to
    :meth:`Client.submit_run` or :meth:`Client.create_workflow`.
    """

    def __init__(
        self,
        name: str,
        *,
        runner_class: Optional[str] = None,
        image_repository: str = "",
        build_runner_class: str = "build",
        build_timeout_secs: int = 900,
        parameters: Optional[Mapping[str, str]] = None,
        tags: Optional[Sequence[str]] = None,
        environment: Optional[str] = None,
        task_defaults: Optional[Mapping[str, Any]] = None,
        run_timeout_secs: Optional[int] = None,
        max_active_runs: Optional[int] = None,
        result_from: Optional[str] = None,
        budget: Optional[Mapping[str, Any]] = None,
        deadline: Optional[Mapping[str, Any]] = None,
        notify: Optional[Mapping[str, Any]] = None,
        on_datasets: Optional[Sequence[str]] = None,
        datasets_mode: Optional[str] = None,
    ) -> None:
        """Create an empty DAG named ``name`` (raises if the name is empty).

        Every other argument is a spec-level property, all optional:

        ``runner_class``
            the default runner class — every task that doesn't set its own routes
            to that pool of engine replicas (e.g. ``"etl"``, ``"ml_training"``).
        ``parameters``
            declared parameters and their defaults; ``{{ name }}`` references in
            commands, images, env values and ``when`` conditions are substituted
            at submit, and a caller overrides them via ``submit_run(parameters=)``.
        ``tags``
            labels for the console's workflow list (``list_workflows(tag=…)``).
        ``environment``
            the named variable set + secrets this spec runs under. Its variables
            join the substitution scope as ``{{ env.NAME }}`` and win over any
            caller parameter of the same name.
        ``task_defaults``
            the DRY block — ``max_attempts``, ``retry_delay_secs``,
            ``retry_max_delay_secs``, ``timeout_secs``, ``docker_image``,
            ``runner_class``, ``env``, ``pool``, ``priority`` — merged into every
            task that doesn't set its own.
        ``run_timeout_secs``/``max_active_runs``
            the run's hard wall-clock budget, and how many of its runs may be in
            flight at once.
        ``budget``
            a ceiling on what one run may expand to — ``{"tasks": N}`` — so a
            fan-out over a bad parameter is refused at submit, not discovered.
        ``deadline``
            the *soft* deadline — ``{"within": "2h"}`` — whose breach notifies
            rather than kills.
        ``result_from``
            the task whose output becomes the run's result, returned by
            :meth:`Client.wait_run`. Must name a real task.
        ``notify``
            per-workflow notification routing, overriding the instance defaults:
            ``{"slack": {"webhook_url": …, "on": [...]}, "webhook": {…}, "git": {…}}``.
        ``on_datasets``/``datasets_mode``
            data-aware scheduling: trigger this workflow when those datasets
            update (``datasets_mode`` picks ``all`` or ``any``).
        """
        super().__init__(
            name,
            what="DAG",
            image_repository=image_repository,
            build_runner_class=build_runner_class,
            build_timeout_secs=build_timeout_secs,
        )
        self.runner_class = runner_class
        self.parameters: Dict[str, str] = (
            {str(k): str(v) for k, v in parameters.items()} if parameters else {}
        )
        self.tags: List[str] = list(tags) if tags else []
        self.environment = environment
        self.task_defaults = dict(task_defaults) if task_defaults else None
        self.run_timeout_secs = run_timeout_secs
        self.max_active_runs = max_active_runs
        self.result_from = result_from
        self.budget = dict(budget) if budget else None
        self.deadline = dict(deadline) if deadline else None
        self.notify = dict(notify) if notify else None
        self.on_datasets: List[str] = list(on_datasets) if on_datasets else []
        self.datasets_mode = datasets_mode
        self._templates: List[Template] = []

    def template(self, name: str, *, parameters: Optional[Mapping[str, str]] = None) -> Template:
        """Declare a reusable sub-DAG and return it for filling with tasks.

        Call it from a task with ``template=<name>`` and ``arguments={…}``; the
        engine inlines the template's tasks in place of the calling task at run
        creation, wiring that task's upstreams to the sub-DAG's roots and its
        downstreams to the sub-DAG's exits.
        """
        if any(t.name == name for t in self._templates):
            raise ValueError(f"duplicate template '{name}'")
        tpl = Template(
            name,
            parameters=parameters,
            image_repository=self.image_repository,
            build_runner_class=self.build_runner_class,
            build_timeout_secs=self.build_timeout_secs,
        )
        self._templates.append(tpl)
        return tpl

    def to_spec(self) -> Dict[str, Any]:
        """Build the validated dagron spec dict.

        Runs the same structural checks the gateway runs server-side, so a bad DAG
        fails locally with a clear message instead of a 400 round-trip: every task
        is exactly one kind, trigger rules and runner classes are well-formed,
        every ``template:`` call and ``depends_on`` resolves, ``result_from``
        names a real task, and the dependency graph is acyclic.
        """
        if self.runner_class and "{{" not in self.runner_class:
            _validate_runner_class(self.runner_class, f"DAG '{self.name}'")
        if self.run_timeout_secs is not None and self.run_timeout_secs < 1:
            raise ValueError(
                f"invalid run_timeout_secs={self.run_timeout_secs} in DAG '{self.name}'; "
                "expected >= 1 (or omit)"
            )
        template_names = [t.name for t in self._templates]
        self._validate(template_names)
        for tpl in self._templates:
            tpl._validate(template_names)
        if self.result_from is not None and self.result_from not in self._names:
            raise ValueError(
                f"result_from '{self.result_from}' in DAG '{self.name}' names no task"
            )

        spec: Dict[str, Any] = {"name": self.name}
        if self.parameters:
            spec["parameters"] = dict(self.parameters)
        if self.tags:
            spec["tags"] = list(self.tags)
        if self.environment:
            spec["environment"] = self.environment
        if self.runner_class:
            spec["runner_class"] = self.runner_class
        if self.task_defaults:
            spec["task_defaults"] = dict(self.task_defaults)
        if self.run_timeout_secs is not None:
            spec["run_timeout_secs"] = self.run_timeout_secs
        if self.max_active_runs is not None:
            spec["max_active_runs"] = self.max_active_runs
        if self.result_from:
            spec["result_from"] = self.result_from
        if self.budget:
            spec["budget"] = dict(self.budget)
        if self.deadline:
            spec["deadline"] = dict(self.deadline)
        if self.notify:
            spec["notify"] = dict(self.notify)
        if self.on_datasets:
            spec["on_datasets"] = list(self.on_datasets)
        if self.datasets_mode:
            spec["datasets_mode"] = self.datasets_mode
        if self._templates:
            spec["templates"] = [t.to_spec() for t in self._templates]
        spec["tasks"] = self._tasks
        # Deep-copy so callers can't mutate our internal task state via the
        # returned spec (to_json/submit both go through here).
        return copy.deepcopy(spec)

    def to_json(self) -> str:
        """dagron spec as JSON (valid dagron input — YAML is a JSON superset)."""
        return json.dumps(self.to_spec())

    def submit(self, api_url: str, token: Optional[str] = None, *, timeout: float = 30) -> str:
        """Submit the DAG as an ad-hoc run; returns the new ``run_id``.

        Convenience one-liner equivalent to ``Client(api_url, token).submit_run(self)``.
        """
        return Client(api_url, token=token, timeout=timeout).submit_run(self)

# ── Client ────────────────────────────────────────────────────────────────────


class DagronError(Exception):
    """A non-2xx response (or transport failure) from dagron-api.

    ``status`` is the HTTP code (``0`` for a transport-level failure), ``message``
    the server's error text (unwrapped from ``{"error": ...}`` when present), and
    ``body`` the raw response body for inspection.
    """

    def __init__(self, status: int, message: str, *, body: Optional[str] = None) -> None:
        """Store the HTTP ``status`` (``0`` for transport failures), ``message``, and raw ``body``."""
        self.status = status
        self.message = message
        self.body = body
        super().__init__(f"dagron-api {status}: {message}" if status else message)

    @classmethod
    def _from_body(cls, status: int, raw: bytes) -> "DagronError":
        """Build an error from a response body, unwrapping ``{"error": ...}`` when present."""
        text = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else (raw or "")
        message = text
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict) and isinstance(parsed.get("error"), str):
                message = parsed["error"]
        except (ValueError, TypeError):
            pass
        message = (message or "").strip() or f"HTTP {status}"
        return cls(status, message, body=text)


class Client:
    """Typed client for the dagron-api gateway (``/api/...``).

    Construct with the gateway base URL (``http://host:port``) and either a session
    JWT (``token=``) or call :meth:`login` to obtain one. Every authed call sends
    ``Authorization: Bearer <token>``; the token can be rotated via :attr:`token`.
    """

    def __init__(self, base_url: str, token: Optional[str] = None, *, timeout: float = 30) -> None:
        """Bind the client to a gateway ``base_url`` (http/https) with an optional session token."""
        # urlopen also speaks file://, ftp://, … — restrict to HTTP(S) so a bad
        # base_url can't leak the bearer token or reach local files (SSRF).
        scheme = urllib.parse.urlparse(base_url).scheme
        if scheme not in ("http", "https"):
            raise ValueError("base_url must use http or https")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    @classmethod
    def from_env(cls, *, timeout: float = 30) -> "Client":
        """Build a client from ``DAGRON_API_URL`` and (optionally) ``DAGRON_TOKEN``.

        The shape automation wants: mint a personal access token once with
        :meth:`create_token`, put it in the environment, and no job ever has to
        store the password that would mint another. Raises :class:`ValueError`
        when ``DAGRON_API_URL`` is unset — an unset URL is a misconfigured job,
        not a reason to guess at localhost.
        """
        url = os.environ.get("DAGRON_API_URL")
        if not url:
            raise ValueError("DAGRON_API_URL is not set")
        return cls(url, token=os.environ.get("DAGRON_TOKEN") or None, timeout=timeout)

    def close(self) -> None:
        """Drop the in-memory token. ``urllib`` holds no connection to release."""
        self.token = None

    def __enter__(self) -> "Client":
        """Support ``with Client(...) as api:`` — the exit drops the token."""
        return self

    def __exit__(self, *exc: Any) -> None:
        """Drop the token on the way out, so it does not outlive the block."""
        self.close()

    # ── auth ──────────────────────────────────────────────────────────────────

    def login(self, email: str, password: str) -> str:
        """Exchange credentials for a session token, store it, and return it.

        After this call the client is authenticated for every other method.
        """
        body = self._request("POST", "/api/login", body={"email": email, "password": password}, auth=False)
        token = body.get("token") if isinstance(body, dict) else None
        if not token:
            raise DagronError(0, "login succeeded but no token was returned")
        self.token = token
        return token

    def logout(self) -> None:
        """Clear the session cookie server-side and drop the local token.

        Sends the bearer token so the call is authenticated and consistent with
        the rest of the client. dagron's session JWT is stateless, so logout is a
        cookie clear today (no server-side denylist to revoke against); dropping
        the local token is what ends the bearer session for this client.
        """
        self._request("POST", "/api/logout", parse_json=False)
        self.token = None

    def me(self) -> Dict[str, Any]:
        """Return the authenticated session's claims (``sub``/``email``/``groups``/…)."""
        return self._request("GET", "/api/me")

    def create_user(
        self, email: str, password: str, name: str, groups: Optional[Sequence[str]] = None
    ) -> Dict[str, Any]:
        """Create a user (caller must be in the ``admin`` group). Returns ``{"id": ...}``."""
        return self._request(
            "POST",
            "/api/users",
            body={"email": email, "password": password, "name": name, "groups": list(groups or [])},
        )

    def list_users(self) -> List[Dict[str, Any]]:
        """List users (admin only). Password hashes are never returned."""
        return self._request("GET", "/api/users")

    # ── personal access tokens ────────────────────────────────────────────────

    def list_tokens(self) -> List[Dict[str, Any]]:
        """List the calling user's access tokens, revoked ones included.

        Each row carries the cleartext ``prefix`` (never the secret), plus
        ``last_used_at`` — the field that answers "is anything still using this".
        """
        return self._request("GET", "/api/tokens")

    def create_token(self, name: str, *, expires_in_days: Optional[int] = None) -> Dict[str, Any]:
        """Mint a named access token; returns it including the plaintext ``token``.

        **This is the only response that ever carries the secret** — only its hash
        is stored, so there is no endpoint that can show it again. Minting
        requires a password session (:meth:`login`): a token cannot mint another,
        which is what keeps a leaked one from outrunning revocation.
        """
        body: Dict[str, Any] = {"name": name}
        if expires_in_days is not None:
            body["expires_in_days"] = expires_in_days
        return self._request("POST", "/api/tokens", body=body)

    def revoke_token(self, token_id: str) -> None:
        """Revoke one access token. Revoking twice is not an error."""
        self._request("DELETE", f"/api/tokens/{_seg(token_id)}", parse_json=False)

    # ── runs ──────────────────────────────────────────────────────────────────

    def submit_run(
        self,
        spec: SpecLike,
        *,
        parameters: Optional[Mapping[str, str]] = None,
        idempotency_key: Optional[str] = None,
    ) -> str:
        """Submit a DAG as an ad-hoc run; returns the new ``run_id``.

        ``spec`` may be a :class:`Dag`, a spec mapping, or a YAML/JSON string.

        ``parameters`` supply arguments for the spec's declared ``parameters:``;
        keys the spec never references are ignored, and a declared
        ``environment:`` still wins over any key it also sets.

        ``idempotency_key`` makes the submit safe to retry: repeating the same
        call with the same key returns the **same** ``run_id`` instead of
        creating a second run. Reusing a key for a *different* spec or different
        parameters raises :class:`DagronError` with status 409 rather than
        quietly handing back the first run — the wrong answer would be a run id
        for work that never ran.
        """
        body: Dict[str, Any] = {"yaml": _spec_to_str(spec)}
        if parameters:
            body["parameters"] = dict(parameters)
        headers = None
        if idempotency_key is not None:
            # An empty key is a 400 server-side. Dropping it silently — the old
            # ``if idempotency_key`` did, since ``""`` is falsy — would hand back
            # a non-idempotent submit the caller believes is retry-safe, which is
            # the exact failure the key exists to remove. Reject it here, loudly.
            if not idempotency_key.strip():
                raise ValueError("idempotency_key must not be empty")
            headers = {"idempotency-key": idempotency_key}
        resp = self._request("POST", "/api/runs", body=body, headers=headers)
        return resp["run_id"]

    def list_runs(
        self,
        *,
        status: Optional[str] = None,
        name: Optional[str] = None,
        trigger: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """List runs newest-first, optionally filtered and paged.

        ``name`` is the workflow/DAG name (exact match) and ``trigger`` is what
        started the run — ``manual``, ``schedule`` or ``backfill``.
        """
        return self._request(
            "GET",
            "/api/runs",
            params={
                "status": status,
                "name": name,
                "trigger": trigger,
                "limit": limit,
                "offset": offset,
            },
        )

    def iter_runs(self, *, page_size: int = 100, **filters: Any) -> Iterator[Dict[str, Any]]:
        """Yield runs across pages, walking ``limit``/``offset`` transparently.

        Takes the same filters as :meth:`list_runs`, minus the two it drives
        itself: passing ``limit`` or ``offset`` would collide with the paging
        arguments and raise an opaque ``TypeError`` from deep inside the loop, so
        they are refused up front with the name of the argument to use instead.
        Stops on the first short page, so a caller can ``break`` out early
        without fetching the rest.
        """
        for reserved in ("limit", "offset"):
            if reserved in filters:
                raise TypeError(
                    f"iter_runs drives '{reserved}' itself — use page_size to size the pages"
                )
        offset = 0
        while True:
            page = self.list_runs(limit=page_size, offset=offset, **filters)
            for run in page:
                yield run
            if len(page) < page_size:
                return
            offset += page_size

    def get_run(self, run_id: str) -> Dict[str, Any]:
        """Fetch one run plus its task rows."""
        return self._request("GET", f"/api/runs/{_seg(run_id)}")

    def get_run_graph(self, run_id: str) -> Dict[str, Any]:
        """Fetch the run's task nodes + dependency edges (for graph rendering)."""
        return self._request("GET", f"/api/runs/{_seg(run_id)}/graph")

    def get_run_spec(self, run_id: str) -> Dict[str, Any]:
        """Fetch the DAG spec this run was created from (``{"yaml", "name"}``).

        The stored, un-expanded spec — what a human authored, chains unresolved —
        so a "re-run with changes" flow can start from the real definition rather
        than reconstructing one.
        """
        return self._request("GET", f"/api/runs/{_seg(run_id)}/spec")

    def get_task_logs(
        self,
        run_id: str,
        task_id: str,
        *,
        offset: Optional[int] = None,
        **log_filter: Any,
    ) -> Dict[str, Any]:
        """Fetch one task's captured output, scoped to its run.

        ``offset`` (a prior response's ``next_offset``) tails: only output past
        that character offset comes back, until ``eof``.

        ``log_filter`` accepts the log filter grammar — see :func:`log_filter`
        — and is applied server-side *within* the tailed slice, so a filtered
        tail appends only matching new lines while ``next_offset`` keeps
        advancing over the raw text.
        """
        params = log_filter_params(**log_filter)
        if offset is not None:
            params["offset"] = offset
        return self._request(
            "GET", f"/api/runs/{_seg(run_id)}/tasks/{_seg(task_id)}/logs", params=params
        )

    def get_run_logs(
        self,
        run_id: str,
        *,
        tasks: Optional[Sequence[str]] = None,
        statuses: Optional[Sequence[str]] = None,
        **log_filter: Any,
    ) -> Dict[str, Any]:
        """Fetch the whole run's output as one attributed, filtered stream.

        This is the call for "something in this run failed and I don't know
        which task" — one request instead of one per task. ``tasks``/``statuses``
        choose which task output is read at all; the filter then chooses which of
        their lines survive.

        Returns ``{"tasks": [...], "lines": [...], "total", "matched",
        "truncated", "eof", "filtered", "limit"}`` — ``total`` and ``matched``
        are counted before the line cap, so a truncated view always says so.

            api.get_run_logs(run_id, level="error", context=2)
            api.get_run_logs(run_id, tasks=["extract"], regex=r"rows=\\d+")
        """
        params = log_filter_params(**log_filter)
        if tasks:
            params["task"] = ",".join(tasks)
        if statuses:
            params["status"] = ",".join(statuses)
        return self._request("GET", f"/api/runs/{_seg(run_id)}/logs", params=params)

    def cancel_run(self, run_id: str) -> int:
        """Cancel a run; returns the number of tasks flipped to ``cancelled``."""
        return self._request("POST", f"/api/runs/{_seg(run_id)}/cancel")["cancelled"]

    def rerun_run(self, run_id: str, *, params: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        """Cascade-rerun a failed/cancelled run from its failure frontier.

        Succeeded tasks are kept; failed/cancelled tasks (and what they blocked)
        reset and re-run. Optional ``params`` is deep-merged into each reset task's
        input for a fix-forward rerun. Returns ``{"run_id", "rerun": <tasks reset>}``.
        """
        body: Dict[str, Any] = {"params": dict(params)} if params else {}
        return self._request("POST", f"/api/runs/{_seg(run_id)}/rerun", body=body)

    def resubmit_run(self, run_id: str) -> str:
        """Start a brand-new run from this run's stored definition; returns the new ``run_id``."""
        return self._request("POST", f"/api/runs/{_seg(run_id)}/resubmit")["run_id"]

    def retry_task(self, run_id: str, task_id: str) -> bool:
        """Resurrect a single failed/cancelled task within a run."""
        return self._request("POST", f"/api/runs/{_seg(run_id)}/tasks/{_seg(task_id)}/retry")["retried"]

    def clear_task(self, run_id: str, task_id: str) -> Dict[str, Any]:
        """Clear a task **and everything downstream of it**, then re-arm the run.

        Unlike :meth:`retry_task` (which resurrects one task), this resets the
        task and its dependents to ``pending`` and recomputes their dependency
        counts, so a fix applied mid-run re-runs the whole affected subtree.
        Returns ``{"run_id", "task_id", "cleared": <tasks reset>}``.
        """
        return self._request(
            "POST", f"/api/runs/{_seg(run_id)}/tasks/{_seg(task_id)}/clear"
        )

    def approve_task(self, run_id: str, task_id: str) -> Dict[str, Any]:
        """Approve a ``type: approval`` gate: the task succeeds and its dependents
        advance. Returns ``{"run_id", "task_id", "resolution"}``. Raises
        :class:`DagronError` with status 409 if the task is not awaiting approval.
        """
        return self._request(
            "POST", f"/api/runs/{_seg(run_id)}/tasks/{_seg(task_id)}/approve"
        )

    def reject_task(self, run_id: str, task_id: str) -> Dict[str, Any]:
        """Reject a ``type: approval`` gate: the task fails and its ``all_success``
        dependents skip. Same return/errors as :meth:`approve_task`.
        """
        return self._request(
            "POST", f"/api/runs/{_seg(run_id)}/tasks/{_seg(task_id)}/reject"
        )

    def stream_run(self, run_id: str, *, timeout: Optional[float] = None) -> Iterator[Dict[str, Any]]:
        """Yield live task-state events for a run as Server-Sent Events.

        Each item is ``{"event": <name>, "data": <parsed JSON or raw str>}``. The
        generator runs until the connection closes; pass ``timeout`` to bound an
        idle read. A ``resync`` event means the client fell behind and should
        refetch the full graph via :meth:`get_run_graph`.
        """
        yield from self._stream(f"/api/runs/{_seg(run_id)}/stream", timeout=timeout)

    def stream_events(self, *, timeout: Optional[float] = None) -> Iterator[Dict[str, Any]]:
        """Yield task-state events across **all** runs as Server-Sent Events.

        The account-wide feed behind the console's live mode: each event carries
        the run it belongs to, so one connection replaces polling every list. Same
        item shape as :meth:`stream_run`.
        """
        yield from self._stream("/api/events/stream", timeout=timeout)

    def _stream(self, path: str, *, timeout: Optional[float] = None) -> Iterator[Dict[str, Any]]:
        """Open one SSE connection and yield its parsed events until it closes."""
        url = self.base_url + path
        headers = {"accept": "text/event-stream"}
        if self.token:
            headers["authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(url, headers=headers, method="GET")
        # Normalise connection/HTTP failures to DagronError, same as _request, so
        # callers see one exception type across the whole client API.
        try:
            resp = urllib.request.urlopen(req, timeout=timeout)  # noqa: S310 (scheme checked in __init__)
        except urllib.error.HTTPError as e:
            raise DagronError._from_body(e.code, e.read()) from None
        except urllib.error.URLError as e:
            raise DagronError(0, f"request to {url} failed: {e.reason}") from None
        try:
            yield from _parse_sse(resp)
        finally:
            resp.close()

    def wait_run(self, run_id: str, *, timeout_secs: Optional[int] = None) -> Dict[str, Any]:
        """Long-poll the run server-side until it is terminal; return the result.

        This is synchronous invocation: one request that blocks on the engine's
        own event feed rather than a poll loop, so there is no polling interval to
        tune and no wasted round trips. Returns ``{"run_id", "status",
        "finished", "result", "failure"}`` — ``result`` is the ``result_from``
        task's output on success, and ``failure`` explains the failure without a
        second call. A wait that times out returns ``finished: false`` with the
        live status, so the caller simply calls again.

        ``timeout_secs`` is the *server's* budget (clamped to 1-600, default 30).
        The transport timeout for this one call is widened to cover it — without
        that, the default 30 s client timeout races the default 30 s server wait
        and aborts the request just as the answer arrives. Only this call is
        widened: :attr:`timeout` is shared with every concurrent request and is
        never mutated.
        """
        budget = min(
            max(
                WAIT_BUDGET_DEFAULT_SECS if timeout_secs is None else timeout_secs,
                WAIT_BUDGET_MIN_SECS,
            ),
            WAIT_BUDGET_MAX_SECS,
        )
        return self._request(
            "GET",
            f"/api/runs/{_seg(run_id)}/wait",
            params={"timeout_secs": timeout_secs},
            timeout=max(self.timeout, budget + WAIT_TRANSPORT_MARGIN_SECS),
        )

    def wait_for_run(
        self, run_id: str, *, poll_interval: float = 2.0, timeout: Optional[float] = 300.0
    ) -> Dict[str, Any]:
        """Poll :meth:`get_run` until the run reaches a terminal state; return it.

        Returns the full run detail (tasks included), which is what makes it worth
        keeping next to :meth:`wait_run`: that one blocks server-side and answers
        with the run's *result*, this one answers with its *contents*. Raises
        :class:`TimeoutError` if ``timeout`` seconds elapse first (``None`` waits
        forever).
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            run = self.get_run(run_id)
            if run.get("status") in TERMINAL_RUN_STATUSES:
                return run
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"run '{run_id}' did not finish within {timeout}s")
            time.sleep(poll_interval)

    # ── triage (what a human decided about a failure) ─────────────────────────

    def set_triage(self, run_id: str, state: str, *, note: Optional[str] = None) -> Dict[str, Any]:
        """Record what a person concluded about a run: ``acknowledged``,
        ``investigating`` or ``resolved``, with an optional ``note``.

        ``status`` is what the engine did; this is what was done about it — the
        distinction a single "mark as read" flag would lose. Re-triaging
        overwrites, because acknowledged-then-resolved is the normal path.
        """
        body: Dict[str, Any] = {"state": state}
        if note is not None:
            body["note"] = note
        return self._request("POST", f"/api/runs/{_seg(run_id)}/triage", body=body)

    def clear_triage(self, run_id: str) -> Dict[str, Any]:
        """Undo a triage decision, putting the run back in the attention queue."""
        return self._request("DELETE", f"/api/runs/{_seg(run_id)}/triage")

    # ── archive (cold storage for terminal runs) ──────────────────────────────

    def list_archived_runs(
        self,
        *,
        name: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Page the archive index, newest-finished-first. Pure index read."""
        return self._request(
            "GET", "/api/archive/runs", params={"name": name, "limit": limit, "offset": offset}
        )

    def get_archived_run(self, run_id: str) -> Dict[str, Any]:
        """Fetch an archived run's full document (run + definition + tasks + events).

        Raises :class:`DagronError` with status 404 when the run was never
        archived, or 410 once it has been compacted to Parquet — the body then
        carries the ``parquet_path`` to read instead.
        """
        return self._request("GET", f"/api/archive/runs/{_seg(run_id)}")

    def archive_run(self, run_id: str) -> Dict[str, Any]:
        """Archive one terminal run **now** instead of waiting for retention.

        Destructive and admin-only: the document is written to the configured
        sink, indexed, and the run is then purged from the hot store — it leaves
        :meth:`list_runs` and reappears under :meth:`list_archived_runs`. Raises
        409 if the run is not terminal and 501 when no archive sink is configured.
        """
        return self._request("POST", f"/api/runs/{_seg(run_id)}/archive")

    # ── workflows (first-class, saved definitions) ────────────────────────────

    def list_workflows(self, *, tag: Optional[str] = None) -> List[Dict[str, Any]]:
        """List saved workflows enriched with schedule + recent-run digest.

        ``tag`` narrows the list to workflows declaring that tag in their spec.
        """
        return self._request("GET", "/api/workflows", params={"tag": tag})

    def get_workflow(self, workflow_id: str) -> Dict[str, Any]:
        """Fetch one saved workflow including its spec."""
        return self._request("GET", f"/api/workflows/{_seg(workflow_id)}")

    def create_workflow(
        self, spec: SpecLike, *, name: Optional[str] = None, description: Optional[str] = None
    ) -> Dict[str, Any]:
        """Save a new workflow. ``name`` defaults to the spec's name. 409 on a dup name."""
        return self._request(
            "POST",
            "/api/workflows",
            body={"spec": _spec_to_str(spec), "name": name, "description": description},
        )

    def update_workflow(
        self,
        workflow_id: str,
        spec: SpecLike,
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Replace a saved workflow's spec (and optionally rename it)."""
        return self._request(
            "PUT",
            f"/api/workflows/{_seg(workflow_id)}",
            body={"spec": _spec_to_str(spec), "name": name, "description": description},
        )

    def delete_workflow(self, workflow_id: str) -> None:
        """Delete a saved workflow."""
        self._request("DELETE", f"/api/workflows/{_seg(workflow_id)}", parse_json=False)

    def run_workflow(
        self, workflow_id: str, *, parameters: Optional[Mapping[str, str]] = None
    ) -> Dict[str, Any]:
        """Trigger a saved workflow as a run. Returns ``{"run_id", "workflow_id"}``.

        ``parameters`` supply arguments for the spec's declared ``parameters:`` —
        this is what makes a stored workflow callable as a function, instead of
        fetching its spec, splicing values in client-side and submitting the
        result as new YAML. A declared ``environment:`` still wins over any key
        it also sets.
        """
        body: Optional[Dict[str, Any]] = {"parameters": dict(parameters)} if parameters else None
        return self._request("POST", f"/api/workflows/{_seg(workflow_id)}/run", body=body)

    def list_workflow_runs(
        self, workflow_id: str, *, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """List one workflow's runs, newest first."""
        return self._request(
            "GET",
            f"/api/workflows/{_seg(workflow_id)}/runs",
            params={"limit": limit, "offset": offset},
        )

    def list_workflow_versions(self, workflow_id: str) -> List[Dict[str, Any]]:
        """List a workflow's version history (every saved definition), newest first."""
        return self._request("GET", f"/api/workflows/{_seg(workflow_id)}/versions")

    def set_workflow_state(self, workflow_id: str, state: str) -> Dict[str, Any]:
        """Set a workflow's lifecycle state: ``active``, ``paused`` or ``retired``.

        Note what this is *not*: deleting. A paused workflow keeps its schedules
        and resumes on exactly the cron it had, and ``retired`` records "we are
        done with this" rather than "off for now" — the distinction a single
        disabled flag would lose.
        """
        return self._request(
            "POST", f"/api/workflows/{_seg(workflow_id)}/state", body={"state": state}
        )

    def apply_bundle(
        self,
        manifest: Union[bytes, str],
        signature: Union[bytes, str],
        files: Mapping[str, Union[bytes, str]],
    ) -> Dict[str, Any]:
        """Apply a signed workflow bundle to this deployment, in one transaction.

        ``manifest`` and ``signature`` are the bundle's raw bytes; ``files`` maps
        each manifest-relative spec path to its content. The SDK base64-encodes
        them for the wire. Verification is fail-closed: an unsigned or
        untrusted-key bundle is refused, and every spec in it is validated before
        anything is written. Raises 501 when no trust set is configured.
        """
        return self._request(
            "POST",
            "/api/workflows/bundle",
            body={
                "manifest_b64": _b64(manifest),
                "signature_b64": _b64(signature),
                "files": [
                    {"path": path, "content_b64": _b64(content)}
                    for path, content in files.items()
                ],
            },
        )

    def workflow_badge(self, name: str) -> str:
        """Fetch a workflow's latest-run status badge as SVG (unauthenticated).

        The same image a README embeds — returned as text so a caller can write
        it to a file or serve it.
        """
        return self._request(
            "GET", f"/api/badges/{_seg(name)}", parse_json=False, auth=False
        )

    def sync_workflow_to_git(self, workflow_id: str) -> Dict[str, Any]:
        """Open a PR committing the workflow's raw spec to the configured GitOps repo."""
        return self._request("POST", f"/api/workflows/{_seg(workflow_id)}/sync-to-git")

    # ── schedules ─────────────────────────────────────────────────────────────

    def list_schedules(self, *, workflow_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all schedules, or just one workflow's."""
        return self._request("GET", "/api/schedules", params={"workflow_id": workflow_id})

    def create_schedule(
        self,
        workflow_id: str,
        cron_expr: str,
        *,
        enabled: bool = True,
        timezone: Optional[str] = None,
        when_expr: Optional[str] = None,
        stop_expr: Optional[str] = None,
        catchup: Optional[bool] = None,
        catchup_window_secs: Optional[int] = None,
        catchup_max_runs: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Attach a cron schedule to a saved workflow.

        ``timezone`` is the IANA zone the cron is read in (so a 02:00 job stays at
        02:00 across a DST shift). ``when_expr`` gates a fire — the schedule only
        runs when it evaluates true — and ``stop_expr`` retires the schedule once
        it does, recording why. The ``catchup*`` knobs decide what happens after
        downtime: whether missed fire-times run at all, how far back to look, and
        how many to materialise at once.
        """
        body: Dict[str, Any] = {
            "workflow_id": workflow_id,
            "cron_expr": cron_expr,
            "enabled": enabled,
        }
        _put_if_set(
            body,
            timezone=timezone,
            when_expr=when_expr,
            stop_expr=stop_expr,
            catchup=catchup,
            catchup_window_secs=catchup_window_secs,
            catchup_max_runs=catchup_max_runs,
        )
        return self._request("POST", "/api/schedules", body=body)

    def update_schedule(
        self,
        schedule_id: str,
        *,
        cron_expr: Optional[str] = None,
        enabled: Optional[bool] = None,
        timezone: Optional[str] = None,
        when_expr: Optional[str] = None,
        stop_expr: Optional[str] = None,
        catchup: Optional[bool] = None,
        catchup_window_secs: Optional[int] = None,
        catchup_max_runs: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Change a schedule; only the fields you pass are sent (and changed).

        Same knobs as :meth:`create_schedule`. The next fire time is recomputed
        server-side from whatever the update leaves in place.
        """
        body: Dict[str, Any] = {}
        _put_if_set(
            body,
            cron_expr=cron_expr,
            enabled=enabled,
            timezone=timezone,
            when_expr=when_expr,
            stop_expr=stop_expr,
            catchup=catchup,
            catchup_window_secs=catchup_window_secs,
            catchup_max_runs=catchup_max_runs,
        )
        return self._request("PUT", f"/api/schedules/{_seg(schedule_id)}", body=body)

    def delete_schedule(self, schedule_id: str) -> None:
        """Remove a schedule."""
        self._request("DELETE", f"/api/schedules/{_seg(schedule_id)}", parse_json=False)

    def backfill_schedule(
        self, schedule_id: str, frm: str, to: str, *, max_runs: Optional[int] = None
    ) -> Dict[str, Any]:
        """Materialise a schedule's missed runs across ``[frm, to]`` (RFC3339).

        Re-issuing the same window is safe — already-materialised fire-times are
        reported as ``skipped`` rather than double-run.
        """
        body: Dict[str, Any] = {"from": frm, "to": to}
        if max_runs is not None:
            body["max_runs"] = max_runs
        return self._request("POST", f"/api/schedules/{_seg(schedule_id)}/backfill", body=body)

    # ── backfill jobs (durable, paced) ────────────────────────────────────────

    def create_backfill(
        self, schedule_id: str, frm: str, to: str, *, max_runs: Optional[int] = None
    ) -> Dict[str, Any]:
        """Create a durable, paced backfill *job* over ``[frm, to]`` (RFC3339).

        Unlike :meth:`backfill_schedule` (which materialises the whole window in one
        synchronous call, capped low), this snapshots the schedule and lets the
        engine drip a bounded number of fire-times per tick — listable, monitorable,
        and cancellable. Returns the created backfill job. Slots already materialised
        by a manual/auto backfill are deduped, never double-run.
        """
        body: Dict[str, Any] = {"schedule_id": schedule_id, "from": frm, "to": to}
        if max_runs is not None:
            body["max_runs"] = max_runs
        return self._request("POST", "/api/backfills", body=body)

    def list_backfills(
        self, *, schedule_id: Optional[str] = None, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """List backfill jobs, newest first; filter by ``schedule_id``."""
        return self._request(
            "GET", "/api/backfills", params={"schedule_id": schedule_id, "limit": limit}
        )

    def get_backfill(self, backfill_id: str) -> Dict[str, Any]:
        """Fetch one backfill job for monitoring (``fired``/``requested``/``status``)."""
        return self._request("GET", f"/api/backfills/{_seg(backfill_id)}")

    def cancel_backfill(self, backfill_id: str) -> Dict[str, Any]:
        """Stop pacing a running backfill job. Returns the updated job."""
        return self._request("POST", f"/api/backfills/{_seg(backfill_id)}/cancel")

    # ── dead letters ──────────────────────────────────────────────────────────

    def list_dead_letters(self, *, limit: int = 100) -> List[Dict[str, Any]]:
        """List parked poison submissions, newest failure first."""
        return self._request("GET", "/api/dead-letters", params={"limit": limit})

    def redrive_dead_letter(self, dead_letter_id: str) -> Dict[str, Any]:
        """Re-attempt a parked payload as a fresh run. Returns ``{"run_id", "redriven_from"}``."""
        return self._request("POST", f"/api/dead-letters/{_seg(dead_letter_id)}/redrive")

    def discard_dead_letter(self, dead_letter_id: str) -> None:
        """Discard a parked payload."""
        self._request("DELETE", f"/api/dead-letters/{_seg(dead_letter_id)}", parse_json=False)

    # ── environments (variable sets + write-only secrets) ─────────────────────

    def list_environments(self) -> List[Dict[str, Any]]:
        """List environments: their variables, and the **names** of their secrets.

        Secret values are never returned — the store is write-only by design, so
        this says what exists, not what it holds.
        """
        return self._request("GET", "/api/environments")

    def create_environment(
        self,
        name: str,
        *,
        variables: Optional[Mapping[str, str]] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create an environment. 409 on a duplicate name.

        A spec names it with ``environment:``; its variables then join the
        substitution scope as ``{{ env.NAME }}`` and its secrets are resolved at
        dispatch. Variable names become env-var names, so keep them
        identifier-shaped.
        """
        body: Dict[str, Any] = {"name": name}
        _put_if_set(
            body,
            description=description,
            variables=dict(variables) if variables is not None else None,
        )
        return self._request("POST", "/api/environments", body=body)

    def update_environment(
        self,
        environment_id: str,
        *,
        variables: Optional[Mapping[str, str]] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Update an environment's description and/or variables.

        A present ``variables`` **replaces the whole map** — pass the full set,
        not a delta. The name is immutable: workflow specs reference it.
        """
        body: Dict[str, Any] = {}
        _put_if_set(
            body,
            description=description,
            variables=dict(variables) if variables is not None else None,
        )
        return self._request("PUT", f"/api/environments/{_seg(environment_id)}", body=body)

    def delete_environment(self, environment_id: str) -> None:
        """Delete an environment and its secrets.

        Runs already created keep working — their parameters were resolved at
        creation. Future runs of specs naming it fail loudly at submit.
        """
        self._request("DELETE", f"/api/environments/{_seg(environment_id)}", parse_json=False)

    def set_environment_secret(self, environment_id: str, name: str, value: str) -> None:
        """Set (or rotate) one secret. Write-only: it is encrypted and never read back.

        Raises :class:`DagronError` with status 503 when the deployment has no
        secret key configured — storing plaintext is not an acceptable fallback.
        """
        self._request(
            "PUT",
            f"/api/environments/{_seg(environment_id)}/secrets/{_seg(name)}",
            body={"value": value},
            parse_json=False,
        )

    def delete_environment_secret(self, environment_id: str, name: str) -> None:
        """Remove one secret from an environment."""
        self._request(
            "DELETE",
            f"/api/environments/{_seg(environment_id)}/secrets/{_seg(name)}",
            parse_json=False,
        )

    # ── datasets (data-aware scheduling + its lineage ledger) ─────────────────

    def list_datasets(self, *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """List the dataset registry, most recently updated first.

        Each row is a dataset URI with when it last changed, what produced that
        change, and which workflows consume it — the registry that ``produces:``
        tasks write and dataset sensors read.
        """
        return self._request("GET", "/api/datasets", params={"limit": limit})

    def list_dataset_events(
        self, *, uri: Optional[str] = None, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """List the lineage ledger newest-first, optionally scoped to one ``uri``.

        Append-only: who updated a dataset, from which run and task, and when.
        """
        return self._request("GET", "/api/datasets/events", params={"uri": uri, "limit": limit})

    # ── instance settings ─────────────────────────────────────────────────────

    def get_notification_settings(self) -> Dict[str, Any]:
        """Read the instance-wide notification defaults (admin only).

        Admin-gated because the stored webhook URLs are effectively secrets.
        """
        return self._request("GET", "/api/settings/notifications")

    def set_notification_settings(self, settings: Mapping[str, Any]) -> Dict[str, Any]:
        """Replace the notification defaults (admin only).

        Takes the full document — ``slack_enabled``, ``slack_webhook_url``,
        ``slack_on``, ``webhook_enabled``, ``webhook_url``, ``webhook_on``. Empty
        ``*_on`` lists mean each target's built-in default: Slack notifies on
        incidents only, the webhook on every event.
        """
        return self._request("PUT", "/api/settings/notifications", body=dict(settings))

    def test_notifications(self, settings: Mapping[str, Any]) -> Dict[str, Any]:
        """Send a test message to each **enabled** target in ``settings``.

        Tests what is on screen, saved or not, and reports per-target outcomes
        rather than failing the whole call, so one broken target does not hide the
        other's success.
        """
        return self._request("POST", "/api/settings/notifications/test", body=dict(settings))

    def get_dead_letter_settings(self) -> Dict[str, Any]:
        """Read the dead-letter retry policy (``max_attempts``; absent = unset)."""
        return self._request("GET", "/api/settings/dead-letters")

    def set_dead_letter_settings(self, max_attempts: int) -> Dict[str, Any]:
        """Set how many times ingestion retries a submission before parking it.

        Takes effect on the next ingestion failure — no restart, and no window
        where the console disagrees with what is running.
        """
        return self._request(
            "PUT", "/api/settings/dead-letters", body={"max_attempts": max_attempts}
        )

    # ── GitOps repository registry ────────────────────────────────────────────

    def list_git_repos(self) -> Dict[str, Any]:
        """List tracked GitOps repositories, with the registry's own state.

        Returns an **object**, not a bare list: ``{"repos": [...],
        "worker_online": bool, "credentials_configured": bool}``. The two flags
        ride along because a repo list alone cannot say whether anything is
        running to sync it, or whether a credential can be stored at all.
        """
        return self._request("GET", "/api/git-repos")

    def connect_git_repo(
        self,
        url: str,
        *,
        branch: Optional[str] = None,
        auto_sync: bool = False,
        path: Optional[str] = None,
        auth: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Register (connect) a Git repository. ``path`` scopes discovery to a
        subdirectory of the repo (server default ``dagron`` when omitted).

        ``auth`` sets the credential in the same call — the same fields
        :meth:`set_git_repo_auth` takes."""
        body: Dict[str, Any] = {"url": url, "branch": branch, "auto_sync": auto_sync}
        if path is not None:
            body["path"] = path
        if auth is not None:
            body["auth"] = dict(auth)
        return self._request("POST", "/api/git-repos", body=body)

    def set_git_repo_auth(
        self,
        repo_id: str,
        *,
        kind: Optional[str] = None,
        username: Optional[str] = None,
        token: Optional[str] = None,
        ssh_private_key: Optional[str] = None,
        known_hosts: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Set or rotate a repository's credential — an HTTPS token or an SSH key.

        Write-only: the secret is encrypted on arrival and is never readable back,
        so this call is the only way to change it. ``known_hosts`` pins the host
        key for SSH remotes.
        """
        body: Dict[str, Any] = {}
        _put_if_set(
            body,
            kind=kind,
            username=username,
            token=token,
            ssh_private_key=ssh_private_key,
            known_hosts=known_hosts,
        )
        return self._request("PUT", f"/api/git-repos/{_seg(repo_id)}/auth", body=body)

    def clear_git_repo_auth(self, repo_id: str) -> None:
        """Remove a repository's stored credential; the repo stays registered."""
        self._request("DELETE", f"/api/git-repos/{_seg(repo_id)}/auth", parse_json=False)

    def sync_git_repo(self, repo_id: str) -> Dict[str, Any]:
        """Mark a tracked repo synced now."""
        return self._request("POST", f"/api/git-repos/{_seg(repo_id)}/sync")

    def disconnect_git_repo(self, repo_id: str) -> None:
        """Stop tracking (disconnect) a repo."""
        self._request("DELETE", f"/api/git-repos/{_seg(repo_id)}", parse_json=False)

    # ── observability ─────────────────────────────────────────────────────────

    def metrics(self) -> Dict[str, Any]:
        """Live run/task counts by status plus the dead-letter total (JSON gauges)."""
        return self._request("GET", "/api/metrics")

    def metrics_timeseries(
        self, *, days: Optional[int] = None, name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Per-day run counts by outcome plus duration stats, newest bucket last.

        ``days`` is the window; ``name`` narrows it to one workflow's trend.
        """
        return self._request("GET", "/api/metrics/timeseries", params={"days": days, "name": name})

    def list_approvals(self) -> List[Dict[str, Any]]:
        """List every task parked in ``awaiting_approval``, oldest first.

        The human-in-the-loop worklist: what to hand :meth:`approve_task` /
        :meth:`reject_task`, across all runs, without walking them.
        """
        return self._request("GET", "/api/approvals")

    def search(self, query: str, *, limit: Optional[int] = None) -> Dict[str, Any]:
        """Search workflows, runs and schedules at once (capped, server-side).

        Returns ``{"query", "workflows", "runs", "schedules"}``.
        """
        return self._request("GET", "/api/search", params={"q": query, "limit": limit})

    def health(self) -> Dict[str, Any]:
        """Rich health: database, scheduler leadership, and the attention counters.

        Never raises for an unhealthy *deployment* — a database failure comes back
        as ``db: "error"``, because an outage is a finding to render, not an error
        to swallow.
        """
        return self._request("GET", "/api/health")

    def healthz(self) -> str:
        """Liveness probe (``"ok"`` while the gateway is serving). Unauthenticated."""
        return self._request("GET", "/healthz", parse_json=False, auth=False)

    def readyz(self) -> str:
        """Readiness probe: 200 only when the datastore answers within its budget
        **and** the event listener is subscribed. Unauthenticated.

        This is the one to point a readiness probe at; :meth:`healthz` is the bare
        liveness check and stays 200 through a database outage.
        """
        return self._request("GET", "/readyz", parse_json=False, auth=False)

    # ── artifacts (task inputs/outputs in the object store) ───────────────────

    def put_artifact(
        self, run_id: str, task: str, name: str, data: Union[bytes, str]
    ) -> str:
        """Store an artifact under ``(run, task, name)``; returns its location.

        Encrypted at rest where the deployment configures a key. The body limit is
        separate from (and much larger than) the one on spec submits, but bodies
        are buffered server-side — this is for checkpoints and outputs, not for
        streaming a dataset.
        """
        return self._request(
            "PUT",
            self._artifact_path(run_id, task, name),
            raw_body=data.encode("utf-8") if isinstance(data, str) else bytes(data),
            parse_json=False,
        )

    def get_artifact(self, run_id: str, task: str, name: str) -> bytes:
        """Fetch an artifact's (decrypted) bytes. 404 when it does not exist."""
        return self._request(
            "GET", self._artifact_path(run_id, task, name), parse_json=False, raw_response=True
        )

    def artifact_exists(self, run_id: str, task: str, name: str) -> bool:
        """Whether an artifact exists, without transferring it."""
        return bool(
            self._request("GET", self._artifact_path(run_id, task, name) + "/exists")["exists"]
        )

    def sync_artifacts(self) -> Dict[str, Any]:
        """Drain the tiered artifact store to its remote tier now (admin only).

        The periodic loop is the default; this is the on-demand path for an
        instance that just regained its uplink. Returns ``{"moved": N}`` — and
        ``0`` on a store that is not tiered. Raises 409 while another store-wide
        sweep is running: overlapping sweeps would upload objects mid-rekey.
        """
        return self._request("POST", "/api/artifacts/sync")

    @staticmethod
    def _artifact_path(run_id: str, task: str, name: str) -> str:
        """Build the artifact route for one ``(run, task, name)`` key."""
        return f"/api/runs/{_seg(run_id)}/artifacts/{_seg(task)}/{_seg(name)}"

    # ── transport ─────────────────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Optional[Any] = None,
        raw_body: Optional[bytes] = None,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        parse_json: bool = True,
        raw_response: bool = False,
        auth: bool = True,
        timeout: Optional[float] = None,
    ) -> Any:
        """Issue one request; return parsed JSON (or text/bytes), or raise :class:`DagronError`.

        ``raw_body`` sends bytes as-is (artifact uploads) instead of JSON-encoding
        ``body``; ``raw_response`` returns the response bytes undecoded, for the
        artifact download that is not text at all. ``timeout`` overrides
        :attr:`timeout` for this call only — a long poll needs more than the
        client's default, and mutating the shared attribute would change every
        request in flight.
        """
        url = self.base_url + path
        if params:
            query = {k: v for k, v in params.items() if v is not None}
            if query:
                url += "?" + urllib.parse.urlencode(query)

        data = None
        hdrs: Dict[str, str] = {"accept": "application/json"}
        if raw_body is not None:
            data = raw_body
            hdrs["content-type"] = "application/octet-stream"
        elif body is not None:
            data = json.dumps(body).encode("utf-8")
            hdrs["content-type"] = "application/json"
        if auth and self.token:
            hdrs["authorization"] = f"Bearer {self.token}"
        # Caller headers last, but they cannot displace auth or content-type:
        # a per-call header is for things like Idempotency-Key, not for
        # quietly re-pointing the request's identity or encoding.
        for k, v in (headers or {}).items():
            if k.lower() not in ("authorization", "content-type"):
                hdrs[k] = v

        req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
        budget = self.timeout if timeout is None else timeout
        try:
            with urllib.request.urlopen(req, timeout=budget) as resp:  # noqa: S310 (scheme checked)
                raw = resp.read()
        except urllib.error.HTTPError as e:
            raise DagronError._from_body(e.code, e.read()) from None
        except urllib.error.URLError as e:
            raise DagronError(0, f"request to {url} failed: {e.reason}") from None
        except TimeoutError:
            # A read that times out *after* the connection is established comes
            # straight out of http.client as a bare socket timeout — URLError
            # never sees it. Without this the client's one-exception-type promise
            # breaks on exactly the calls that wait longest.
            raise DagronError(
                0, f"request to {url} timed out after {budget}s"
            ) from None

        if raw_response:
            return raw
        if not parse_json:
            return raw.decode("utf-8")
        if not raw:
            return None
        return json.loads(raw)


# ── helpers ───────────────────────────────────────────────────────────────────


def _spec_to_str(spec: SpecLike) -> str:
    """Coerce a Dag / mapping / string into the YAML-or-JSON spec string the API wants."""
    if isinstance(spec, Dag):
        return spec.to_json()
    if isinstance(spec, str):
        return spec
    if isinstance(spec, Mapping):
        return json.dumps(spec)
    raise TypeError("spec must be a Dag, a mapping, or a YAML/JSON string")


def _normalize_env(
    env: Union[Mapping[str, str], Sequence[Mapping[str, Any]]],
) -> List[Dict[str, Any]]:
    """Normalise env to the engine's ``[{"name", "value"|"value_from"}]`` shape.

    Accepts a ``{name: value}`` map for the common literal case, or a list of
    entries — each either ``{"name", "value"}`` or ``{"name", "value_from":
    {"secret": NAME}}``, which resolves a secret at dispatch so the credential
    never lands in the spec or the datastore.
    """
    if isinstance(env, Mapping):
        return [{"name": str(k), "value": str(v)} for k, v in env.items()]
    out: List[Dict[str, Any]] = []
    for item in env:
        if not isinstance(item, Mapping) or "name" not in item:
            raise TypeError(
                "env list items must be {'name': ..., 'value': ...} or "
                "{'name': ..., 'value_from': {'secret': ...}} mappings"
            )
        if "value_from" in item:
            ref = item["value_from"]
            if isinstance(ref, str):
                ref = {"secret": ref}
            if not isinstance(ref, Mapping) or "secret" not in ref:
                raise TypeError("env value_from must be {'secret': NAME} (or the name itself)")
            entry: Dict[str, Any] = {
                "name": str(item["name"]),
                "value_from": {"secret": str(ref["secret"])},
            }
            if "value" in item:
                entry["value"] = str(item["value"])
            out.append(entry)
            continue
        if "value" not in item:
            raise TypeError("env list items must set 'value' or 'value_from'")
        out.append({"name": str(item["name"]), "value": str(item["value"])})
    return out


def _normalize_wait(wait: Mapping[str, Any]) -> Dict[str, Any]:
    """Drop unset keys from a ``wait:`` sensor spec, keeping only what was chosen.

    ``sensor()`` passes all four forms with ``None`` for the ones the caller left
    out; emitting those nulls would make the server see four keys set to nothing.
    """
    known = ("for", "until", "url", "dataset")
    out = {k: v for k, v in wait.items() if v is not None}
    unknown = set(out) - set(known)
    if unknown:
        raise TypeError(
            f"unknown wait key(s): {', '.join(sorted(unknown))}; expected any of {', '.join(known)}"
        )
    return out


def _validate_runner_class(name: str, where: str) -> None:
    """Mirror the server's runner-class rule: lowercase ``[a-z0-9_-]``, 1-64 chars.

    ``other`` is refused because it is the metrics tail bucket — a task routed
    there would vanish into the bucket that counts everything else.
    """
    if not name or len(name) > 64:
        raise ValueError(
            f"invalid runner_class for {where}: must be 1-64 characters, got {len(name)} ('{name}')"
        )
    if not re.fullmatch(r"[a-z0-9_-]+", name):
        raise ValueError(f"invalid runner_class for {where}: '{name}' may only contain [a-z0-9_-]")
    if name == "other":
        raise ValueError(
            f"invalid runner_class for {where}: 'other' is reserved (the metrics tail bucket)"
        )


def _when_output_refs(condition: str) -> List[str]:
    """Task names a ``when:`` reads as ``{{ tasks.<name>.output }}``, in order.

    Mirrors the server's ``when_output_refs``: only that exact shape counts, so a
    ``{{ param }}`` substituted at submit is not mistaken for a dependency.
    """
    refs: List[str] = []
    for key in re.findall(r"\{\{(.*?)\}\}", condition, flags=re.S):
        key = key.strip()
        if key.startswith("tasks.") and key.endswith(".output"):
            name = key[len("tasks.") : -len(".output")]
            if name and name not in refs:
                refs.append(name)
    return refs


def _put_if_set(body: Dict[str, Any], **fields: Any) -> Dict[str, Any]:
    """Copy the fields that were actually given into ``body``, in call order.

    A partial-update body must carry only what the caller chose: sending
    ``{"enabled": null}`` for an argument they never passed asks the server to
    change a field they never mentioned.
    """
    for key, value in fields.items():
        if value is not None:
            body[key] = value
    return body


def _b64(data: Union[bytes, str]) -> str:
    """Standard base64 of bytes (or of a string's UTF-8), as the wire wants."""
    return base64.b64encode(data.encode("utf-8") if isinstance(data, str) else data).decode("ascii")


def _seg(value: str) -> str:
    """Percent-encode a single path segment (ids are UUIDs, but never trust input)."""
    return urllib.parse.quote(str(value), safe="")


#: The log filter grammar, as accepted by both log endpoints. Mirrors
#: ``dagron_logging::logfilter`` — the server owns the semantics; this is only
#: the list of names, so a typo becomes a ``TypeError`` here instead of a
#: silently-ignored parameter that makes an unfiltered response look filtered.
LOG_FILTER_PARAMS = frozenset(
    {"q", "exclude", "regex", "level", "case", "context", "limit", "tail"}
)


def log_filter_params(**kwargs: Any) -> Dict[str, Any]:
    """Normalise log filter keyword arguments into query parameters.

    Recognised keys (all optional):

    ``q``
        keep only lines containing this text
    ``exclude``
        drop lines containing this text
    ``regex``
        keep only lines matching this regular expression
    ``level``
        keep only these inferred levels — a string or a sequence of
        ``error``/``warn``/``info``/``debug``/``trace``/``plain``
    ``case``
        match case-sensitively (default: insensitive)
    ``context``
        also keep this many lines either side of each match
    ``limit``
        maximum lines to return
    ``tail``
        when capped, keep the last lines instead of the first

    Levels may be passed as a list; booleans become ``1``/``0``. An unknown key
    raises :class:`TypeError`.
    """
    unknown = set(kwargs) - LOG_FILTER_PARAMS
    if unknown:
        raise TypeError(
            f"unknown log filter parameter(s): {', '.join(sorted(unknown))}; "
            f"expected any of {', '.join(sorted(LOG_FILTER_PARAMS))}"
        )
    out: Dict[str, Any] = {}
    for key, value in kwargs.items():
        if value is None or value == "":
            continue
        if isinstance(value, bool):
            # Only emit the flag when it's on: an explicit `case=0` would still
            # count as "the caller filtered", which changes server behaviour.
            if value:
                out[key] = "1"
        elif isinstance(value, (list, tuple, set, frozenset)):
            joined = ",".join(str(v) for v in value)
            if joined:
                out[key] = joined
        else:
            out[key] = value
    return out


def _parse_sse(lines: Iterable[bytes]) -> Iterator[Dict[str, Any]]:
    """Minimal Server-Sent-Events parser: group ``event:``/``data:`` lines into
    one dict per blank-line-delimited event, JSON-decoding the data when possible."""
    event: Optional[str] = None
    data_lines: List[str] = []
    for raw_line in lines:
        line = raw_line.decode("utf-8", "replace").rstrip("\r\n")
        if line == "":  # dispatch on the blank line that terminates an event
            if data_lines:
                payload = "\n".join(data_lines)
                yield {"event": event or "message", "data": _maybe_json(payload)}
            event, data_lines = None, []
            continue
        if line.startswith(":"):  # comment / keep-alive ping
            continue
        field, _, rest = line.partition(":")
        value = rest[1:] if rest.startswith(" ") else rest
        if field == "event":
            event = value
        elif field == "data":
            data_lines.append(value)
    # Flush a trailing event with no terminating blank line.
    if data_lines:
        yield {"event": event or "message", "data": _maybe_json("\n".join(data_lines))}


def _maybe_json(text: str) -> Any:
    """Parse ``text`` as JSON, returning the raw string when it isn't valid JSON."""
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return text
