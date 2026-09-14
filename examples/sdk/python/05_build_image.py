"""Build the image a task needs, from the workflow that needs it.

The problem this solves: a task needs DuckDB, no image you have has DuckDB, and
getting one normally means writing a Dockerfile, adding a CI job, waiting for it
to publish, and only then writing the workflow. Here the image is part of the
workflow.

    python 05_build_image.py

Pass a `Recipe` as a task's `image=` and the SDK adds the build task for you,
makes the task depend on it, and fills in `docker_image` with the reference that
build will produce. That reference is known *here*, before anything runs,
because it is derived from the recipe — which is also why re-running an
unchanged recipe finds the image already built instead of building it again.

REQUIREMENT: a runner pool that has `dagron-build` on it and claims the `build`
runner class (`ee/compose.image-build.yaml`, or the chart's `build` pool). This
is an enterprise capability — the SDK can describe the build, but it cannot
perform one. Without such a pool the build task sits pending, which is the
honest failure rather than a confusing one.
"""
from __future__ import annotations

from _config import connect
from dagron import Dag, Recipe, RecipeFile


def main() -> None:
    # 1. Say what the image should contain. No Dockerfile, no registry, no CI —
    #    just the base, the packages, and the script.
    recipe = Recipe(
        "duckdb-report",
        "docker.io/library/python:3.12-slim",
        pip=["duckdb==1.1.3"],
        workdir="/app",
        files=[
            RecipeFile(
                "/app/report.py",
                "import duckdb\n"
                "rows = duckdb.sql('SELECT 42 AS answer').fetchall()\n"
                "print(f'duckdb {duckdb.__version__} says {rows[0][0]}')\n",
            )
        ],
    )

    # The reference exists before the image does. This is the whole trick: it is
    # a function of the recipe, so downstream tasks can name it at author time.
    print("image will be:", recipe.image_ref())
    print("recipe sha256:", recipe.hash())

    # 2. Use it. `image=recipe` adds the build task and the dependency.
    #
    #    `image_repository` is where the built image is pushed and pulled from.
    #    Leave it out — as here — for a daemon-local image: built and used on
    #    the same socket, never pushed, which is what the compose stack does.
    #    Set it (registry.example/ws-8f3a) and the SDK pins the build to that
    #    repository *and* turns pushing on, so the tasks and the build can never
    #    disagree about where the image lives.
    dag = Dag("sdk-build-image")
    dag.task("report", image=recipe, command=["python", "/app/report.py"])

    spec = dag.to_spec()
    print(f"\nspec has {len(spec['tasks'])} tasks:")
    for t in spec["tasks"]:
        line = f"  {t['name']}"
        if t.get("runner_class"):
            line += f"  (runner_class: {t['runner_class']})"
        if t.get("docker_image"):
            line += f"  image: {t['docker_image']}"
        print(line)

    # 3. Submit it like any other workflow. The build is an ordinary task: it
    #    shows up on the canvas, it has logs, and it retries like anything else.
    api = connect()
    run_id = api.submit_run(dag)
    print("\nsubmitted run:", run_id)

    run = api.wait_for_run(run_id, timeout=900)
    print("run status:", run["status"])
    for t in run["tasks"]:
        print(f"  {t['name']}: {t['status']}")

    # The build task's stdout ends with the pinned reference — the digest the
    # image actually got, not just the tag it was published under.
    if run["status"] == "succeeded":
        build = next((t for t in run["tasks"] if t["name"].startswith("build-")), None)
        if build:
            logs = api.get_task_logs(run_id, build["id"])
            tail = (logs.get("stdout") or "").strip().splitlines()
            if tail:
                print("\npinned image:", tail[-1])


if __name__ == "__main__":
    main()
