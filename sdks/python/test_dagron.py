"""Tests for the dagron Python SDK — builder validation + a real-socket client.

Standard-library only. The :class:`Client` tests run against a threaded
``http.server`` fake gateway that records each request and returns canned
responses, so we exercise actual URL/method/header/body construction (not mocks).
Run with ``python -m unittest``.
"""

import base64
import json
import os
import threading
import unittest
from unittest import mock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dagron import (
    BUILD_GENERATOR_VERSION,
    Client,
    Dag,
    DagronError,
    Recipe,
    RecipeFile,
)


# ── Builder (Dag) ─────────────────────────────────────────────────────────────


class DagTests(unittest.TestCase):
    def test_builds_spec_with_deps(self):
        dag = Dag("etl")
        a = dag.task("extract", image="alpine", command=["echo", "hi"])
        dag.task("load", image="alpine", command=["true"], depends_on=[a])

        spec = dag.to_spec()
        self.assertEqual(spec["name"], "etl")
        self.assertEqual(len(spec["tasks"]), 2)
        self.assertEqual(spec["tasks"][0]["docker_image"], "alpine")
        self.assertEqual(spec["tasks"][1]["depends_on"], ["extract"])
        # Empty fields omitted.
        self.assertNotIn("depends_on", spec["tasks"][0])

    def test_to_json_is_valid_json(self):
        dag = Dag("w")
        dag.task("t", command=["true"])
        parsed = json.loads(dag.to_json())
        self.assertEqual(parsed["name"], "w")
        self.assertEqual(parsed["tasks"][0]["name"], "t")

    def test_rejects_duplicate(self):
        dag = Dag("w")
        dag.task("a", command=["true"])
        with self.assertRaises(ValueError):
            dag.task("a", command=["true"])

    def test_rejects_unknown_dependency(self):
        dag = Dag("w")
        dag.task("a", command=["true"], depends_on=["ghost"])
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_rejects_empty_task_name(self):
        dag = Dag("w")
        with self.assertRaises(ValueError):
            dag.task("")

    def test_rejects_string_command_and_depends_on(self):
        dag = Dag("w")
        with self.assertRaises(TypeError):
            dag.task("a", command="echo hi")
        with self.assertRaises(TypeError):
            dag.task("b", depends_on="a")

    def test_to_spec_does_not_expose_internal_state(self):
        dag = Dag("w")
        dag.task("a", command=["true"])
        dag.to_spec()["tasks"][0]["command"].append("mutated")
        self.assertEqual(dag.to_spec()["tasks"][0]["command"], ["true"])

    def test_rejects_task_without_command_or_ref(self):
        dag = Dag("w")
        dag.task("a")  # neither command nor workflow_ref
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_rejects_task_with_both_command_and_ref(self):
        dag = Dag("w")
        dag.task("a", command=["true"], workflow_ref="other")
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_accepts_workflow_ref_chain(self):
        dag = Dag("w")
        dag.task("call", workflow_ref="child")
        spec = dag.to_spec()
        self.assertEqual(spec["tasks"][0]["workflow_ref"], "child")
        self.assertNotIn("command", spec["tasks"][0])

    def test_rejects_cycle(self):
        dag = Dag("w")
        dag.task("a", command=["true"], depends_on=["b"])
        dag.task("b", command=["true"], depends_on=["a"])
        with self.assertRaisesRegex(ValueError, "cycle"):
            dag.to_spec()

    def test_full_task_fields(self):
        dag = Dag("w")
        dag.task(
            "a",
            image="alpine",
            command=["run"],
            input={"k": "v"},
            max_attempts=3,
            retry_delay_secs=5,
            timeout_secs=60,
            env={"FOO": "bar"},
            resources={"requests": {"cpu": "250m"}},
            service_account="task-sa",
        )
        t = dag.to_spec()["tasks"][0]
        self.assertEqual(t["max_attempts"], 3)
        self.assertEqual(t["retry_delay_secs"], 5)
        self.assertEqual(t["timeout_secs"], 60)
        self.assertEqual(t["input"], {"k": "v"})
        self.assertEqual(t["env"], [{"name": "FOO", "value": "bar"}])
        self.assertEqual(t["resources"], {"requests": {"cpu": "250m"}})
        self.assertEqual(t["service_account"], "task-sa")

    def test_rejects_max_attempts_below_one(self):
        dag = Dag("w")
        with self.assertRaises(ValueError):
            dag.task("a", command=["true"], max_attempts=0)


class DagSpecLevelTests(unittest.TestCase):
    """Spec-level properties: emitted when set, omitted when not, validated here."""

    def test_emits_spec_level_fields(self):
        dag = Dag(
            "etl",
            runner_class="etl",
            parameters={"day": "today"},
            tags=["nightly"],
            environment="prod",
            task_defaults={"max_attempts": 3},
            run_timeout_secs=3600,
            max_active_runs=1,
            result_from="load",
            budget={"tasks": 50},
            deadline={"within": "2h"},
            notify={"slack": {"webhook_url": "https://hooks.example/x", "on": ["failed"]}},
            on_datasets=["s3://bucket/raw"],
            datasets_mode="all",
        )
        dag.task("load", command=["true"])
        spec = dag.to_spec()
        self.assertEqual(spec["parameters"], {"day": "today"})
        self.assertEqual(spec["tags"], ["nightly"])
        self.assertEqual(spec["environment"], "prod")
        self.assertEqual(spec["task_defaults"], {"max_attempts": 3})
        self.assertEqual(spec["run_timeout_secs"], 3600)
        self.assertEqual(spec["max_active_runs"], 1)
        self.assertEqual(spec["result_from"], "load")
        self.assertEqual(spec["budget"], {"tasks": 50})
        self.assertEqual(spec["deadline"], {"within": "2h"})
        self.assertEqual(
            spec["notify"],
            {"slack": {"webhook_url": "https://hooks.example/x", "on": ["failed"]}},
        )
        self.assertEqual(spec["on_datasets"], ["s3://bucket/raw"])
        self.assertEqual(spec["datasets_mode"], "all")

    def test_omits_unset_spec_level_fields(self):
        dag = Dag("w")
        dag.task("a", command=["true"])
        self.assertEqual(sorted(dag.to_spec()), ["name", "tasks"])

    def test_result_from_must_name_a_task(self):
        dag = Dag("w", result_from="ghost")
        dag.task("a", command=["true"])
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_zero_run_timeout_is_rejected(self):
        dag = Dag("w", run_timeout_secs=0)
        dag.task("a", command=["true"])
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_runner_class_charset_is_enforced(self):
        for bad in ("ETL", "with space", "other", "x" * 65):
            dag = Dag("w", runner_class=bad)
            dag.task("a", command=["true"])
            with self.assertRaises(ValueError, msg=bad):
                dag.to_spec()

    def test_templated_runner_class_is_left_to_the_server(self):
        # `{{ pool }}` is only a real class name after the server substitutes it,
        # so the charset check must not fire on the template itself.
        dag = Dag("w", runner_class="{{ pool }}")
        dag.task("a", command=["true"], runner_class="{{ pool }}")
        self.assertEqual(dag.to_spec()["runner_class"], "{{ pool }}")


class DagTaskKindTests(unittest.TestCase):
    """The kind rules: leaf / call / chain / approval / workflow / wait."""

    def test_approval_gate(self):
        dag = Dag("w")
        dag.task("build", command=["true"])
        dag.approval("gate", depends_on=["build"], timeout_secs=3600, on_timeout="reject")
        t = dag.to_spec()["tasks"][1]
        self.assertEqual(t["type"], "approval")
        self.assertEqual(t["approval_timeout_secs"], 3600)
        self.assertEqual(t["approval_on_timeout"], "reject")
        self.assertNotIn("command", t)

    def test_rejects_bad_approval_timeout_resolution(self):
        dag = Dag("w")
        with self.assertRaises(ValueError):
            dag.approval("gate", on_timeout="maybe")

    def test_sensor_forms(self):
        dag = Dag("w")
        dag.sensor("wait_5m", duration="5m")
        dag.sensor("wait_data", dataset="s3://bucket/raw")
        tasks = dag.to_spec()["tasks"]
        self.assertEqual(tasks[0]["type"], "wait")
        self.assertEqual(tasks[0]["wait"], {"for": "5m"})
        self.assertEqual(tasks[1]["wait"], {"dataset": "s3://bucket/raw"})

    def test_sensor_needs_exactly_one_form(self):
        dag = Dag("w")
        dag.sensor("none")
        with self.assertRaises(ValueError):
            dag.to_spec()
        dag = Dag("w")
        dag.sensor("both", duration="5m", until="2026-01-01T00:00:00Z")
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_wait_block_only_on_a_wait_task(self):
        dag = Dag("w")
        dag.task("a", command=["true"], wait={"for": "5m"})
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_a_sensor_cannot_also_be_a_hook(self):
        dag = Dag("w")
        dag.sensor("settle", duration="5m", hook="on_exit")
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_sub_workflow_trigger(self):
        dag = Dag("w")
        dag.trigger("child", "downstream", arguments={"shard": "1"})
        t = dag.to_spec()["tasks"][0]
        self.assertEqual(t["type"], "workflow")
        self.assertEqual(t["workflow"], "downstream")
        self.assertEqual(t["arguments"], {"shard": "1"})

    def test_workflow_target_only_on_a_workflow_task(self):
        dag = Dag("w")
        dag.task("a", command=["true"], workflow="other")
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_workflow_task_needs_a_target(self):
        dag = Dag("w")
        dag.task("a", task_type="workflow")
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_commandless_kind_cannot_carry_a_command(self):
        dag = Dag("w")
        dag.task("gate", task_type="approval", command=["true"])
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_arguments_need_a_callee(self):
        dag = Dag("w")
        dag.task("a", command=["true"], arguments={"k": "v"})
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_depends_on_may_forward_reference_into_a_chain(self):
        # `call.inner` only exists once the chain is inlined server-side, so the
        # builder must defer rather than call it an unknown dependency.
        dag = Dag("w")
        dag.task("call", workflow_ref="child")
        dag.task("after", command=["true"], depends_on=["call.inner"])
        self.assertEqual(len(dag.to_spec()["tasks"]), 2)


class DagTemplateTests(unittest.TestCase):
    def test_declares_and_calls_a_template(self):
        dag = Dag("w")
        tpl = dag.template("build", parameters={"target": "release"})
        tpl.task("compile", command=["make", "{{ target }}"])
        dag.task("run-build", template="build", arguments={"target": "debug"})

        spec = dag.to_spec()
        self.assertEqual(spec["templates"][0]["name"], "build")
        self.assertEqual(spec["templates"][0]["parameters"], {"target": "release"})
        self.assertEqual(spec["templates"][0]["tasks"][0]["name"], "compile")
        self.assertEqual(spec["tasks"][0]["template"], "build")
        self.assertEqual(spec["tasks"][0]["arguments"], {"target": "debug"})

    def test_rejects_call_to_undeclared_template(self):
        dag = Dag("w")
        dag.task("call", template="ghost")
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_rejects_duplicate_template(self):
        dag = Dag("w")
        dag.template("t")
        with self.assertRaises(ValueError):
            dag.template("t")

    def test_template_to_spec_does_not_expose_internal_state(self):
        # Read directly (not through Dag.to_spec, whose outer deep-copy would
        # mask it), the returned tasks must not be the builder's own list.
        dag = Dag("w")
        tpl = dag.template("t")
        tpl.task("a", command=["true"])
        tpl.to_spec()["tasks"][0]["command"].append("mutated")
        self.assertEqual(tpl.to_spec()["tasks"][0]["command"], ["true"])

    def test_template_subgraph_is_validated_too(self):
        dag = Dag("w")
        tpl = dag.template("t")
        tpl.task("a", command=["true"], depends_on=["ghost"])
        dag.task("call", template="t")
        with self.assertRaises(ValueError):
            dag.to_spec()


class DagTaskOptionTests(unittest.TestCase):
    def test_scheduling_and_retry_options(self):
        dag = Dag("w")
        dag.task(
            "train",
            command=["train.sh"],
            pool="gpu",
            priority=10,
            runner_class="ml_training",
            retry_max_delay_secs=300,
            retry_on_timeout=False,
            retry_budgets={"gpu-ecc": 8, "nan-loss": 0},
            gang=4,
            produces=["s3://bucket/model"],
            cache={"key": "{{ params.day }}", "max_age_secs": 86400},
            isolation={"read_only_root": True},
        )
        t = dag.to_spec()["tasks"][0]
        self.assertEqual(t["pool"], "gpu")
        self.assertEqual(t["priority"], 10)
        self.assertEqual(t["retry_max_delay_secs"], 300)
        self.assertIs(t["retry_on_timeout"], False)
        self.assertEqual(t["retry_budgets"], {"gpu-ecc": 8, "nan-loss": 0})
        self.assertEqual(t["gang"], {"size": 4})
        self.assertEqual(t["produces"], ["s3://bucket/model"])
        self.assertEqual(t["cache"], {"key": "{{ params.day }}", "max_age_secs": 86400})
        self.assertEqual(t["isolation"], {"read_only_root": True})

    def test_zero_priority_is_omitted(self):
        # 0 means "fall back to task_defaults"; emitting it would pin the task.
        dag = Dag("w")
        dag.task("a", command=["true"], priority=0)
        self.assertNotIn("priority", dag.to_spec()["tasks"][0])

    def test_fan_out_options(self):
        dag = Dag("w")
        dag.task(
            "shard",
            command=["run", "{{ item }}"],
            with_items=["eu", "us"],
            instance_key="{{ item }}",
        )
        t = dag.to_spec()["tasks"][0]
        self.assertEqual(t["with_items"], ["eu", "us"])
        self.assertEqual(t["instance_key"], "{{ item }}")

    def test_flow_control_options(self):
        dag = Dag("w")
        dag.task("a", command=["true"])
        dag.task("cleanup", command=["true"], hook="on_exit", allow_failure=True)
        dag.task("b", command=["true"], depends_on=["a"], trigger_rule="all_done")
        tasks = dag.to_spec()["tasks"]
        self.assertEqual(tasks[1]["hook"], "on_exit")
        self.assertIs(tasks[1]["allow_failure"], True)
        self.assertEqual(tasks[2]["trigger_rule"], "all_done")

    def test_rejects_unknown_trigger_rule(self):
        dag = Dag("w")
        dag.task("a", command=["true"], trigger_rule="sometimes")
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_rejects_unknown_hook(self):
        dag = Dag("w")
        with self.assertRaises(ValueError):
            dag.task("a", command=["true"], hook="on_tuesday")

    def test_when_must_depend_on_what_it_reads(self):
        dag = Dag("w")
        dag.task("a", command=["true"])
        dag.task("b", command=["true"], when="{{ tasks.a.output }} == go")
        with self.assertRaises(ValueError):
            dag.to_spec()
        dag = Dag("w")
        dag.task("a", command=["true"])
        dag.task("b", command=["true"], depends_on=["a"], when="{{ tasks.a.output }} == go")
        self.assertEqual(dag.to_spec()["tasks"][1]["when"], "{{ tasks.a.output }} == go")

    def test_repeat_is_validated(self):
        dag = Dag("w")
        dag.task("poll", command=["check"], repeat={"until": "", "max_iterations": 5})
        with self.assertRaises(ValueError):
            dag.to_spec()
        dag = Dag("w")
        dag.task("poll", command=["check"], repeat={"until": "done", "max_iterations": 0})
        with self.assertRaises(ValueError):
            dag.to_spec()
        dag = Dag("w")
        dag.approval("gate")
        dag._tasks[0]["repeat"] = {"until": "done", "max_iterations": 3}
        with self.assertRaises(ValueError):
            dag.to_spec()

    def test_env_accepts_a_secret_reference(self):
        dag = Dag("w")
        dag.task(
            "a",
            command=["true"],
            env=[{"name": "TOKEN", "value_from": {"secret": "API_TOKEN"}}, {"name": "X", "value": "1"}],
        )
        self.assertEqual(
            dag.to_spec()["tasks"][0]["env"],
            [{"name": "TOKEN", "value_from": {"secret": "API_TOKEN"}}, {"name": "X", "value": "1"}],
        )

    def test_env_rejects_an_entry_with_neither_value_nor_secret(self):
        dag = Dag("w")
        with self.assertRaises(TypeError):
            dag.task("a", command=["true"], env=[{"name": "X"}])

    def test_repeat_rejects_a_key_the_wire_does_not_have(self):
        # A camelCased `maxIterations` would leave the required field unset:
        # local validation would pass and the submit would be refused.
        dag = Dag("w")
        with self.assertRaises(TypeError):
            dag.task("poll", command=["check"], repeat={"until": "done", "maxIterations": 3})

    def test_rejects_string_produces(self):
        dag = Dag("w")
        with self.assertRaises(TypeError):
            dag.task("a", command=["true"], produces="s3://bucket/x")


# ── Fake gateway ──────────────────────────────────────────────────────────────


class _Handler(BaseHTTPRequestHandler):
    """Records each request on the server and replies from its response table."""

    def _serve(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw_body = self.rfile.read(length) if length else b""
        parsed = urlparse(self.path)
        self.server.requests.append(
            {
                "method": self.command,
                "path": parsed.path,
                "query": parse_qs(parsed.query),
                "headers": dict(self.headers),
                "body": raw_body.decode("utf-8", "replace") if raw_body else "",
                # The utf-8 view mangles non-text bytes, so keep the raw body for
                # the assertions that care (artifact uploads).
                "raw_body": raw_body,
            }
        )

        # Server-Sent Events: stream canned lines, then close to signal EOF.
        if parsed.path.endswith("/stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            self.wfile.write(self.server.sse_body.encode("utf-8"))
            return

        # Fail fast on an unconfigured route: a 200 default would mask a typo or a
        # client-side path regression as a false green. Force every route to be stubbed.
        key = (self.command, parsed.path)
        if key not in self.server.responses and not self.server.sequences.get(key):
            msg = f"unconfigured fake-gateway route: {self.command} {parsed.path}".encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)
            return
        # A queued sequence answers one call each, so a paginating client can be
        # driven through more than one page of the same route.
        queued = self.server.sequences.get(key)
        status, payload = queued.pop(0) if queued else self.server.responses[key]
        body = b""
        self.send_response(status)
        if payload is None:
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if isinstance(payload, (bytes, bytearray)):
            body = bytes(payload)
            self.send_header("Content-Type", "application/octet-stream")
        elif isinstance(payload, (dict, list)):
            body = json.dumps(payload).encode("utf-8")
            self.send_header("Content-Type", "application/json")
        else:
            body = str(payload).encode("utf-8")
            self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = do_PUT = do_DELETE = _serve

    def log_message(self, *args):  # silence the default stderr access log
        pass


class GatewayTestCase(unittest.TestCase):
    """Spins up a fake gateway for each test; configure ``self.server.responses``."""

    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.server.requests = []
        self.server.responses = {}
        self.server.sequences = {}
        self.server.sse_body = ""
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.base_url = f"http://{host}:{port}"
        self.client = Client(self.base_url, token="tok")

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def respond(self, method, path, status, payload):
        self.server.responses[(method, path)] = (status, payload)

    def respond_each(self, method, path, *responses):
        """Queue one response per call to this route, in order."""
        self.server.sequences[(method, path)] = list(responses)

    def last_request(self):
        return self.server.requests[-1]


# ── Client transport + auth ───────────────────────────────────────────────────


class ClientConstructionTests(unittest.TestCase):
    def test_rejects_non_http_scheme(self):
        with self.assertRaises(ValueError):
            Client("file:///etc/passwd")

    def test_dag_submit_rejects_non_http_scheme(self):
        dag = Dag("w")
        dag.task("a", command=["true"])
        with self.assertRaises(ValueError):
            dag.submit("file:///etc/passwd")


class ClientAuthTests(GatewayTestCase):
    def test_login_stores_and_sends_token(self):
        client = Client(self.base_url)  # no token yet
        self.respond("POST", "/api/login", 200, {"token": "JWT123"})
        self.respond("GET", "/api/me", 200, {"email": "a@b.c"})

        token = client.login("a@b.c", "password1")
        self.assertEqual(token, "JWT123")
        self.assertEqual(client.token, "JWT123")
        # Login itself is unauthenticated; credentials are in the body.
        login_req = self.server.requests[0]
        self.assertNotIn("Authorization", login_req["headers"])
        self.assertEqual(json.loads(login_req["body"]), {"email": "a@b.c", "password": "password1"})
        # A subsequent call carries the freshly minted bearer token.
        client.me()
        self.assertEqual(self.last_request()["headers"].get("Authorization"), "Bearer JWT123")

    def test_login_without_token_raises(self):
        client = Client(self.base_url)
        self.respond("POST", "/api/login", 200, {})
        with self.assertRaises(DagronError):
            client.login("a@b.c", "password1")


# ── Client: runs ──────────────────────────────────────────────────────────────


class ClientRunTests(GatewayTestCase):
    def test_submit_run_wraps_spec_in_yaml_field(self):
        self.respond("POST", "/api/runs", 201, {"run_id": "r-1"})
        dag = Dag("etl")
        dag.task("a", command=["true"])

        run_id = self.client.submit_run(dag)
        self.assertEqual(run_id, "r-1")
        req = self.last_request()
        self.assertEqual(req["method"], "POST")
        self.assertEqual(req["path"], "/api/runs")
        self.assertEqual(req["headers"].get("Authorization"), "Bearer tok")
        # The gateway contract is {"yaml": "<spec string>"} — NOT the raw spec.
        body = json.loads(req["body"])
        self.assertEqual(set(body), {"yaml"})
        self.assertEqual(json.loads(body["yaml"]), {"name": "etl", "tasks": [{"name": "a", "command": ["true"]}]})

    def test_submit_run_accepts_dict_and_string(self):
        self.respond("POST", "/api/runs", 201, {"run_id": "r-2"})
        self.client.submit_run({"name": "x", "tasks": []})
        self.assertEqual(json.loads(self.last_request()["body"])["yaml"], '{"name": "x", "tasks": []}')

        self.respond("POST", "/api/runs", 201, {"run_id": "r-3"})
        self.client.submit_run("name: y\ntasks: []\n")
        self.assertEqual(self.last_request()["body"], json.dumps({"yaml": "name: y\ntasks: []\n"}))

    def test_submit_run_sends_parameters_only_when_given(self):
        self.respond("POST", "/api/runs", 201, {"run_id": "r-4"})
        self.client.submit_run("name: y\ntasks: []\n", parameters={"date": "2026-08-18"})
        body = json.loads(self.last_request()["body"])
        self.assertEqual(body["parameters"], {"date": "2026-08-18"})

        # Omitted (and empty) parameters must not appear at all: the gateway's
        # pre-existing body shape is {"yaml": ...} and every older server has
        # to keep accepting exactly that.
        self.respond("POST", "/api/runs", 201, {"run_id": "r-5"})
        self.client.submit_run("name: y\ntasks: []\n", parameters={})
        self.assertEqual(set(json.loads(self.last_request()["body"])), {"yaml"})

    def test_submit_run_sends_the_idempotency_key_as_a_header(self):
        self.respond("POST", "/api/runs", 201, {"run_id": "r-6"})
        self.client.submit_run("name: y\ntasks: []\n", idempotency_key="job-42")
        headers = {k.lower(): v for k, v in self.last_request()["headers"].items()}
        self.assertEqual(headers.get("idempotency-key"), "job-42")

        # No key, no header — the endpoint stays non-idempotent by default and
        # an empty key is a 400 server-side, so one must never be invented here.
        self.respond("POST", "/api/runs", 201, {"run_id": "r-7"})
        self.client.submit_run("name: y\ntasks: []\n")
        headers = {k.lower(): v for k, v in self.last_request()["headers"].items()}
        self.assertNotIn("idempotency-key", headers)

    def test_an_explicit_empty_idempotency_key_is_rejected_locally(self):
        # An empty string is a 400 server-side. The old ``if idempotency_key``
        # dropped it silently (``""`` is falsy), handing back a non-idempotent
        # submit the caller thinks is retry-safe. Reject it before the request,
        # so the caller learns loudly rather than after a duplicate has run.
        with self.assertRaises(ValueError):
            self.client.submit_run("name: y\ntasks: []\n", idempotency_key="")
        with self.assertRaises(ValueError):
            self.client.submit_run("name: y\ntasks: []\n", idempotency_key="   ")

    def test_a_caller_header_cannot_displace_authorization(self):
        self.respond("POST", "/api/runs", 201, {"run_id": "r-8"})
        self.client._request(
            "POST", "/api/runs", body={"yaml": "x"}, headers={"Authorization": "Bearer stolen"}
        )
        self.assertEqual(self.last_request()["headers"].get("Authorization"), "Bearer tok")

    def test_list_runs_builds_query_string(self):
        self.respond("GET", "/api/runs", 200, [{"id": "r-1"}])
        rows = self.client.list_runs(status="failed", limit=10, offset=20)
        self.assertEqual(rows, [{"id": "r-1"}])
        q = self.last_request()["query"]
        self.assertEqual(q, {"status": ["failed"], "limit": ["10"], "offset": ["20"]})

    def test_list_runs_omits_unset_params(self):
        self.respond("GET", "/api/runs", 200, [])
        self.client.list_runs()
        self.assertEqual(self.last_request()["query"], {})

    def test_get_run_logs_encodes_the_filter(self):
        self.respond("GET", "/api/runs/r-1/logs", 200, {"lines": []})
        self.client.get_run_logs(
            "r-1", level=["error", "warn"], context=2, tail=True, case=False,
            regex=r"rows=\d+", tasks=["extract", "load"],
        )
        req = self.last_request()
        self.assertEqual(req["path"], "/api/runs/r-1/logs")
        self.assertEqual(
            req["query"],
            {
                "level": ["error,warn"],
                "context": ["2"],
                "tail": ["1"],
                "regex": [r"rows=\d+"],
                "task": ["extract,load"],
            },
        )
        # `case=False` is dropped, not sent as 0: an explicit `case=0` would
        # still count as "the caller filtered" and change server behaviour.
        self.assertNotIn("case", req["query"])

    def test_get_run_logs_without_a_filter_sends_no_params(self):
        # An unfiltered read must stay byte-identical to what it was before
        # filters existed — a stray `q=` would push the server onto its filter
        # path for nothing.
        self.respond("GET", "/api/runs/r-1/logs", 200, {"lines": []})
        self.client.get_run_logs("r-1")
        self.assertEqual(self.last_request()["query"], {})

    def test_get_task_logs_combines_offset_and_filter(self):
        self.respond("GET", "/api/runs/r-1/tasks/t-1/logs", 200, {"output": ""})
        self.client.get_task_logs("r-1", "t-1", offset=120, level="error")
        self.assertEqual(
            self.last_request()["query"], {"offset": ["120"], "level": ["error"]}
        )

    def test_unknown_log_filter_parameter_raises(self):
        # A typo'd filter name must fail loudly here: silently dropping it would
        # return an unfiltered response the caller reads as filtered.
        with self.assertRaises(TypeError):
            self.client.get_run_logs("r-1", levl="error")

    def test_get_run_encodes_path_segment(self):
        # A '/' in the id must be percent-encoded into one segment, not split into
        # path structure (no traversal / wrong-endpoint hits).
        self.respond("GET", "/api/runs/a%2Fb", 200, {"id": "a/b"})
        self.assertEqual(self.client.get_run("a/b"), {"id": "a/b"})
        self.assertEqual(self.last_request()["path"], "/api/runs/a%2Fb")

    def test_cancel_run_returns_count(self):
        self.respond("POST", "/api/runs/r-1/cancel", 200, {"cancelled": 3})
        self.assertEqual(self.client.cancel_run("r-1"), 3)

    def test_rerun_with_and_without_params(self):
        self.respond("POST", "/api/runs/r-1/rerun", 200, {"run_id": "r-1", "rerun": 2})
        self.client.rerun_run("r-1", params={"k": "v"})
        self.assertEqual(json.loads(self.last_request()["body"]), {"params": {"k": "v"}})

        self.client.rerun_run("r-1")
        self.assertEqual(json.loads(self.last_request()["body"]), {})

    def test_resubmit_and_retry(self):
        self.respond("POST", "/api/runs/r-1/resubmit", 201, {"run_id": "r-9"})
        self.assertEqual(self.client.resubmit_run("r-1"), "r-9")
        self.respond("POST", "/api/runs/r-1/tasks/t-1/retry", 200, {"retried": True})
        self.assertTrue(self.client.retry_task("r-1", "t-1"))

    def test_stream_run_parses_sse_events(self):
        self.server.sse_body = (
            "event: task\n"
            'data: {"task": "a", "status": "running"}\n'
            "\n"
            ": keep-alive\n"
            "event: resync\n"
            "data: lagged\n"
            "\n"
        )
        events = list(self.client.stream_run("r-1"))
        self.assertEqual(events[0], {"event": "task", "data": {"task": "a", "status": "running"}})
        self.assertEqual(events[1], {"event": "resync", "data": "lagged"})
        # Also pin the request contract, not just the parse: right verb, path, auth.
        req = self.last_request()
        self.assertEqual(req["method"], "GET")
        self.assertEqual(req["path"], "/api/runs/r-1/stream")
        self.assertEqual(req["headers"].get("Authorization"), "Bearer tok")


# ── Client: workflows / schedules / dead-letters ──────────────────────────────


class ClientWorkflowTests(GatewayTestCase):
    def test_create_workflow_sends_spec_name_description(self):
        self.respond("POST", "/api/workflows", 201, {"id": "wf-1", "name": "etl"})
        dag = Dag("etl")
        dag.task("a", command=["true"])
        self.client.create_workflow(dag, name="etl", description="nightly")
        body = json.loads(self.last_request()["body"])
        self.assertEqual(body["name"], "etl")
        self.assertEqual(body["description"], "nightly")
        self.assertEqual(json.loads(body["spec"])["name"], "etl")

    def test_delete_workflow_returns_none_on_204(self):
        self.respond("DELETE", "/api/workflows/wf-1", 204, None)
        self.assertIsNone(self.client.delete_workflow("wf-1"))

    def test_run_workflow(self):
        self.respond("POST", "/api/workflows/wf-1/run", 201, {"run_id": "r-1", "workflow_id": "wf-1"})
        self.assertEqual(self.client.run_workflow("wf-1")["run_id"], "r-1")

    def test_create_schedule(self):
        self.respond("POST", "/api/schedules", 201, {"id": "s-1"})
        self.client.create_schedule("wf-1", "0 0 * * * *", enabled=False)
        self.assertEqual(
            json.loads(self.last_request()["body"]),
            {"workflow_id": "wf-1", "cron_expr": "0 0 * * * *", "enabled": False},
        )

    def test_update_schedule_patches_only_given_fields(self):
        self.respond("PUT", "/api/schedules/s-1", 200, {"id": "s-1"})
        self.client.update_schedule("s-1", enabled=True)
        self.assertEqual(json.loads(self.last_request()["body"]), {"enabled": True})

    def test_backfill_schedule(self):
        self.respond("POST", "/api/schedules/s-1/backfill", 200, {"scheduled": 2, "skipped": 0})
        self.client.backfill_schedule("s-1", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", max_runs=50)
        body = json.loads(self.last_request()["body"])
        self.assertEqual(body, {"from": "2026-01-01T00:00:00Z", "to": "2026-01-02T00:00:00Z", "max_runs": 50})

    def test_create_backfill_job(self):
        self.respond("POST", "/api/backfills", 201, {"id": "bf-1", "status": "running"})
        job = self.client.create_backfill(
            "s-1", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", max_runs=100
        )
        self.assertEqual(job["id"], "bf-1")
        self.assertEqual(
            json.loads(self.last_request()["body"]),
            {
                "schedule_id": "s-1",
                "from": "2026-01-01T00:00:00Z",
                "to": "2026-01-02T00:00:00Z",
                "max_runs": 100,
            },
        )

    def test_create_backfill_omits_unset_max_runs(self):
        self.respond("POST", "/api/backfills", 201, {"id": "bf-2"})
        self.client.create_backfill("s-1", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
        self.assertNotIn("max_runs", json.loads(self.last_request()["body"]))

    def test_list_get_cancel_backfill(self):
        self.respond("GET", "/api/backfills", 200, [{"id": "bf-1"}])
        self.assertEqual(self.client.list_backfills(schedule_id="s-1")[0]["id"], "bf-1")
        self.assertEqual(self.last_request()["query"], {"schedule_id": ["s-1"]})

        self.respond("GET", "/api/backfills/bf-1", 200, {"id": "bf-1", "fired": 3})
        self.assertEqual(self.client.get_backfill("bf-1")["fired"], 3)

        self.respond("POST", "/api/backfills/bf-1/cancel", 200, {"id": "bf-1", "status": "cancelled"})
        self.assertEqual(self.client.cancel_backfill("bf-1")["status"], "cancelled")

    def test_approve_and_reject_task(self):
        self.respond(
            "POST", "/api/runs/r-1/tasks/t-1/approve", 200,
            {"run_id": "r-1", "task_id": "t-1", "resolution": "approved"},
        )
        self.assertEqual(self.client.approve_task("r-1", "t-1")["resolution"], "approved")

        self.respond(
            "POST", "/api/runs/r-1/tasks/t-2/reject", 200,
            {"run_id": "r-1", "task_id": "t-2", "resolution": "rejected"},
        )
        self.assertEqual(self.client.reject_task("r-1", "t-2")["resolution"], "rejected")

    def test_dead_letters_and_git_repos(self):
        self.respond("GET", "/api/dead-letters", 200, [{"id": "dl-1"}])
        self.assertEqual(self.client.list_dead_letters(limit=5)[0]["id"], "dl-1")
        self.assertEqual(self.last_request()["query"], {"limit": ["5"]})

        self.respond("POST", "/api/git-repos", 201, {"id": "g-1"})
        self.client.connect_git_repo(
            "https://github.com/o/r", branch="main", auto_sync=True, path="pipelines"
        )
        self.assertEqual(
            json.loads(self.last_request()["body"]),
            {
                "url": "https://github.com/o/r",
                "branch": "main",
                "auto_sync": True,
                "path": "pipelines",
            },
        )

    def test_list_git_repos_returns_the_registry_object(self):
        # `GET /api/git-repos` answers an object, not a bare list: the flags say
        # whether anything is running to sync it and whether a credential can be
        # stored at all.
        self.respond(
            "GET",
            "/api/git-repos",
            200,
            {"repos": [{"id": "g1"}], "worker_online": True, "credentials_configured": False},
        )
        repos = self.client.list_git_repos()
        self.assertEqual(repos["repos"][0]["id"], "g1")
        self.assertIs(repos["worker_online"], True)

    def test_connect_git_repo_omits_unset_path(self):
        self.respond("POST", "/api/git-repos", 201, {"id": "g-2"})
        self.client.connect_git_repo("https://github.com/o/r")
        self.assertNotIn("path", json.loads(self.last_request()["body"]))


# ── Client: error mapping + ops ───────────────────────────────────────────────


class ClientErrorTests(GatewayTestCase):
    def test_json_error_body_unwrapped(self):
        self.respond("POST", "/api/runs", 400, {"error": "DAG 'x' contains a cycle"})
        with self.assertRaises(DagronError) as cm:
            self.client.submit_run({"name": "x", "tasks": []})
        self.assertEqual(cm.exception.status, 400)
        self.assertEqual(cm.exception.message, "DAG 'x' contains a cycle")

    def test_plain_text_error_body(self):
        self.respond("PUT", "/api/workflows/wf-x", 404, "workflow 'wf-x' not found")
        with self.assertRaises(DagronError) as cm:
            self.client.update_workflow("wf-x", {"name": "x", "tasks": []})
        self.assertEqual(cm.exception.status, 404)
        self.assertEqual(cm.exception.message, "workflow 'wf-x' not found")

    def test_empty_error_body_falls_back_to_status(self):
        self.respond("GET", "/api/runs/r-x", 404, None)
        with self.assertRaises(DagronError) as cm:
            self.client.get_run("r-x")
        self.assertEqual(cm.exception.status, 404)
        self.assertIn("404", cm.exception.message)

    def test_metrics_and_healthz(self):
        self.respond("GET", "/api/metrics", 200, {"dead_letters": 0})
        self.assertEqual(self.client.metrics()["dead_letters"], 0)
        self.respond("GET", "/healthz", 200, "ok")
        self.assertEqual(self.client.healthz(), "ok")
        # healthz is unauthenticated — no bearer header attached.
        self.assertNotIn("Authorization", self.last_request()["headers"])


# ── Dag.submit end-to-end (the gateway contract bug-fix) ──────────────────────


class ClientEnvironmentConstructionTests(unittest.TestCase):
    def test_from_env_reads_url_and_token(self):
        with mock.patch.dict(
            os.environ, {"DAGRON_API_URL": "http://gw:8080/", "DAGRON_TOKEN": "dgp_abc"}
        ):
            api = Client.from_env()
        self.assertEqual(api.base_url, "http://gw:8080")
        self.assertEqual(api.token, "dgp_abc")

    def test_from_env_without_a_url_raises(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                Client.from_env()

    def test_from_env_without_a_token_is_anonymous(self):
        with mock.patch.dict(os.environ, {"DAGRON_API_URL": "http://gw:8080"}, clear=True):
            self.assertIsNone(Client.from_env().token)

    def test_context_manager_drops_the_token(self):
        with Client("http://gw:8080", token="tok") as api:
            self.assertEqual(api.token, "tok")
        self.assertIsNone(api.token)


class ClientTokenTests(GatewayTestCase):
    def test_create_list_and_revoke_tokens(self):
        self.respond("POST", "/api/tokens", 201, {"id": "t1", "token": "dgp_secret"})
        self.assertEqual(
            self.client.create_token("ci", expires_in_days=30)["token"], "dgp_secret"
        )
        self.assertEqual(
            json.loads(self.last_request()["body"]), {"name": "ci", "expires_in_days": 30}
        )

        self.respond("GET", "/api/tokens", 200, [{"id": "t1", "prefix": "dgp_abc"}])
        self.assertEqual(self.client.list_tokens()[0]["prefix"], "dgp_abc")

        self.respond("DELETE", "/api/tokens/t1", 204, None)
        self.assertIsNone(self.client.revoke_token("t1"))

    def test_create_token_omits_unset_expiry(self):
        self.respond("POST", "/api/tokens", 201, {"id": "t1"})
        self.client.create_token("ci")
        self.assertEqual(json.loads(self.last_request()["body"]), {"name": "ci"})

    def test_list_users(self):
        self.respond("GET", "/api/users", 200, [{"id": "u1", "email": "a@b.c"}])
        self.assertEqual(self.client.list_users()[0]["email"], "a@b.c")


class ClientRunControlTests(GatewayTestCase):
    def test_list_runs_sends_name_and_trigger_filters(self):
        self.respond("GET", "/api/runs", 200, [])
        self.client.list_runs(status="failed", name="etl", trigger="schedule")
        self.assertEqual(
            self.last_request()["query"],
            {"status": ["failed"], "name": ["etl"], "trigger": ["schedule"]},
        )

    def test_iter_runs_walks_pages_until_a_short_one(self):
        self.respond_each(
            "GET",
            "/api/runs",
            (200, [{"id": "r1"}, {"id": "r2"}]),
            (200, [{"id": "r3"}]),
        )
        runs = list(self.client.iter_runs(page_size=2, status="succeeded"))
        self.assertEqual([r["id"] for r in runs], ["r1", "r2", "r3"])
        offsets = [r["query"].get("offset") for r in self.server.requests]
        self.assertEqual(offsets, [["0"], ["2"]])
        # The filter rides along on every page, not just the first.
        self.assertEqual(self.server.requests[1]["query"]["status"], ["succeeded"])

    def test_iter_runs_refuses_the_pagination_it_drives(self):
        # Passing either would collide with the paging arguments and surface as
        # an opaque TypeError from inside the generator.
        for reserved in ("limit", "offset"):
            with self.assertRaises(TypeError):
                next(self.client.iter_runs(**{reserved: 10}))

    def test_wait_run_outlives_a_shorter_client_timeout(self):
        # The transport budget for this one call covers the server's wait, so a
        # short client timeout cannot abort the long poll mid-answer.
        slow = Client(self.base_url, token="tok", timeout=1)
        with mock.patch.object(Client, "_request", autospec=True) as req:
            req.return_value = {"finished": True}
            slow.wait_run("r1", timeout_secs=120)
        self.assertEqual(req.call_args.kwargs["timeout"], 125)  # 120s budget + margin

    def test_wait_run_never_shrinks_a_generous_client_timeout(self):
        roomy = Client(self.base_url, token="tok", timeout=900)
        with mock.patch.object(Client, "_request", autospec=True) as req:
            req.return_value = {"finished": True}
            roomy.wait_run("r1")
        self.assertEqual(req.call_args.kwargs["timeout"], 900)

    def test_wait_run_clamps_to_the_servers_own_bounds(self):
        # The server clamps to [1, 600]; sizing the transport off an unclamped
        # value would wait far past anything the gateway will honour.
        with mock.patch.object(Client, "_request", autospec=True) as req:
            req.return_value = {"finished": True}
            self.client.wait_run("r1", timeout_secs=99999)
        self.assertEqual(req.call_args.kwargs["timeout"], 605)

    def test_a_read_timeout_is_a_dagron_error_not_a_bare_timeout(self):
        # A read that times out after the connection is established comes out of
        # http.client as a socket timeout that URLError never sees.
        with mock.patch("dagron.urllib.request.urlopen", side_effect=TimeoutError()):
            with self.assertRaises(DagronError) as caught:
                self.client.get_run("r1")
        self.assertEqual(caught.exception.status, 0)
        self.assertIn("timed out", caught.exception.message)

    def test_get_run_spec(self):
        self.respond("GET", "/api/runs/r1/spec", 200, {"yaml": "name: etl\n", "name": "etl"})
        self.assertEqual(self.client.get_run_spec("r1")["name"], "etl")

    def test_wait_run_long_polls_with_a_server_budget(self):
        self.respond(
            "GET",
            "/api/runs/r1/wait",
            200,
            {"run_id": "r1", "status": "succeeded", "finished": True, "result": "42"},
        )
        result = self.client.wait_run("r1", timeout_secs=120)
        self.assertEqual(result["result"], "42")
        self.assertEqual(self.last_request()["query"], {"timeout_secs": ["120"]})

    def test_wait_run_without_a_budget_sends_no_query(self):
        self.respond("GET", "/api/runs/r1/wait", 200, {"finished": False})
        self.client.wait_run("r1")
        self.assertEqual(self.last_request()["query"], {})

    def test_clear_task(self):
        self.respond(
            "POST", "/api/runs/r1/tasks/t1/clear", 200, {"run_id": "r1", "task_id": "t1", "cleared": 3}
        )
        self.assertEqual(self.client.clear_task("r1", "t1")["cleared"], 3)

    def test_triage_set_and_clear(self):
        self.respond("POST", "/api/runs/r1/triage", 200, {"triage_state": "acknowledged"})
        self.client.set_triage("r1", "acknowledged", note="on it")
        self.assertEqual(
            json.loads(self.last_request()["body"]), {"state": "acknowledged", "note": "on it"}
        )
        self.respond("DELETE", "/api/runs/r1/triage", 200, {"triage_state": None})
        self.assertIsNone(self.client.clear_triage("r1")["triage_state"])

    def test_triage_omits_an_unset_note(self):
        self.respond("POST", "/api/runs/r1/triage", 200, {})
        self.client.set_triage("r1", "resolved")
        self.assertEqual(json.loads(self.last_request()["body"]), {"state": "resolved"})

    def test_archive_reads_and_write(self):
        self.respond("GET", "/api/archive/runs", 200, [{"run_id": "r1"}])
        self.client.list_archived_runs(name="etl", limit=10)
        self.assertEqual(self.last_request()["query"], {"name": ["etl"], "limit": ["10"]})

        self.respond("GET", "/api/archive/runs/r1", 200, {"run_id": "r1", "index": {}})
        self.assertEqual(self.client.get_archived_run("r1")["run_id"], "r1")

        self.respond("POST", "/api/runs/r1/archive", 200, {"archived": True})
        self.assertTrue(self.client.archive_run("r1")["archived"])

    def test_stream_events_reads_the_account_wide_feed(self):
        self.server.sse_body = 'event: task\ndata: {"run_id": "r1"}\n\n'
        events = list(self.client.stream_events(timeout=5))
        self.assertEqual(events, [{"event": "task", "data": {"run_id": "r1"}}])
        self.assertEqual(self.last_request()["path"], "/api/events/stream")


class ClientWorkflowLifecycleTests(GatewayTestCase):
    def test_list_workflows_filters_by_tag(self):
        self.respond("GET", "/api/workflows", 200, [])
        self.client.list_workflows(tag="nightly")
        self.assertEqual(self.last_request()["query"], {"tag": ["nightly"]})

    def test_run_workflow_passes_parameters(self):
        self.respond("POST", "/api/workflows/w1/run", 200, {"run_id": "r1"})
        self.client.run_workflow("w1", parameters={"day": "2026-01-01"})
        self.assertEqual(
            json.loads(self.last_request()["body"]), {"parameters": {"day": "2026-01-01"}}
        )

    def test_run_workflow_without_parameters_sends_no_body(self):
        self.respond("POST", "/api/workflows/w1/run", 200, {"run_id": "r1"})
        self.client.run_workflow("w1")
        self.assertEqual(self.last_request()["body"], "")

    def test_versions_state_and_runs(self):
        self.respond("GET", "/api/workflows/w1/versions", 200, [{"version": 2}])
        self.assertEqual(self.client.list_workflow_versions("w1")[0]["version"], 2)

        self.respond("POST", "/api/workflows/w1/state", 200, {"id": "w1", "state": "paused"})
        self.assertEqual(self.client.set_workflow_state("w1", "paused")["state"], "paused")
        self.assertEqual(json.loads(self.last_request()["body"]), {"state": "paused"})

        self.respond("GET", "/api/workflows/w1/runs", 200, [{"id": "r1"}])
        self.client.list_workflow_runs("w1", limit=5)
        self.assertEqual(self.last_request()["query"], {"limit": ["5"]})

    def test_apply_bundle_base64_encodes_every_binary_field(self):
        self.respond("POST", "/api/workflows/bundle", 200, {"applied": []})
        self.client.apply_bundle(b"manifest", b"sig", {"dags/etl.yaml": b"name: etl"})
        body = json.loads(self.last_request()["body"])
        self.assertEqual(base64.b64decode(body["manifest_b64"]), b"manifest")
        self.assertEqual(base64.b64decode(body["signature_b64"]), b"sig")
        self.assertEqual(body["files"][0]["path"], "dags/etl.yaml")
        self.assertEqual(base64.b64decode(body["files"][0]["content_b64"]), b"name: etl")

    def test_workflow_badge_returns_svg_unauthenticated(self):
        self.respond("GET", "/api/badges/etl", 200, "<svg/>")
        self.assertEqual(self.client.workflow_badge("etl"), "<svg/>")
        self.assertNotIn("authorization", {k.lower() for k in self.last_request()["headers"]})


class ClientScheduleTests(GatewayTestCase):
    def test_create_schedule_carries_the_whole_policy(self):
        self.respond("POST", "/api/schedules", 200, {"id": "s1"})
        self.client.create_schedule(
            "w1",
            "0 0 2 * * *",
            timezone="Europe/Berlin",
            when_expr="{{ day_of_week }} != 0",
            stop_expr="{{ done }}",
            catchup=True,
            catchup_window_secs=86400,
            catchup_max_runs=10,
        )
        self.assertEqual(
            json.loads(self.last_request()["body"]),
            {
                "workflow_id": "w1",
                "cron_expr": "0 0 2 * * *",
                "enabled": True,
                "timezone": "Europe/Berlin",
                "when_expr": "{{ day_of_week }} != 0",
                "stop_expr": "{{ done }}",
                "catchup": True,
                "catchup_window_secs": 86400,
                "catchup_max_runs": 10,
            },
        )

    def test_update_schedule_still_patches_only_given_fields(self):
        self.respond("PUT", "/api/schedules/s1", 200, {"id": "s1"})
        self.client.update_schedule("s1", timezone="UTC")
        self.assertEqual(json.loads(self.last_request()["body"]), {"timezone": "UTC"})


class ClientEnvironmentTests(GatewayTestCase):
    def test_create_update_and_delete(self):
        self.respond("POST", "/api/environments", 201, {"id": "e1"})
        self.client.create_environment("prod", variables={"BUCKET": "s3://x"}, description="live")
        self.assertEqual(
            json.loads(self.last_request()["body"]),
            {"name": "prod", "description": "live", "variables": {"BUCKET": "s3://x"}},
        )

        self.respond("PUT", "/api/environments/e1", 200, {"id": "e1"})
        self.client.update_environment("e1", variables={})
        # An explicit empty map is a real instruction ("no variables"), not an
        # omission, so it must survive to the wire.
        self.assertEqual(json.loads(self.last_request()["body"]), {"variables": {}})

        self.respond("DELETE", "/api/environments/e1", 204, None)
        self.assertIsNone(self.client.delete_environment("e1"))

    def test_secrets_are_write_only(self):
        self.respond("PUT", "/api/environments/e1/secrets/API_TOKEN", 204, None)
        self.client.set_environment_secret("e1", "API_TOKEN", "hunter2")
        self.assertEqual(json.loads(self.last_request()["body"]), {"value": "hunter2"})

        self.respond("DELETE", "/api/environments/e1/secrets/API_TOKEN", 204, None)
        self.assertIsNone(self.client.delete_environment_secret("e1", "API_TOKEN"))

    def test_list_environments(self):
        self.respond("GET", "/api/environments", 200, [{"id": "e1", "secret_names": ["T"]}])
        self.assertEqual(self.client.list_environments()[0]["secret_names"], ["T"])


class ClientSettingsAndDatasetTests(GatewayTestCase):
    def test_notification_settings_round_trip(self):
        self.respond("GET", "/api/settings/notifications", 200, {"slack_enabled": False})
        self.assertFalse(self.client.get_notification_settings()["slack_enabled"])

        self.respond("PUT", "/api/settings/notifications", 200, {"slack_enabled": True})
        self.client.set_notification_settings({"slack_enabled": True, "slack_on": ["failed"]})
        self.assertEqual(
            json.loads(self.last_request()["body"]),
            {"slack_enabled": True, "slack_on": ["failed"]},
        )

        self.respond("POST", "/api/settings/notifications/test", 200, {"slack": "ok"})
        self.assertEqual(self.client.test_notifications({"slack_enabled": True})["slack"], "ok")

    def test_dead_letter_settings(self):
        self.respond("GET", "/api/settings/dead-letters", 200, {"max_attempts": 3})
        self.assertEqual(self.client.get_dead_letter_settings()["max_attempts"], 3)

        self.respond("PUT", "/api/settings/dead-letters", 200, {"max_attempts": 5})
        self.client.set_dead_letter_settings(5)
        self.assertEqual(json.loads(self.last_request()["body"]), {"max_attempts": 5})

    def test_datasets_and_lineage(self):
        self.respond("GET", "/api/datasets", 200, [{"uri": "s3://bucket/raw"}])
        self.client.list_datasets(limit=10)
        self.assertEqual(self.last_request()["query"], {"limit": ["10"]})

        self.respond("GET", "/api/datasets/events", 200, [{"uri": "s3://bucket/raw"}])
        self.client.list_dataset_events(uri="s3://bucket/raw")
        self.assertEqual(self.last_request()["query"], {"uri": ["s3://bucket/raw"]})


class ClientGitAuthTests(GatewayTestCase):
    def test_set_and_clear_repo_credential(self):
        self.respond("PUT", "/api/git-repos/g1/auth", 200, {"auth_kind": "https"})
        self.client.set_git_repo_auth("g1", kind="https", username="git", token="ghp_x")
        self.assertEqual(
            json.loads(self.last_request()["body"]),
            {"kind": "https", "username": "git", "token": "ghp_x"},
        )

        self.respond("DELETE", "/api/git-repos/g1/auth", 204, None)
        self.assertIsNone(self.client.clear_git_repo_auth("g1"))

    def test_connect_can_carry_the_credential(self):
        self.respond("POST", "/api/git-repos", 201, {"id": "g1"})
        self.client.connect_git_repo("https://example.com/x.git", auth={"kind": "https", "token": "t"})
        self.assertEqual(
            json.loads(self.last_request()["body"])["auth"], {"kind": "https", "token": "t"}
        )


class ClientArtifactTests(GatewayTestCase):
    PATH = "/api/runs/r1/artifacts/extract/rows.csv"

    def test_put_sends_raw_bytes(self):
        self.respond("PUT", self.PATH, 201, "local://r1/extract/rows.csv")
        self.assertEqual(
            self.client.put_artifact("r1", "extract", "rows.csv", b"a,b\n1,2\n"),
            "local://r1/extract/rows.csv",
        )
        req = self.last_request()
        self.assertEqual(req["body"], "a,b\n1,2\n")
        self.assertEqual(req["headers"]["Content-Type"], "application/octet-stream")

    def test_put_sends_binary_unmangled(self):
        png = b"\x89PNG\r\n"
        self.respond("PUT", self.PATH, 201, "local://r1/extract/rows.csv")
        self.client.put_artifact("r1", "extract", "rows.csv", png)
        self.assertEqual(self.last_request()["raw_body"], png)

    def test_get_returns_undecoded_bytes(self):
        self.respond("GET", self.PATH, 200, b"\x89PNG\r\n")
        self.assertEqual(self.client.get_artifact("r1", "extract", "rows.csv"), b"\x89PNG\r\n")

    def test_exists_returns_a_bool(self):
        self.respond("GET", self.PATH + "/exists", 200, {"exists": False})
        self.assertIs(self.client.artifact_exists("r1", "extract", "rows.csv"), False)

    def test_sync(self):
        self.respond("POST", "/api/artifacts/sync", 200, {"moved": 4})
        self.assertEqual(self.client.sync_artifacts()["moved"], 4)


class ClientObservabilityTests(GatewayTestCase):
    def test_health_search_and_timeseries(self):
        self.respond("GET", "/api/health", 200, {"db": "ok", "active_runs": 2})
        self.assertEqual(self.client.health()["active_runs"], 2)

        self.respond("GET", "/readyz", 200, "ready")
        self.assertEqual(self.client.readyz(), "ready")

        self.respond("GET", "/api/search", 200, {"query": "etl", "runs": []})
        self.client.search("etl", limit=5)
        self.assertEqual(self.last_request()["query"], {"q": ["etl"], "limit": ["5"]})

        self.respond("GET", "/api/metrics/timeseries", 200, [{"day": "2026-01-01"}])
        self.client.metrics_timeseries(days=30, name="etl")
        self.assertEqual(self.last_request()["query"], {"days": ["30"], "name": ["etl"]})

    def test_approvals_worklist(self):
        self.respond("GET", "/api/approvals", 200, [{"run_id": "r1", "task_name": "gate"}])
        self.assertEqual(self.client.list_approvals()[0]["task_name"], "gate")


class DagSubmitTests(GatewayTestCase):
    def test_dag_submit_posts_yaml_field(self):
        self.respond("POST", "/api/runs", 201, {"run_id": "r-1"})
        dag = Dag("etl")
        dag.task("a", command=["true"])
        run_id = dag.submit(self.base_url, token="tok")
        self.assertEqual(run_id, "r-1")
        body = json.loads(self.last_request()["body"])
        self.assertIn("yaml", body)
        self.assertEqual(self.last_request()["headers"].get("Authorization"), "Bearer tok")


# ── Recipe: the content-addressed tag ─────────────────────────────────────────
#
# The tag is a function of the recipe, and the program that actually builds the
# image is a different program in a different language. If the two disagree by
# one byte, an author pins a task to an image no build will ever push and
# nothing fails until the run does — so these tests are a cross-language
# contract, not unit tests of an implementation detail.
#
# `recipe-vectors.json` is generated FROM the builder
# (ee/examples/image-build/sdk/gen-vectors.py); the builder's own test suite
# asserts against the same file. A change to either side that moves a tag turns
# both red.

VECTORS_PATH = Path(__file__).resolve().parent.parent / "recipe-vectors.json"


@unittest.skipUnless(
    VECTORS_PATH.exists(),
    f"{VECTORS_PATH} not found — run from a checkout, not an installed wheel",
)
class RecipeVectorTests(unittest.TestCase):
    """Every golden vector, tag and image reference alike."""

    @classmethod
    def setUpClass(cls) -> None:
        with open(VECTORS_PATH, encoding="utf-8") as f:
            cls.doc = json.load(f)

    def test_generator_version_matches(self):
        # Bumping this in one language and not the other would re-tag every
        # image on one side only, which is the same failure as a hash bug.
        self.assertEqual(BUILD_GENERATOR_VERSION, self.doc["generator_version"])

    def test_every_vector_tags_identically(self):
        prefix = self.doc["prefix_for_image_ref_with_prefix"]
        self.assertTrue(self.doc["vectors"], "vector file is empty")
        for v in self.doc["vectors"]:
            with self.subTest(vector=v["key"]):
                r = Recipe(**v["recipe"])
                self.assertEqual(r.tag(), v["tag"], v["why"])
                self.assertEqual(r.image_ref(), v["image_ref"])
                self.assertEqual(r.image_ref(prefix), v["image_ref_with_prefix"])

    def test_a_trailing_slash_on_the_prefix_is_not_a_different_image(self):
        v = self.doc["vectors"][0]
        r = Recipe(**v["recipe"])
        prefix = self.doc["prefix_for_image_ref_with_prefix"]
        self.assertEqual(r.image_ref(prefix + "/"), r.image_ref(prefix))


class RecipeCanonicalFormTests(unittest.TestCase):
    """The three encoder settings that silently re-tag everything if wrong."""

    def test_fields_are_in_declaration_order_not_alphabetical(self):
        r = Recipe("x", "alpine", apt=["a"], workdir="/w")
        # `base` before `apt` before `workdir` — alphabetical would put apt first.
        self.assertEqual(
            r.canonical_json(),
            '{"name":"x","base":"alpine","apt":["a"],"workdir":"/w"}',
        )

    def test_env_is_sorted_by_key(self):
        r = Recipe("x", "alpine", env={"Z": "1", "A": "2", "M": "3"})
        self.assertEqual(
            r.canonical_json(),
            '{"name":"x","base":"alpine","env":{"A":"2","M":"3","Z":"1"}}',
        )

    def test_non_ascii_is_raw_not_escaped(self):
        # Python's json.dumps escapes to \u00e9 by default; the builder does not.
        r = Recipe("x", "alpine", workdir="/héllo")
        self.assertIn("/héllo", r.canonical_json())
        self.assertNotIn("\\u00e9", r.canonical_json())

    def test_empty_and_false_fields_are_omitted_entirely(self):
        r = Recipe("x", "alpine", apt=[], pip=[], env={}, files=[], run=[],
                   keep_entrypoint=False)
        self.assertEqual(r.canonical_json(), '{"name":"x","base":"alpine"}')

    def test_an_explicit_none_is_the_same_recipe_as_an_absent_field(self):
        # Python got this right from the start (`is not None`); the TypeScript
        # SDK did not, and the two disagreed about the same recipe. Pinned here
        # so the Python side cannot drift into the same mistake.
        absent = Recipe("etl", "alpine", pip=["x==1"])
        explicit = Recipe(
            "etl", "alpine", pip=["x==1"],
            workdir=None, user=None, platform=None, dockerfile=None,
        )
        self.assertEqual(explicit.canonical_json(), absent.canonical_json())
        self.assertEqual(explicit.tag(), absent.tag())
        explicit.validate()

    def test_the_whitespace_rules_are_the_builders_not_pythons(self):
        # `str.isspace()` counts U+001C-U+001F and Rust's `char::is_whitespace`
        # does not, so isspace() refused recipes the builder accepts — an author
        # blocked from something legal, which is the mirror image of the usual
        # bug. Measured against the builder, one code point at a time.
        for sep in ("\x1c", "\x1d", "\x1e", "\x1f"):
            with self.subTest(sep=repr(sep)):
                Recipe("x", f"a{sep}b").validate()          # the builder accepts
        for ws in ("\xa0", "\u2028", "\u3000", "\x85"):
            with self.subTest(ws=repr(ws)):
                with self.assertRaises(ValueError):
                    Recipe("x", f"a{ws}b").validate()       # and refuses these

    def test_a_lone_surrogate_is_refused_by_name(self):
        # Representable in a Python str and not in UTF-8. Without this the
        # failure is a UnicodeEncodeError naming a byte offset — true, useless —
        # or, in JavaScript, a spec whose recipe the builder cannot parse at all.
        with self.assertRaises(ValueError) as ctx:
            Recipe("x", "alpine", files=[RecipeFile("/a", "lone\ud800")]).validate()
        self.assertIn("/a", str(ctx.exception), "the message must name the field")
        # A real astral character is a *pair* and is fine.
        Recipe("x", "alpine", files=[RecipeFile("/a", "\U0001f409")]).validate()

    def test_a_nul_is_refused_where_the_builder_refuses_one(self):
        with self.assertRaises(ValueError):
            Recipe("x", "alpine", env={"A": "a\0b"}).validate()
        with self.assertRaises(ValueError):
            Recipe("x", "alpine", run=["a\0b"]).validate()
        with self.assertRaises(ValueError):
            Recipe("x", "alpine", files=[RecipeFile("/a\0b", "c")]).validate()

    def test_a_trailing_newline_does_not_sneak_past_the_name_regex(self):
        # Python's `$` also matches just before a trailing newline, so `^...$`
        # accepted "etl\n" — a name the builder refuses.
        with self.assertRaises(ValueError):
            Recipe("etl\n", "alpine").validate()
        with self.assertRaises(ValueError):
            Recipe("x", "alpine", env={"A\n": "v"}).validate()

    def test_an_empty_string_is_not_an_absent_field(self):
        with_empty = Recipe("x", "alpine", user="")
        without = Recipe("x", "alpine")
        self.assertNotEqual(with_empty.tag(), without.tag())

    def test_list_order_is_part_of_the_identity(self):
        self.assertNotEqual(
            Recipe("x", "alpine", apt=["a", "b"]).tag(),
            Recipe("x", "alpine", apt=["b", "a"]).tag(),
        )

    def test_executable_false_is_omitted_but_true_is_not(self):
        plain = Recipe("x", "alpine", files=[RecipeFile("/a", "c")])
        explicit = Recipe("x", "alpine", files=[RecipeFile("/a", "c", executable=False)])
        exe = Recipe("x", "alpine", files=[RecipeFile("/a", "c", executable=True)])
        self.assertEqual(plain.tag(), explicit.tag())
        self.assertNotEqual(plain.tag(), exe.tag())

    def test_files_accept_plain_mappings(self):
        as_obj = Recipe("x", "alpine", files=[RecipeFile("/a", "c", executable=True)])
        as_map = Recipe("x", "alpine", files=[{"path": "/a", "content": "c", "executable": True}])
        self.assertEqual(as_obj.tag(), as_map.tag())

    def test_the_tag_is_sixteen_hex_after_the_prefix(self):
        tag = Recipe("x", "alpine").tag()
        self.assertRegex(tag, r"^r-[0-9a-f]{16}$")


class RecipeValidationTests(unittest.TestCase):
    """Refuse early what the builder would refuse late."""

    def test_name_must_be_a_single_lowercase_path_component(self):
        for bad in ["", "UPPER", "with/slash", "-leading", "trailing-", "sp ace"]:
            with self.subTest(name=bad):
                with self.assertRaises(ValueError):
                    Recipe(bad, "alpine").validate()
        Recipe("a.b_c-d9", "alpine").validate()  # legal

    def test_base_must_be_one_reference(self):
        for bad in ["", "   ", "two refs"]:
            with self.subTest(base=bad):
                with self.assertRaises(ValueError):
                    Recipe("x", bad).validate()

    def test_a_trailing_backslash_would_splice_two_dockerfile_lines(self):
        with self.assertRaises(ValueError):
            Recipe("x", "alpine", run=["echo hi \\"]).validate()

    def test_a_credential_in_an_env_url_is_refused(self):
        # ENV is baked into the image and echoed into the build log.
        with self.assertRaises(ValueError):
            Recipe("x", "alpine", env={"PIP_INDEX_URL": "https://u:p@mirror/simple"}).validate()
        # The same URL without user:password is fine.
        Recipe("x", "alpine", env={"PIP_INDEX_URL": "https://mirror/simple"}).validate()

    def test_env_keys_must_be_shell_identifiers(self):
        with self.assertRaises(ValueError):
            Recipe("x", "alpine", env={"not-a-key": "v"}).validate()

    def test_paths_must_be_absolute_and_clean(self):
        for bad in ["relative", "/a/../b", "/a//b", "/with space", "/a/."]:
            with self.subTest(path=bad):
                with self.assertRaises(ValueError):
                    Recipe("x", "alpine", files=[RecipeFile(bad, "c")]).validate()
        Recipe("x", "alpine", files=[RecipeFile("/a/b.py", "c")]).validate()

    def test_a_line_break_in_a_run_line_is_refused(self):
        with self.assertRaises(ValueError):
            Recipe("x", "alpine", run=["echo a\necho b"]).validate()

    # The rules below were found by differentially fuzzing this validate()
    # against the builder's: each one is a recipe the SDK used to accept and the
    # builder refuses, which meant the error arrived in a build log minutes
    # later instead of where the recipe was written.

    def test_a_name_longer_than_the_builder_allows_is_refused(self):
        Recipe("a" * 128, "alpine").validate()
        with self.assertRaises(ValueError):
            Recipe("a" * 129, "alpine").validate()

    def test_an_empty_entry_in_a_list_is_refused(self):
        for field in ("apt", "pip", "run"):
            for entry in ("", "   "):
                with self.subTest(field=field, entry=repr(entry)):
                    with self.assertRaises(ValueError):
                        Recipe("x", "alpine", **{field: [entry]}).validate()

    def test_a_backslash_in_a_package_specifier_is_refused_but_allowed_in_run(self):
        # `run` is the field for shell; a backslash in a package name is a habit
        # from somewhere else.
        for field in ("apt", "pip"):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    Recipe("x", "alpine", **{field: ["pkg\\x"]}).validate()
        Recipe("x", "alpine", run=["echo a\\x"]).validate()

    def test_an_installer_option_is_not_a_package(self):
        # Found by adversarially reviewing the MCP tool, which offers no shell
        # field and could still reach one: `apt` entries are argv words to
        # `apt-get install`, and `apt-get -o DPkg::Pre-Invoke::=<shell>` runs
        # that shell as root during the build. `pip --index-url` is the same
        # shape — not a shell, but every package then comes from elsewhere.
        # Quoting is not a defence; quoting is what makes `-o` a clean argument.
        for field in ("apt", "pip"):
            for entry in ("-o", "--index-url", "-e", "--config-settings=x"):
                with self.subTest(field=field, entry=entry):
                    with self.assertRaises(ValueError):
                        Recipe("x", "alpine", **{field: [entry]}).validate()
        # `run` is the field for shell, and says so.
        Recipe("x", "alpine", run=["apt-get -o Foo=bar install curl"]).validate()
        # A dash inside a package name is untouched.
        Recipe("x", "alpine", apt=["ca-certificates"], pip=["duckdb==1.1.3"]).validate()

    def test_a_credential_url_is_refused_in_a_package_list_too(self):
        for field in ("apt", "pip", "run"):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    Recipe("x", "alpine", **{field: ["https://u:p@mirror/x"]}).validate()

    def test_an_empty_user_or_platform_is_refused(self):
        with self.assertRaises(ValueError):
            Recipe("x", "alpine", user="").validate()
        with self.assertRaises(ValueError):
            Recipe("x", "alpine", user="two words").validate()
        with self.assertRaises(ValueError):
            Recipe("x", "alpine", platform="").validate()
        with self.assertRaises(ValueError):
            Recipe("x", "alpine", platform="two words").validate()

    def test_the_same_file_path_twice_is_refused(self):
        with self.assertRaises(ValueError):
            Recipe(
                "x", "alpine",
                files=[RecipeFile("/a", "one"), RecipeFile("/a", "two")],
            ).validate()
        # Two different paths are fine.
        Recipe("x", "alpine", files=[RecipeFile("/a", "one"), RecipeFile("/b", "two")]).validate()

    def test_a_trailing_backslash_is_refused_wherever_it_would_splice_a_line(self):
        for kwargs in (
            {"base": "alpine\\"},
            {"workdir": "/app\\"},
            {"user": "app\\"},
            {"files": [RecipeFile("/a\\", "x")]},
        ):
            with self.subTest(**{k: str(v) for k, v in kwargs.items()}):
                with self.assertRaises(ValueError):
                    Recipe("x", kwargs.pop("base", "alpine"), **kwargs).validate()


# ── The build task the SDK injects ────────────────────────────────────────────


class RecipeBuildInjectionTests(unittest.TestCase):
    def _spec(self, dag):
        return {t["name"]: t for t in dag.to_spec()["tasks"]}

    def test_a_recipe_image_adds_one_build_and_a_dependency(self):
        r = Recipe("etl", "python:3.12-slim", pip=["duckdb==1.1.3"])
        dag = Dag("nightly")
        dag.task("query", image=r, command=["python", "/app/q.py"])
        tasks = self._spec(dag)

        self.assertEqual(set(tasks), {"build-etl", "query"})
        self.assertEqual(tasks["query"]["depends_on"], ["build-etl"])
        # The reference is known here, before anything is built.
        self.assertEqual(tasks["query"]["docker_image"], r.image_ref())
        self.assertEqual(tasks["build-etl"]["command"], ["dagron-build"])
        self.assertEqual(tasks["build-etl"]["runner_class"], "build")
        self.assertEqual(tasks["build-etl"]["produces"], [f"oci://{r.image_ref()}"])

    def test_the_embedded_recipe_is_the_canonical_form(self):
        # Not the YAML someone typed: whitespace is what moved the hash last time.
        r = Recipe("etl", "alpine", files=[RecipeFile("/a", "x\n")])
        dag = Dag("w")
        dag.task("t", image=r, command=["true"])
        env = {e["name"]: e["value"] for e in self._spec(dag)["build-etl"]["env"]}
        self.assertEqual(env["DAGRON_BUILD_RECIPE"], r.canonical_json())
        # And it round-trips to the same tag.
        self.assertEqual(Recipe(**json.loads(env["DAGRON_BUILD_RECIPE"])).tag(), r.tag())

    def test_one_recipe_shared_by_several_tasks_is_built_once(self):
        r = Recipe("etl", "alpine")
        dag = Dag("w")
        a = dag.task("a", image=r, command=["true"])
        dag.task("b", image=r, command=["true"], depends_on=[a])
        dag.task("c", image=r, command=["true"])
        tasks = self._spec(dag)
        self.assertEqual(sum(1 for n in tasks if n.startswith("build-")), 1)
        for n in ("a", "b", "c"):
            self.assertIn("build-etl", tasks[n]["depends_on"])
        self.assertEqual(tasks["b"]["depends_on"], ["a", "build-etl"])

    def test_two_different_recipes_named_the_same_get_two_builds(self):
        one = Recipe("etl", "alpine:3.20")
        two = Recipe("etl", "alpine:3.19")
        dag = Dag("w")
        dag.task("a", image=one, command=["true"])
        dag.task("b", image=two, command=["true"])
        tasks = self._spec(dag)
        builds = sorted(n for n in tasks if n.startswith("build-"))
        self.assertEqual(len(builds), 2, builds)
        self.assertNotEqual(tasks["a"]["docker_image"], tasks["b"]["docker_image"])

    def test_a_repository_pins_both_halves_in_the_spec(self):
        # The pool has its own defaults for these; a task's env wins, so the
        # image the build pushes is the image the tasks reference even against a
        # pool configured for a different registry.
        r = Recipe("etl", "alpine")
        dag = Dag("w", image_repository="registry.example/ws-8f3a")
        dag.task("t", image=r, command=["true"])
        tasks = self._spec(dag)
        env = {e["name"]: e["value"] for e in tasks["build-etl"]["env"]}
        self.assertEqual(env["DAGRON_IMAGE_REPOSITORY"], "registry.example/ws-8f3a")
        self.assertEqual(env["DAGRON_BUILD_PUSH"], "1")
        self.assertEqual(
            tasks["t"]["docker_image"], "registry.example/ws-8f3a/etl:" + r.tag()
        )

    def test_no_repository_means_a_daemon_local_image_and_no_push(self):
        r = Recipe("etl", "alpine")
        dag = Dag("w")
        dag.task("t", image=r, command=["true"])
        env = {e["name"]: e["value"] for e in self._spec(dag)["build-etl"]["env"]}
        self.assertNotIn("DAGRON_BUILD_PUSH", env)
        self.assertNotIn("DAGRON_IMAGE_REPOSITORY", env)
        self.assertEqual(self._spec(dag)["t"]["docker_image"], "etl:" + r.tag())

    def test_to_dict_emits_the_declared_fields_in_the_declared_order(self):
        """`_RECIPE_FIELDS` is the declared canonical order, and the canonical
        form is hashed — so a field reordered in `to_dict` re-tags every image in
        every deployment, silently. This is what makes that tuple mean
        something."""
        from dagron import _RECIPE_FIELDS

        r = Recipe(
            "etl",
            "python:3.12-slim",
            apt=["curl"],
            pip=["duckdb==1.1.3"],
            env={"B": "2", "A": "1"},
            workdir="/app",
            files=[RecipeFile("/app/x.py", "print(1)")],
            run=["echo hi"],
            user="1000",
            keep_entrypoint=True,
            platform="linux/amd64",
            dockerfile="FROM scratch",
        )
        self.assertEqual(list(r.to_dict().keys()), list(_RECIPE_FIELDS))

    def test_a_recipe_inside_a_template_uses_the_dags_repository(self):
        """A template is a task set like any other, and its build settings are
        the DAG's. Without this the template kept the `_TaskSet` defaults, so the
        same recipe produced `etl:r-<tag>` inside a template and
        `registry.example/ws-8f3a/etl:r-<tag>` outside it -- one spec, two
        references, and the template's image never pushed."""
        r = Recipe("etl", "alpine")
        dag = Dag("w", image_repository="registry.example/ws-8f3a")
        tpl = dag.template("sub")
        tpl.task("t", image=r, command=["true"])
        spec = dag.to_spec()
        tasks = {t["name"]: t for t in spec["templates"][0]["tasks"]}
        env = {e["name"]: e["value"] for e in tasks["build-etl"]["env"]}
        self.assertEqual(env["DAGRON_IMAGE_REPOSITORY"], "registry.example/ws-8f3a")
        self.assertEqual(env["DAGRON_BUILD_PUSH"], "1")
        self.assertEqual(
            tasks["t"]["docker_image"], "registry.example/ws-8f3a/etl:" + r.tag()
        )
        # And it is the same reference the DAG itself would produce.
        dag.task("direct", image=r, command=["true"])
        top = {t["name"]: t for t in dag.to_spec()["tasks"]}
        self.assertEqual(top["direct"]["docker_image"], tasks["t"]["docker_image"])

    def test_a_build_task_never_shadows_an_authors_own_task(self):
        r = Recipe("etl", "alpine")
        dag = Dag("w")
        dag.task("build-etl", command=["true"])          # the author's, first
        dag.task("t", image=r, command=["true"])
        tasks = self._spec(dag)
        self.assertEqual(tasks["build-etl"]["command"], ["true"])
        injected = [n for n in tasks if n.startswith("build-etl-")]
        self.assertEqual(len(injected), 1, tasks)
        self.assertEqual(tasks["t"]["depends_on"], injected)

    def test_the_authors_name_wins_over_the_injected_one(self):
        # `task("build-etl", image=Recipe("etl", ...))` — the name the author
        # asked for is theirs; the injected task takes a disambiguated one. The
        # TypeScript SDK reserved the injected name first and threw "duplicate
        # task" on the author's own call, so the same program worked in one
        # language and failed in the other.
        dag = Dag("w")
        dag.task("build-etl", image=Recipe("etl", "alpine"), command=["true"])
        names = [t["name"] for t in dag.to_spec()["tasks"]]
        self.assertIn("build-etl", names)
        self.assertEqual(len(names), 2, names)
        author = next(t for t in dag.to_spec()["tasks"] if t["name"] == "build-etl")
        self.assertEqual(author["command"], ["true"], "the author's task was replaced")

    def test_a_rejected_recipe_does_not_reserve_the_task_name(self):
        # Validating after reserving left the name taken, so fixing the recipe
        # and calling again reported a duplicate the author never wrote.
        dag = Dag("w")
        with self.assertRaises(ValueError):
            dag.task("t", image=Recipe("BAD NAME", "alpine"), command=["true"])
        dag.task("t", image=Recipe("etl", "alpine"), command=["true"])
        self.assertEqual(
            sorted(t["name"] for t in dag.to_spec()["tasks"]), ["build-etl", "t"]
        )

    def test_the_build_runner_class_and_timeout_are_configurable(self):
        dag = Dag("w", build_runner_class="images", build_timeout_secs=120)
        dag.task("t", image=Recipe("etl", "alpine"), command=["true"])
        build = self._spec(dag)["build-etl"]
        self.assertEqual(build["runner_class"], "images")
        self.assertEqual(build["timeout_secs"], 120)

    def test_an_invalid_recipe_fails_where_it_was_written(self):
        dag = Dag("w")
        with self.assertRaises(ValueError):
            dag.task("t", image=Recipe("BAD NAME", "alpine"), command=["true"])

    def test_a_plain_string_image_still_works(self):
        dag = Dag("w")
        dag.task("t", image="alpine:3.20", command=["true"])
        tasks = self._spec(dag)
        self.assertEqual(set(tasks), {"t"})
        self.assertEqual(tasks["t"]["docker_image"], "alpine:3.20")

    def test_produces_is_emitted_when_given(self):
        dag = Dag("w")
        dag.task("t", command=["true"], produces=["s3://bucket/key"])
        self.assertEqual(self._spec(dag)["t"]["produces"], ["s3://bucket/key"])

    def test_the_injected_graph_is_valid_and_acyclic(self):
        r = Recipe("etl", "alpine")
        dag = Dag("w")
        a = dag.task("a", image=r, command=["true"])
        dag.task("b", image=r, command=["true"], depends_on=[a])
        dag.to_spec()  # runs the leaf/chain, unknown-dep and cycle checks


if __name__ == "__main__":
    unittest.main()
