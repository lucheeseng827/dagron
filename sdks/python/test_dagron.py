"""Tests for the dagron Python SDK — builder validation + a real-socket client.

Standard-library only. The :class:`Client` tests run against a threaded
``http.server`` fake gateway that records each request and returns canned
responses, so we exercise actual URL/method/header/body construction (not mocks).
Run with ``python -m unittest``.
"""

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from dagron import Client, Dag, DagronError


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
                "body": raw_body.decode("utf-8") if raw_body else "",
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
        if key not in self.server.responses:
            msg = f"unconfigured fake-gateway route: {self.command} {parsed.path}".encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)
            return
        status, payload = self.server.responses[key]
        body = b""
        self.send_response(status)
        if payload is None:
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if isinstance(payload, (dict, list)):
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
        self.respond("POST", "/api/workflows/wf-1/run", 200, {"run_id": "r-1", "workflow_id": "wf-1"})
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

    def test_dead_letters_and_git_repos(self):
        self.respond("GET", "/api/dead-letters", 200, [{"id": "dl-1"}])
        self.assertEqual(self.client.list_dead_letters(limit=5)[0]["id"], "dl-1")
        self.assertEqual(self.last_request()["query"], {"limit": ["5"]})

        self.respond("POST", "/api/git-repos", 201, {"id": "g-1"})
        self.client.connect_git_repo("https://github.com/o/r", branch="main", auto_sync=True)
        self.assertEqual(
            json.loads(self.last_request()["body"]),
            {"url": "https://github.com/o/r", "branch": "main", "auto_sync": True},
        )


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


if __name__ == "__main__":
    unittest.main()
