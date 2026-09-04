#!/usr/bin/env python3
"""dagron fleet load driver.

Open-loop generator that submits the ETL fleet to dagron's `POST /runs` at a
profile-driven rate, honours the admission cap (HTTP 429 → backoff), and records
throughput + submit/completion latency percentiles. This is the productionized
replacement for the `dagron-load.sh` flood referenced in docs/LOADTEST.md:
realistic DAGs, configurable profiles, and latency — not just counters.

Usage:
  python3 run_fleet.py --profile sustained
  python3 run_fleet.py --profile spike --base-url http://dagron.dagron.svc:8080
  python3 run_fleet.py --profile ramp --image <ACCT>.dkr.ecr.us-east-1.amazonaws.com/dagron-etl-task:latest

Stdlib + PyYAML only (no requests) so it runs anywhere kubectl port-forward does.
"""
import argparse
import os
import random
import re
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import _common as C

HERE = os.path.dirname(os.path.abspath(__file__))


def http_post(url: str, body: bytes, timeout: float = 30.0):
    """POST a YAML workflow body to the dagron API."""
    req = urllib.request.Request(url, data=body, method="POST",
                                 headers={"Content-Type": "application/yaml"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read(), dict(resp.headers)


def http_get(url: str, timeout: float = 30.0):
    """GET a dagron API endpoint."""
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.status, resp.read()


def rewrite_image(yaml_text: str, image: str) -> str:
    """Point every task at `image` without a full YAML round-trip (preserves the
    file's comments/ordering)."""
    # NOTE: `\g<1>`, not `\1` — an ECR image ref starts with the account id, so
    # `\1` + "734…" would be read as the escape `\1734…` (Python parses `\173`
    # as the octal char `{`), silently corrupting the line. `\g<1>` is unambiguous.
    return re.sub(r"(?m)^(\s*docker_image:\s*).*$", rf"\g<1>{image}", yaml_text)


class Driver:
    """Open-loop load generator for dagron.

    The sibling of airflow_trigger.AirflowDriver and argo_submit.ArgoDriver:
    same profile maths, same result schema, different control plane.
    """

    def __init__(self, cfg: dict, profile_name: str, args):
        """Resolve config, then pre-load and image-rewrite every fleet workflow body."""
        self.cfg = cfg
        self.profile_name = profile_name
        self.profile = cfg["profiles"][profile_name]
        self.base_url = (args.base_url or cfg["dagron"]["base_url"]).rstrip("/")
        self.image = args.image or cfg["dagron"].get("image")
        wf_dir = os.path.normpath(os.path.join(HERE, cfg["dagron"]["workflows_dir"]))
        self.fleet = C.weighted_fleet(cfg["fleet"])
        self.bodies = {}
        for wf in set(self.fleet):
            with open(os.path.join(wf_dir, wf)) as fh:
                text = fh.read()
            self.bodies[wf] = rewrite_image(text, self.image) if self.image else text

        self.result = C.FleetResult(engine="dagron", profile=profile_name)
        self.lock = threading.Lock()
        self.outstanding = {}   # run_id -> (workflow, t_submit)
        self.stop = threading.Event()

    # ── submit path ──────────────────────────────────────────────────────────
    def submit_one(self):
        """POST one run and record the submit outcome, latency, and 429 shedding."""
        wf = random.choice(self.fleet)
        body = self.bodies[wf].encode()
        t0 = time.time()
        try:
            status, payload, _ = http_post(f"{self.base_url}/runs", body)
            lat = (time.time() - t0) * 1000.0
            run_id = None
            if status in (200, 201):
                import json
                try:
                    run_id = json.loads(payload).get("run_id")
                except (ValueError, AttributeError) as e:
                    # The server accepted the run; we just cannot read its id.
                    # Not a transport failure — see _common.ResponseParseError.
                    raise C.ResponseParseError(status) from e
            rec = C.RunRecord(workflow=wf, t_submit=t0, submit_latency_ms=lat,
                              accepted=run_id is not None, status_code=status,
                              run_id=run_id)
            with self.lock:
                self.result.records.append(rec)
                if run_id:
                    self.outstanding[run_id] = rec
        except urllib.error.HTTPError as e:
            lat = (time.time() - t0) * 1000.0
            rec = C.RunRecord(workflow=wf, t_submit=t0, submit_latency_ms=lat,
                              accepted=False, status_code=e.code)
            with self.lock:
                self.result.records.append(rec)
                if e.code == 429:
                    self.result.rejected += 1   # admission cap working as intended
                else:
                    self.result.errors += 1
        except C.ResponseParseError as e:
            # This path is only reachable after a 200/201, so the submission WAS
            # accepted — `accepted=True` and the real status. Marking it rejected
            # would understate accept_rate, which is specifically a measure of
            # admission control (dagron's 429 shedding); losing the run id is a
            # client-side tracking failure, not the control plane refusing work.
            #
            # dagron assigns the run_id, so we have no handle on a run that IS
            # executing: it never enters `outstanding` and never completes. The
            # resulting `completed < accepted` gap is the honest picture —
            # accepted, then unobservable — and the warning says why.
            lat = (time.time() - t0) * 1000.0
            rec = C.RunRecord(workflow=wf, t_submit=t0, submit_latency_ms=lat,
                              accepted=True, status_code=e.status)
            with self.lock:
                self.result.records.append(rec)
                self.result.errors += 1
                if self.result.errors % 20 == 1:
                    print(f"unparseable response to an accepted POST /runs "
                          f"(HTTP {e.status}) — a run is executing unpolled",
                          file=__import__("sys").stderr)
        except Exception:
            # Record the failure as well as counting it. FleetResult.report()
            # derives `submitted` and `accept_rate` from `records`, so a DNS/TLS/
            # timeout/decode failure that only bumps `errors` silently leaves the
            # denominator — and a run that half failed to submit reports a 100%
            # accept rate. status_code=0 marks "never reached the server".
            #
            # Applied identically to all three drivers on purpose: error
            # accounting has to mean the same thing on every plane or the
            # comparison is measuring the harness, not the control plane.
            lat = (time.time() - t0) * 1000.0
            rec = C.RunRecord(workflow=wf, t_submit=t0, submit_latency_ms=lat,
                              accepted=False, status_code=0)
            with self.lock:
                self.result.records.append(rec)
                self.result.errors += 1

    # ── poll path ────────────────────────────────────────────────────────────
    def poll_loop(self):
        """Poll outstanding runs once a second until each reaches a terminal status."""
        import json
        import sys
        poll_errors = 0
        while not self.stop.is_set() or self.outstanding:
            with self.lock:
                ids = list(self.outstanding.keys())
            for run_id in ids:
                try:
                    _status, payload = http_get(f"{self.base_url}/runs/{run_id}")
                    run = json.loads(payload).get("run", {})
                    st = run.get("status")
                    if st in ("succeeded", "failed", "cancelled"):
                        with self.lock:
                            rec = self.outstanding.pop(run_id, None)
                        if rec:
                            rec.t_complete = time.time()
                            rec.final_status = st
                except Exception as e:  # transient network / malformed response
                    poll_errors += 1
                    if poll_errors % 20 == 1:  # throttle: first + every 20th
                        print(f"poll error (count={poll_errors}): {e}", file=sys.stderr)
            time.sleep(1.0)

    # ── generator ────────────────────────────────────────────────────────────
    def preflight_backlog(self):
        """Count runs already active before this measurement starts.

        Finding 9 of THREE-WAY-RESULTS.md: a residual backlog from a previous run
        silently produced `completed: 0, throughput: 0.000` on a *working* system.
        The failure is invisible in the result JSON — the numbers look like an
        engine that cannot schedule, not like a dirty datastore — and at hyper
        scale a wasted run is a wasted cluster-hour, so this is cheap insurance.

        Returns the active-run count, or None when the backlog cannot be
        determined (an unreachable or differently-shaped API is not itself a
        reason to refuse to run).
        """
        import json
        total = 0
        for status in ("pending", "running"):
            try:
                _s, raw = http_get(f"{self.base_url}/runs?status={status}&limit=100")
                # http_get returns the RAW body; type-checking the bytes without
                # decoding them is always false, which silently disabled this
                # guard entirely.
                payload = json.loads(raw or b"[]")
            except Exception:
                return None
            # The engine answers `{"runs": [...]}`; earlier builds answered a bare
            # array. Accept both — the alternative is the guard silently returning
            # None against the current engine, which is exactly the "looks like it
            # ran clean" failure Finding 9 is about.
            if isinstance(payload, dict):
                payload = payload.get("runs")
            if not isinstance(payload, list):
                return None
            total += len(payload)
        return total

    def run(self, drain_timeout: float = 300.0, allow_dirty_queue: bool = False):
        """Drive the profile's submission window, drain, then write the result JSON."""
        backlog = self.preflight_backlog()
        if backlog is None:
            print("note: could not read the active-run backlog — proceeding unchecked "
                  "(see Finding 9 if the result shows completed: 0)")
        elif backlog > 0:
            msg = (f"{backlog} run(s) already active on {self.base_url} before this "
                   f"measurement starts")
            if not allow_dirty_queue:
                raise SystemExit(
                    f"refusing to start: {msg}.\n"
                    "  A residual backlog reports as completed: 0 / throughput: 0.000 —\n"
                    "  a result that looks like a broken engine rather than a dirty queue\n"
                    "  (THREE-WAY-RESULTS.md, Finding 9).\n"
                    "  Reset the datastore, or pass --allow-dirty-queue if this is deliberate.")
            print(f"warning: {msg} — continuing because --allow-dirty-queue was given; "
                  f"this run is NOT comparable to a clean-start measurement")

        poller = threading.Thread(target=self.poll_loop, daemon=True)
        poller.start()
        dur = float(self.profile["duration_secs"])
        print(f"→ dagron fleet: profile={self.profile_name} duration={dur:.0f}s "
              f"target={self.base_url}")
        start = time.time()
        credit = 0.0
        last = start
        with ThreadPoolExecutor(max_workers=64) as pool:
            while True:
                now = time.time()
                t = now - start
                if t >= dur:
                    break
                rate = C.target_rate(self.profile, t)
                credit += rate * (now - last)
                last = now
                while credit >= 1.0:
                    pool.submit(self.submit_one)
                    credit -= 1.0
                time.sleep(0.02)
        self.stop.set()
        print(f"→ submission window closed; draining (≤{drain_timeout:.0f}s)…")
        deadline = time.time() + drain_timeout
        while time.time() < deadline:
            with self.lock:
                if not self.outstanding:
                    break
            time.sleep(1.0)
        poller.join(timeout=5.0)

        pcts = self.cfg.get("report", {}).get("percentiles", [50, 90, 95, 99])
        out_dir = os.path.normpath(os.path.join(
            HERE, self.cfg.get("report", {}).get("out_dir", "../../results")))
        path = self.result.write(out_dir, pcts)
        C.print_summary(self.result.report(pcts))
        print(f"  result → {path}")


def main():
    """CLI entry point."""
    ap = argparse.ArgumentParser(description="dagron fleet load driver")
    ap.add_argument("--config", default=os.path.join(HERE, "profiles.yaml"))
    ap.add_argument("--profile", default="sustained")
    ap.add_argument("--base-url", default=None)
    ap.add_argument("--image", default=None,
                    help="override docker_image on every submitted task")
    ap.add_argument("--drain-timeout", type=float, default=300.0)
    ap.add_argument("--allow-dirty-queue", action="store_true",
                    help="start even when runs are already active (Finding 9). The "
                         "result will not be comparable to a clean-start measurement")
    args = ap.parse_args()

    cfg = C.load_config(args.config)
    if args.profile not in cfg["profiles"]:
        ap.error(f"unknown profile {args.profile!r}; choices: {list(cfg['profiles'])}")
    Driver(cfg, args.profile, args).run(args.drain_timeout, args.allow_dirty_queue)


if __name__ == "__main__":
    main()
