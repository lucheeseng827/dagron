"""Shared harness primitives: load profiles, the open-loop rate schedule, latency
percentiles, and a uniform result document every driver emits.

Keeping these in one place is what makes the dagron, Airflow, and Argo runs
directly comparable — identical profile maths, identical percentile maths,
identical output schema (report.py only has to diff the JSON files)."""
import json
import math
import os
import time
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional


# ── Config ────────────────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    """Load profiles.yaml (or an override passed with --config)."""
    import yaml  # PyYAML; see requirements.txt
    with open(path) as fh:
        return yaml.safe_load(fh)


def weighted_fleet(fleet: List[dict]) -> List[str]:
    """Expand `[{workflow, weight}]` into a flat pick-list for random.choice."""
    out: List[str] = []
    for item in fleet:
        out.extend([item["workflow"]] * int(item.get("weight", 1)))
    return out or [f["workflow"] for f in fleet]


# ── Open-loop rate schedule ───────────────────────────────────────────────────

def target_rate(profile: dict, t: float) -> float:
    """Instantaneous submission rate (runs/sec) at elapsed time `t` for a profile.

    Open-loop on purpose: the generator submits at the scheduled rate regardless
    of whether the system keeps up, so a backlog (or 429 shedding) is *measured*
    rather than hidden by closed-loop self-throttling."""
    kind = profile["kind"]
    if kind == "sustained":
        return float(profile["rate_per_sec"])
    if kind == "ramp":
        dur = max(1.0, float(profile["duration_secs"]))
        frac = min(1.0, t / dur)
        return profile["start_rate"] + (profile["end_rate"] - profile["start_rate"]) * frac
    if kind == "spike":
        # Default period = 4× the burst, so a spike occupies 1/4 of each cycle
        # (burst = spike_secs, quiet = 3× spike_secs) when spike_every_secs is omitted.
        period = float(profile.get("spike_every_secs", profile["spike_secs"] * 4))
        in_window = (t % period) < float(profile["spike_secs"])
        return float(profile["spike_rate"] if in_window else profile["base_rate"])
    raise ValueError(f"unknown profile kind {kind!r}")


# ── Latency statistics ────────────────────────────────────────────────────────

def percentile(values: List[float], p: float) -> Optional[float]:
    """Linear-interpolated percentile. Returns None for an empty series.

    Every driver shares this so a p95 means the same thing on every plane.
    """
    if not values:
        return None
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    rank = (p / 100.0) * (len(s) - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return s[int(rank)]
    return s[lo] + (s[hi] - s[lo]) * (rank - lo)


def summarize(values: List[float], pcts: List[int]) -> dict:
    """count/min/max/mean plus the requested percentiles for one latency series."""
    if not values:
        return {"count": 0}
    out = {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "mean": sum(values) / len(values),
    }
    for p in pcts:
        out[f"p{p}"] = percentile(values, p)
    return out


# ── Errors ────────────────────────────────────────────────────────────────────

class ResponseParseError(Exception):
    """The server answered, but the body could not be parsed.

    Distinct from a transport failure, because the two mean opposite things about
    what happened on the server. A DNS/TLS/timeout error means the submission
    probably did not land; a 2xx with an unreadable body (a proxy error page, a
    truncated response) means it almost certainly DID, and a run is now executing
    that the harness may never poll.

    Carrying the status lets the record say so instead of claiming `status_code=0`
    — which would report a successful submission as a network failure and quietly
    depress the completed count, the headline metric.
    """

    def __init__(self, status: int):
        super().__init__(f"unparseable response body (HTTP {status})")
        self.status = status


# ── Result document (one per driver run) ──────────────────────────────────────

@dataclass
class RunRecord:
    """One submitted run: when it went out, whether it was accepted, how it ended.

    `status_code=0` marks a submission that never reached the server (DNS/TLS/
    timeout/decode) — it still belongs in `records`, because report() derives
    `submitted` and `accept_rate` from them.
    """

    workflow: str
    t_submit: float
    submit_latency_ms: float
    accepted: bool
    status_code: int
    run_id: Optional[str] = None
    t_complete: Optional[float] = None
    final_status: Optional[str] = None


@dataclass
class FleetResult:
    """All records from one driver run, plus the counters report() folds in."""

    engine: str            # "dagron" | "airflow" | "argo"
    profile: str
    started_at: float = field(default_factory=time.time)
    records: List[RunRecord] = field(default_factory=list)
    rejected: int = 0      # 429s (admission cap)
    errors: int = 0        # transport / 5xx

    def report(self, pcts: List[int]) -> dict:
        """The summary block every driver emits, in one shared shape.

        Throughput is completed runs over wall-clock from start to last
        completion — not over the submission window — so a plane that keeps
        draining after the window closes is credited for it.
        """
        accepted = [r for r in self.records if r.accepted]
        completed = [r for r in accepted if r.t_complete is not None]
        succeeded = [r for r in completed if r.final_status == "succeeded"]
        # EVERY record, including status_code=0 (never reached the server). Those
        # carry a real measured client-side latency — usually the slowest in the
        # run, because a DNS/TLS/timeout failure takes the full timeout to fail —
        # so filtering them out biases the percentiles LOW precisely when the
        # network is degraded and the numbers matter most. The old
        # `if r.status_code` guard was a no-op when nothing produced a 0 status;
        # it became a silent filter the moment the drivers started recording
        # transport failures.
        submit_lat = [r.submit_latency_ms for r in self.records]
        complete_lat = [
            (r.t_complete - r.t_submit) * 1000.0 for r in completed
        ]
        wall = (max((r.t_complete for r in completed), default=time.time()) - self.started_at)
        wall = max(wall, 1e-6)
        return {
            "engine": self.engine,
            "profile": self.profile,
            "submitted": len(self.records),
            "accepted": len(accepted),
            "rejected_429": self.rejected,
            "errors": self.errors,
            "completed": len(completed),
            "succeeded": len(succeeded),
            "failed": len(completed) - len(succeeded),
            "throughput_runs_per_sec": len(completed) / wall,
            "accept_rate": (len(accepted) / len(self.records)) if self.records else 0.0,
            "submit_latency_ms": summarize(submit_lat, pcts),
            "completion_latency_ms": summarize(complete_lat, pcts),
        }

    def write(self, out_dir: str, pcts: List[int]) -> str:
        """Write `<engine>_<profile>_<stamp>.json`; return the path."""
        os.makedirs(out_dir, exist_ok=True)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        path = os.path.join(out_dir, f"{self.engine}_{self.profile}_{stamp}.json")
        doc = {
            "summary": self.report(pcts),
            "records": [asdict(r) for r in self.records],
        }
        with open(path, "w") as fh:
            json.dump(doc, fh, indent=2)
        return path


def print_summary(summary: dict) -> None:
    """Human-readable summary for the console; report.py renders the tables."""
    print("\n" + "=" * 60)
    print(f"  {summary['engine'].upper()}  profile={summary['profile']}")
    print("=" * 60)
    for k in ("submitted", "accepted", "rejected_429", "errors",
              "completed", "succeeded", "failed"):
        print(f"  {k:<24} {summary[k]}")
    print(f"  {'throughput_runs_per_sec':<24} {summary['throughput_runs_per_sec']:.3f}")
    print(f"  {'accept_rate':<24} {summary['accept_rate']:.3f}")
    for series in ("submit_latency_ms", "completion_latency_ms"):
        s = summary[series]
        if s.get("count"):
            pcts = " ".join(f"{k}={v:.0f}" for k, v in s.items()
                            if k.startswith("p"))
            print(f"  {series:<24} n={s['count']} mean={s['mean']:.0f} {pcts}")
    print()
