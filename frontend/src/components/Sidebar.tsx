"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { getHealth, getMe, listWorkflows, logout, type Me } from "@/lib/dagron-api";
import type { HealthResponse } from "@/types/dagron";

type IconName =
  | "overview"
  | "workflows"
  | "runs"
  | "submit"
  | "gitops"
  | "dead"
  | "metrics"
  | "approvals"
  | "backfills"
  | "users"
  | "audit"
  | "bell"
  | "search"
  | "envs";

interface NavItem {
  href: string;
  label: string;
  icon: IconName;
  /// Optional trailing badge: a numeric count or a status dot.
  badge?: "wfCount" | "deadCount" | "approvalCount" | "dot";
}

// Primary navigation, grouped like the design comp.
const MAIN: NavItem[] = [
  { href: "/overview", label: "Overview", icon: "overview" },
  { href: "/workflows", label: "Workflows", icon: "workflows", badge: "wfCount" },
  { href: "/runs", label: "Runs", icon: "runs" },
  { href: "/approvals", label: "Approvals", icon: "approvals", badge: "approvalCount" },
  { href: "/submit", label: "Submit", icon: "submit" },
  { href: "/gitops", label: "GitOps", icon: "gitops", badge: "dot" },
];
const OPS: NavItem[] = [
  { href: "/environments", label: "Environments", icon: "envs" },
  { href: "/backfills", label: "Backfills", icon: "backfills" },
  { href: "/dead-letters", label: "Dead letters", icon: "dead", badge: "deadCount" },
  { href: "/metrics", label: "Metrics", icon: "metrics" },
];
// Visible only to members of the `admin` group. Notification defaults live
// here because the stored webhook URLs are secrets and rerouting them affects
// every run (the API enforces the same admin gate).
const ADMIN: NavItem[] = [
  { href: "/settings/notifications", label: "Notifications", icon: "bell" },
  { href: "/settings/users", label: "Users", icon: "users" },
];
// Enterprise-build screens (health.edition === "enterprise"): the audit trail.
const ADMIN_EE: NavItem[] = [{ href: "/settings/audit", label: "Audit log", icon: "audit" }];

export default function Sidebar() {
  const pathname = usePathname();
  const [me, setMe] = useState<Me | null>(null);
  const [wfCount, setWfCount] = useState<number | null>(null);
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [healthErr, setHealthErr] = useState(false);

  // Best-effort identity + badge counts; failures leave the chrome unadorned.
  useEffect(() => {
    let alive = true;
    getMe().then((m) => alive && setMe(m)).catch((e) => console.warn("Sidebar: getMe failed", e));
    listWorkflows()
      .then((w) => alive && setWfCount(w.length))
      .catch((e) => console.warn("Sidebar: listWorkflows failed", e));
    // Health drives the status widget + attention badges; refresh every 30s.
    const loadHealth = () =>
      getHealth()
        .then((h) => {
          if (!alive) return;
          setHealth(h);
          setHealthErr(false);
        })
        .catch(() => alive && setHealthErr(true));
    loadHealth();
    const t = setInterval(loadHealth, 30_000);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, [pathname]);

  async function signOut() {
    await logout();
    window.location.assign("/");
  }

  const deadCount = health?.dead_letters ?? null;
  const approvalCount = health?.awaiting_approvals ?? null;

  function renderItem(n: NavItem) {
    const active = pathname === n.href || pathname.startsWith(`${n.href}/`);
    return (
      <Link key={n.href} href={n.href} className={`dy-navitem ${active ? "dy-navitem-active" : ""}`}>
        <NavIcon name={n.icon} />
        <span>{n.label}</span>
        {n.badge === "wfCount" && wfCount != null && <span className="dy-navcount">{wfCount}</span>}
        {n.badge === "deadCount" && deadCount != null && deadCount > 0 ? (
          <span className="dy-navcount dy-navcount-danger">{deadCount}</span>
        ) : null}
        {n.badge === "approvalCount" && approvalCount != null && approvalCount > 0 ? (
          <span className="dy-navcount" style={{ color: "#a371f7", fontWeight: 700 }}>
            {approvalCount}
          </span>
        ) : null}
        {n.badge === "dot" && <span className="dy-navdot" />}
      </Link>
    );
  }

  // Status widget: green = DB ok + a scheduler holds a fresh leadership lease;
  // amber = DB ok but no live scheduler (schedules will not fire); red = API/DB
  // unreachable. Driven by GET /api/health — not hardcoded.
  const isAdmin = me?.groups?.includes("admin") ?? false;
  const status = healthErr
    ? { color: "var(--red)", title: "API unreachable", sub: "health check failing" }
    : !health
      ? { color: "var(--dim)", title: "Checking…", sub: "contacting api" }
      : health.db !== "ok"
        ? { color: "var(--red)", title: "Database down", sub: "db unreachable" }
        : !health.scheduler_leader
          ? { color: "var(--amber)", title: "No scheduler", sub: "schedules won't fire" }
          : {
              color: "var(--green)",
              title: "Scheduler live",
              sub: `${health.active_runs} active run${health.active_runs === 1 ? "" : "s"}`,
            };

  const initial = (me?.name || me?.email || "?").trim().charAt(0).toUpperCase();

  return (
    <aside className="dy-side">
      <Link href="/overview" className="dy-brand-row" style={{ color: "var(--fg)" }}>
        {/* Same mark as the favicon (app/icon.svg): orange gradient tile + the
            two-tone double-chevron, so the tab icon and the brand logo match. */}
        <div className="dy-logo">
          <svg
            viewBox="0 0 64 64"
            width="34"
            height="34"
            fill="none"
            stroke="#1a1207"
            strokeWidth="5.5"
            strokeLinecap="round"
            strokeLinejoin="round"
            aria-hidden="true"
          >
            <polyline points="20,21 31,32 20,43" />
            <polyline points="33,21 44,32 33,43" strokeOpacity="0.5" />
          </svg>
        </div>
        <div style={{ lineHeight: 1 }}>
          <div className="dy-brand-name" style={{ fontSize: 17 }}>
            dagron
          </div>
          <div className="dy-brand-sub">
            WORKFLOWS
            {process.env.NEXT_PUBLIC_APP_VERSION ? (
              <span className="dy-brand-ver">v{process.env.NEXT_PUBLIC_APP_VERSION}</span>
            ) : null}
          </div>
        </div>
      </Link>

      {/* Global search — opens the ⌘K palette (also bound to Ctrl/Cmd-K). */}
      <button
        type="button"
        className="dy-navitem dy-search-btn"
        onClick={() => window.dispatchEvent(new Event("dagron:open-search"))}
      >
        <NavIcon name="search" />
        <span>Search</span>
        <kbd className="dy-kbd">⌘K</kbd>
      </button>

      {MAIN.map(renderItem)}

      <div className="dy-navsection">OPERATIONS</div>
      {OPS.map(renderItem)}

      {isAdmin && (
        <>
          <div className="dy-navsection">ADMIN</div>
          {ADMIN.map(renderItem)}
          {health?.edition === "enterprise" && ADMIN_EE.map(renderItem)}
        </>
      )}

      <div className="dy-side-foot">
        <div
          className="dy-status"
          title={
            health?.leader_holder
              ? `leader: ${health.leader_holder}`
              : "no leadership lease held"
          }
        >
          <span
            className="dy-status-dot"
            style={{ background: status.color, boxShadow: `0 0 8px ${status.color}` }}
          />
          <div style={{ lineHeight: 1.3 }}>
            <div style={{ fontSize: 12, fontWeight: 600 }}>{status.title}</div>
            <div className="mono" style={{ fontSize: 10, color: "var(--dim)" }}>
              {status.sub}
            </div>
          </div>
        </div>
        <button type="button" className="dy-user" onClick={() => void signOut()} title="Sign out">
          <div className="dy-avatar">{initial}</div>
          <div style={{ lineHeight: 1.25, minWidth: 0 }}>
            <div style={{ fontSize: 12, fontWeight: 600, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
              {me?.name || me?.email || "Account"}
            </div>
            <div style={{ fontSize: 10, color: "var(--dim)" }}>Sign out</div>
          </div>
        </button>
      </div>
    </aside>
  );
}

/// Inline feather-style icons (stroke = currentColor so they inherit nav state).
function NavIcon({ name }: { name: IconName }) {
  const p = {
    className: "dy-icon",
    viewBox: "0 0 24 24",
    fill: "none",
    stroke: "currentColor",
    strokeWidth: 1.8,
    strokeLinecap: "round" as const,
    strokeLinejoin: "round" as const,
  };
  switch (name) {
    case "overview":
      return (
        <svg {...p}>
          <rect x="3" y="3" width="7" height="7" rx="1" />
          <rect x="14" y="3" width="7" height="7" rx="1" />
          <rect x="3" y="14" width="7" height="7" rx="1" />
          <rect x="14" y="14" width="7" height="7" rx="1" />
        </svg>
      );
    case "workflows":
      return (
        <svg {...p}>
          <polygon points="12 2 2 7 12 12 22 7 12 2" />
          <polyline points="2 17 12 22 22 17" />
          <polyline points="2 12 12 17 22 12" />
        </svg>
      );
    case "runs":
      return (
        <svg {...p}>
          <circle cx="12" cy="12" r="9" />
          <polygon points="10 8 16 12 10 16 10 8" fill="currentColor" stroke="none" />
        </svg>
      );
    case "submit":
      return (
        <svg {...p}>
          <line x1="22" y1="2" x2="11" y2="13" />
          <polygon points="22 2 15 22 11 13 2 9 22 2" />
        </svg>
      );
    case "gitops":
      return (
        <svg {...p}>
          <circle cx="18" cy="6" r="3" />
          <circle cx="6" cy="18" r="3" />
          <path d="M18 9a9 9 0 0 1-9 9" />
        </svg>
      );
    case "dead":
      return (
        <svg {...p}>
          <path d="M22 12h-6l-2 3h-4l-2-3H2" />
          <path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z" />
        </svg>
      );
    case "metrics":
      return (
        <svg {...p}>
          <line x1="18" y1="20" x2="18" y2="10" />
          <line x1="12" y1="20" x2="12" y2="4" />
          <line x1="6" y1="20" x2="6" y2="14" />
        </svg>
      );
    case "approvals":
      return (
        <svg {...p}>
          <path d="M9 11l3 3L22 4" />
          <path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11" />
        </svg>
      );
    case "backfills":
      return (
        <svg {...p}>
          <polyline points="1 4 1 10 7 10" />
          <path d="M3.51 15a9 9 0 1 0 2.13-9.36L1 10" />
        </svg>
      );
    case "users":
      return (
        <svg {...p}>
          <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" />
          <circle cx="9" cy="7" r="4" />
          <path d="M23 21v-2a4 4 0 0 0-3-3.87" />
          <path d="M16 3.13a4 4 0 0 1 0 7.75" />
        </svg>
      );
    case "audit":
      return (
        <svg {...p}>
          <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
          <polyline points="14 2 14 8 20 8" />
          <line x1="16" y1="13" x2="8" y2="13" />
          <line x1="16" y1="17" x2="8" y2="17" />
        </svg>
      );
    case "bell":
      return (
        <svg {...p}>
          <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9" />
          <path d="M13.73 21a2 2 0 0 1-3.46 0" />
        </svg>
      );
    case "search":
      return (
        <svg {...p}>
          <circle cx="11" cy="11" r="8" />
          <line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
      );
    case "envs":
      return (
        <svg {...p}>
          <ellipse cx="12" cy="5" rx="9" ry="3" />
          <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
          <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
        </svg>
      );
  }
}
