"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { getMe, listWorkflows, listDeadLetters, logout, type Me } from "@/lib/dagron-api";

type IconName = "overview" | "workflows" | "runs" | "submit" | "gitops" | "dead" | "metrics";

interface NavItem {
  href: string;
  label: string;
  icon: IconName;
  /// Optional trailing badge: a numeric count or a status dot.
  badge?: "wfCount" | "deadCount" | "dot";
}

// Primary navigation, grouped like the design comp.
const MAIN: NavItem[] = [
  { href: "/overview", label: "Overview", icon: "overview" },
  { href: "/workflows", label: "Workflows", icon: "workflows", badge: "wfCount" },
  { href: "/runs", label: "Runs", icon: "runs" },
  { href: "/submit", label: "Submit", icon: "submit" },
  { href: "/gitops", label: "GitOps", icon: "gitops", badge: "dot" },
];
const OPS: NavItem[] = [
  { href: "/dead-letters", label: "Dead letters", icon: "dead", badge: "deadCount" },
  { href: "/metrics", label: "Metrics", icon: "metrics" },
];

export default function Sidebar() {
  const pathname = usePathname();
  const [me, setMe] = useState<Me | null>(null);
  const [wfCount, setWfCount] = useState<number | null>(null);
  const [deadCount, setDeadCount] = useState<number | null>(null);

  // Best-effort identity + badge counts; failures leave the chrome unadorned.
  useEffect(() => {
    let alive = true;
    getMe().then((m) => alive && setMe(m)).catch((e) => console.warn("Sidebar: getMe failed", e));
    listWorkflows()
      .then((w) => alive && setWfCount(w.length))
      .catch((e) => console.warn("Sidebar: listWorkflows failed", e));
    listDeadLetters()
      .then((d) => alive && setDeadCount(d.length))
      .catch((e) => console.warn("Sidebar: listDeadLetters failed", e));
    return () => {
      alive = false;
    };
  }, []);

  async function signOut() {
    await logout();
    window.location.assign("/");
  }

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
        {n.badge === "dot" && <span className="dy-navdot" />}
      </Link>
    );
  }

  const initial = (me?.name || me?.email || "?").trim().charAt(0).toUpperCase();

  return (
    <aside className="dy-side">
      <Link href="/overview" className="dy-brand-row" style={{ color: "var(--fg)" }}>
        <div className="dy-logo">
          <i />
          <i />
        </div>
        <div style={{ lineHeight: 1 }}>
          <div className="dy-brand-name" style={{ fontSize: 17 }}>
            dagron
          </div>
          <div className="dy-brand-sub">WORKFLOWS</div>
        </div>
      </Link>

      {MAIN.map(renderItem)}

      <div className="dy-navsection">OPERATIONS</div>
      {OPS.map(renderItem)}

      <div className="dy-side-foot">
        <div className="dy-status">
          <span className="dy-status-dot" />
          <div style={{ lineHeight: 1.3 }}>
            <div style={{ fontSize: 12, fontWeight: 600 }}>Scheduler live</div>
            <div className="mono" style={{ fontSize: 10, color: "var(--dim)" }}>
              engine ok
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
  }
}
