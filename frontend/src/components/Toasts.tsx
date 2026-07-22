"use client";

// Minimal toast system: `useToast()` returns a `toast(msg, kind?)` function;
// <ToastProvider> renders the stack bottom-right. No portal library — a fixed
// div is enough for the app shell.

import { createContext, useCallback, useContext, useRef, useState } from "react";

type Kind = "ok" | "error";

interface Toast {
  id: number;
  msg: string;
  kind: Kind;
}

const ToastCtx = createContext<(msg: string, kind?: Kind) => void>(() => {});

export function useToast() {
  return useContext(ToastCtx);
}

export default function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const nextId = useRef(1);

  const push = useCallback((msg: string, kind: Kind = "ok") => {
    const id = nextId.current++;
    setToasts((t) => [...t, { id, msg, kind }]);
    // Errors linger longer — they carry information the user must act on.
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), kind === "error" ? 8000 : 3500);
  }, []);

  return (
    <ToastCtx.Provider value={push}>
      {children}
      <div className="dy-toasts" aria-live="polite">
        {toasts.map((t) => (
          <div key={t.id} className={`dy-toast ${t.kind === "error" ? "dy-toast-error" : ""}`}>
            {t.kind === "error" ? "✕ " : "✓ "}
            {t.msg}
          </div>
        ))}
      </div>
    </ToastCtx.Provider>
  );
}
