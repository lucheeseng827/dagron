# dagron frontend

Next.js 15 / React 19 UI for the dagron workflow engine. Talks to `dagron-api`
(the authenticated edge) via the `/api/*` rewrite.

## Scope

The dagron workflow UI, over `dagron-api`:

| Area | Notes |
|---|---|
| Auth / session | self-contained email/password login → HttpOnly `dagron_session` cookie; `AuthGuard` probes `/api/me`. No external IdP. |
| Run board / DAG graph / run detail | the core dagron views |
| Submit / cancel / retry / dead-letters / metrics | over `dagron-api` |
| Workflows-as-first-class, editor, schedule, GitOps | the visual workflow surface |

## Config

Copy `.env.example` → `.env.local`:

| Var | Purpose |
|---|---|
| `DAGRON_API_URL` | dagron-api base for the `/api/*` rewrite (default `http://localhost:8080`) |

## Run

```bash
npm install
npm run dev   # http://localhost:3000
```

Needs `dagron-api` running for data (see `../dagron-api/README.md`).
