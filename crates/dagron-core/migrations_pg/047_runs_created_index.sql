-- no-transaction
-- Read-path index for the API edge (docs/LOW_LATENCY.md R-5, executing
-- docs/SCALE_ROADMAP.md §0.1 for the shapes dagron-api actually queries):
-- `GET /api/runs` sorts every page by `wr.created_at DESC`; without this the
-- default runs page is a full scan plus a top-N sort of `workflow_runs`,
-- growing linearly with history (`created_at` is TEXT holding RFC-3339, which
-- orders correctly as text — the type migration rides the partitioning work,
-- not this index). CONCURRENTLY (which is why this is its own no-transaction,
-- single-statement migration — see 022/023) so building on a populated
-- history never blocks the live claim/mark writers. If a concurrent build is
-- interrupted it can leave an INVALID index that IF NOT EXISTS then skips —
-- drop the invalid index and re-run migrations to rebuild (standard
-- CONCURRENTLY care, same as 023).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_runs_created
    ON workflow_runs (created_at DESC);
