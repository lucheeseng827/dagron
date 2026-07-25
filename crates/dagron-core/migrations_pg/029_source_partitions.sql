-- Per-partition range leases for multi-consumer streaming sources: N engines
-- split one logical stream (files in a shard dir, broker partitions, CDC
-- shards) by leasing partitions — one consumer per partition at a time, the
-- same lease/reclaim shape as task claims. Committed positions stay in
-- source_offsets, namespaced "<source>/<partition>", so the exactly-once
-- transaction path is untouched.
CREATE TABLE IF NOT EXISTS source_partitions (
    source_name      TEXT NOT NULL,
    partition        TEXT NOT NULL,
    claimed_by       TEXT,
    lease_expires_at TEXT,
    updated_at       TEXT NOT NULL,
    PRIMARY KEY (source_name, partition)
);
