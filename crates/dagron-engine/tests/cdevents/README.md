# CDEvents schemas

The JSON schemas of the two CDEvents the engine sends (`src/cdevents.rs`), as
the CDEvents specification publishes them at its tag `v0.5.1`:

- `pipelinerunfinished.json`: <https://github.com/cdevents/spec/blob/v0.5.1/schemas/pipelinerunfinished.json>
- `taskrunfinished.json`: <https://github.com/cdevents/spec/blob/v0.5.1/schemas/taskrunfinished.json>

They are copied unchanged, and are the specification's, under its licence
(Apache License 2.0, <https://github.com/cdevents/spec/blob/v0.5.1/LICENSE>).
The engine's tests check every event it builds against them.

v0.5.1 and not v0.5.0: the two define the same event types
(`dev.cdevents.*.0.3.0`), and v0.5.0's `pipelinerun-finished` schema requires a
`context.version` that the same schema forbids, so that no event can satisfy it.
