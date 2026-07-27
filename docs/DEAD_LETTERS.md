# Working with the dead-letter queue

How to live with dead letters day to day: what lands there, how to inspect and
redrive, and how to set the retry policy. For the source-side design — the
`WorkflowSource::dead_letter` hook and broker-native routing — see
[`DLQ.md`](DLQ.md).

## What is in it (and what is not)

The dead-letter queue holds **submissions that never became runs**. A payload
arrived from a source, could not be turned into a workflow, and was parked so
ingestion could carry on instead of nack-looping on it forever.

Two ways a submission gets there:

| failure | behaviour |
|---|---|
| It does not parse into a DAG | Parked **immediately** — redelivering the same bytes can never succeed. |
| `create_run` fails (a DB blip, say) | Nacked and retried, then parked once it has failed the configured number of times. |

**A run that failed is not a dead letter.** It parsed, it started, tasks ran.
Its recovery lives on the run itself — rerun, resume from failure, or the
triage controls on the run page. The two are separate on purpose: a dead letter
has a payload and no run, a failed run has tasks and logs and no payload to
re-parse.

## Inspect and act

Every dead letter carries the original payload, the last error, which source
produced it, and how many times it failed.

**Console** — *Dead letters* in the sidebar. Expand a row to see the payload,
then redrive or discard.

**REST**

```bash
curl -s localhost:8080/api/dead-letters -b cookies.txt          # list
curl -s -X POST localhost:8080/api/dead-letters/<id>/redrive -b cookies.txt
curl -s -X DELETE localhost:8080/api/dead-letters/<id> -b cookies.txt
```

**Python SDK**

```python
from dagron import Dagron

d = Dagron("http://localhost:8080", token="dgp_…")
dead_letters = d.list_dead_letters(limit=50)
for dl in dead_letters:
    print(dl["source"], dl["failures"], dl["error"])

if dead_letters:
    first = dead_letters[0]
    d.redrive_dead_letter(first["id"])   # re-attempt as a fresh submission
    # d.discard_dead_letter(first["id"])  # ...or give up on it — pick one
```

**TypeScript SDK**

```js
import { Dagron } from "@dagron/sdk";

const d = new Dagron("http://localhost:8080", { token: "dgp_…" });
const parked = await d.listDeadLetters({ limit: 50 });
if (parked.length > 0) {
  await d.redriveDeadLetter(parked[0].id);   // re-attempt as a fresh submission
  // await d.discardDeadLetter(parked[0].id); // ...or give up on it — pick one
}
```

Both SDKs authenticate with a personal access token — see
[HOWTO §5](HOWTO.md#5-api-tokens-for-ci-and-scripts). Don't put a password in CI
to reach this queue.

**Redrive fixes nothing by itself.** It re-attempts the same payload, so it only
helps once the cause is gone: the schema was corrected, the dependency came
back, the bug was deployed. Redriving into an unfixed cause just parks it again
with the failure count one higher.

## Retry policy

How many times a submission is retried before parking.

`max_attempts` counts total delivery attempts, including the first one — it is
not the number of *retries*. `max_attempts: 1` parks on the first failure with
no retry at all; `max_attempts: 5` allows 4 retries (5 attempts total). The
minimum accepted value is `1`.

**Console** — *Dead letters* → **Retries before parking**. Takes effect on the
next ingestion failure; the engine reads this on the failure path rather than
caching it at startup, so there is no restart and no window where the page
disagrees with what is running.

**REST**

```bash
curl -s localhost:8080/api/settings/dead-letters -b cookies.txt
curl -s -X PUT localhost:8080/api/settings/dead-letters -b cookies.txt \
  -H 'content-type: application/json' -d '{"max_attempts":5}'
```

`0` is rejected — `{"max_attempts":0}` → `400 max_attempts must be at least 1, got 0`.
`PUT` requires an admin session.

Leave it unset and the engine uses its `DEAD_LETTER_MAX_ATTEMPTS` environment
value (default `3`), so an existing deployment behaves exactly as before until
someone overrides it deliberately.

Only this one knob is settable from the console. The other,
[`STREAM_DLQ_PATH`](CONFIG.md) — the NDJSON mirror written beside a file/FIFO
stream — is a filesystem path on the engine's own host, and a form that chooses
where a server process writes files is a footgun. It stays deployment
configuration.

### Choosing a number

- **Higher** suits a flaky dependency: more retries ride out a blip, at the cost
  of a poison payload occupying the ingestion path longer before it parks.
- **Lower** parks faster and keeps the queue moving, at the cost of parking
  submissions that a moment's patience would have admitted.
- It does **not** apply to parse failures. Those are deterministic and park on
  the first attempt whatever this is set to.

## Watching it

`GET /api/health` returns `dead_letters`, the current depth — the same number
the console's overview counts. A queue that grows steadily is a cause that has
not been fixed; the `source` and `error` fields on the rows say where to look.

## See also

- [`DLQ.md`](DLQ.md) — the source-side hook and broker-native routing design
- [`CONFIG.md`](CONFIG.md) — `DEAD_LETTER_MAX_ATTEMPTS`, `STREAM_DLQ_PATH`
- [`STREAMING.md`](STREAMING.md) — the file/FIFO source that produces most dead letters
- [`HOWTO.md`](HOWTO.md) — API tokens, so CI reaches this without a password
