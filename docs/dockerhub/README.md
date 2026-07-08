# Docker Hub overviews for the dagron images + chart

Source for the **"full description"** shown on each public Docker Hub repo (paste the matching file
into the repo's description, or sync it). Keep these in step with releases.

| Docker Hub repo | File | What |
|---|---|---|
| [`mancube/dagron-engine`](https://hub.docker.com/r/mancube/dagron-engine) | [`dagron-engine.md`](./dagron-engine.md) | the workflow/DAG engine |
| [`mancube/dagron-api`](https://hub.docker.com/r/mancube/dagron-api) | [`dagron-api.md`](./dagron-api.md) | auth + management API |
| [`mancube/dagron-frontend`](https://hub.docker.com/r/mancube/dagron-frontend) | [`dagron-frontend.md`](./dagron-frontend.md) | Next.js operator console |
| `oci://registry-1.docker.io/mancube/dagron` | [`dagron-chart.md`](./dagron-chart.md) | Helm chart (the full stack) |

All three images are published **`linux/amd64` + `linux/arm64`** at `0.3.0` + `latest`.
