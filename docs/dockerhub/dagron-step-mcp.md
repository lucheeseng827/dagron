# dagron MCP-tool step (`mancube/dagron-step-mcp`)

**A dagron task that calls one tool on an MCP server — so a DAG can drive an agent's tools with retries, timeouts, artifacts and an approval gate around each call.**

- **Image:** `mancube/dagron-step-mcp` — a Rust binary on **distroless/cc** (no shell, no package manager), runs as **nonroot** (uid 65532).
- **Arch:** `linux/amd64`, `linux/arm64`
- **Runtime:** spawns an MCP server as a child process and speaks stdio JSON-RPC (protocol `2024-11-05`) to it · **no ports**
- **Website:** dagron.dev · **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## This is a step, not a service

The mirror image of [`mancube/dagron-mcp`](https://hub.docker.com/r/mancube/dagron-mcp). That one is a **server**: it lets an agent drive dagron. This one is a **task**: it lets a dagron DAG drive an agent's tools.

Once a tool call is a task it inherits everything a task already has — retries with backoff, a timeout, captured output, artifacts between steps, an approval gate in front of it, and a place in the run's history. An in-process tool call has none of that.

So you usually do **not** deploy this image. You copy the binary into whatever image your task already uses — typically one that also carries the MCP server you want to call:

```dockerfile
FROM your-task-image:1.2.3
COPY --from=mancube/dagron-step-mcp:0.10 \
     /usr/local/bin/dagron-step-mcp /usr/local/bin/dagron-step-mcp
```

Running the image directly requires the server named by `DAGRON_MCP_STEP_SERVER` to exist in the image: the step *spawns* it as a child process, and this runtime carries only the step binary. So direct execution fails unless you have added the server — copy the binary into an image that has one instead.

## Configure it

Entirely by environment, like every other dagron step:

| Variable | Meaning |
|---|---|
| `DAGRON_MCP_STEP_SERVER` | server program to spawn (**required**) |
| `DAGRON_MCP_STEP_SERVER_ARGS` | JSON array of its arguments |
| `DAGRON_MCP_STEP_TOOL` | tool name to call (**required**) |
| `DAGRON_MCP_STEP_ARGS` | JSON object of tool arguments (default `{}`) |
| `DAGRON_MCP_STEP_ARGS_FILE` | read them from a file instead (`-` = stdin) |
| `DAGRON_MCP_STEP_OUTPUT` | write the result here instead of stdout |
| `DAGRON_MCP_STEP_TIMEOUT_SECS` | whole-exchange deadline (default 300) |

## Use it in a DAG

```yaml
tasks:
  - name: search
    docker_image: your-task-image:1.2.3   # with the binary COPY'd in
    command: ["dagron-step-mcp"]
    max_attempts: 3                        # the engine retries a flaky tool call
    retry_delay_secs: 5
    timeout_secs: 120                      # …and kills a hung one
    env:
      - { name: DAGRON_MCP_STEP_SERVER, value: "mcp-server-brave-search" }
      - { name: DAGRON_MCP_STEP_TOOL,   value: "brave_web_search" }
      - { name: DAGRON_MCP_STEP_ARGS,   value: '{"query": "dagron scheduler"}' }
      - { name: BRAVE_API_KEY, value_from: { secret: BRAVE_API_KEY } }
```

Secrets resolve at dispatch and are masked in task output.

## Tags

| Tag | Notes |
|---|---|
| `latest` | newest release |
| `0.10` | floating minor — newest `0.10.x` |

Pin in production — pick the newest published tag rather than copying a version from this page, which ages.

> First published in **0.10.0**. Earlier releases documented this step but shipped no image for it; build it from source with `podman build -f crates/dagron-step-mcp/Dockerfile .` if you are on one.

## See also

- **`docs/MCP.md`** — both directions of the integration, and the tool catalogue
- **`mancube/dagron-mcp`** — the server, for driving dagron *from* an agent
