// Air-gap: point @monaco-editor/react at the self-hosted Monaco runtime.
//
// By default the loader pulls the runtime from cdn.jsdelivr.net, which fails in
// a disconnected cluster and leaves the YAML editor stuck on a spinner. The
// pre{dev,build} step (scripts/copy-monaco.mjs) stages monaco's `min/vs` under
// public/monaco/vs, served same-origin by the Next server; here we tell the
// loader to use it. `NEXT_PUBLIC_MONACO_VS` allows a non-root base path.
//
// Import this module for its side effect (`import "@/lib/monaco"`) before the
// first <Editor> mounts. It is a no-op on the server and runs once.
import { loader } from "@monaco-editor/react";

const vs = process.env.NEXT_PUBLIC_MONACO_VS || "/monaco/vs";
loader.config({ paths: { vs } });

export {};
