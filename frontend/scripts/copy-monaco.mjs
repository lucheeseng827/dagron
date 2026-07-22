// Air-gap: stage the Monaco editor runtime as same-origin static assets.
//
// @monaco-editor/react loads the Monaco AMD runtime from cdn.jsdelivr.net by
// default, so the YAML editor hangs on a spinner with no egress. We instead
// copy monaco-editor's `min/vs` into `public/monaco/vs` and point the loader at
// `/monaco/vs` (see src/lib/monaco.ts). Runs as a pre{dev,build} step; the
// output is git-ignored and regenerated from node_modules on every build.
import { cpSync, existsSync, mkdirSync, rmSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const src = resolve(here, "../node_modules/monaco-editor/min/vs");
const dest = resolve(here, "../public/monaco/vs");

if (!existsSync(src)) {
  console.error(
    `[copy-monaco] monaco-editor not found at ${src} — run \`npm ci\` first.`,
  );
  process.exit(1);
}

rmSync(dest, { recursive: true, force: true });
mkdirSync(dirname(dest), { recursive: true });
cpSync(src, dest, { recursive: true });
console.log(`[copy-monaco] staged ${dest} from ${src}`);
