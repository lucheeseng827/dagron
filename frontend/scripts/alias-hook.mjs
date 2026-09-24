// Resolve the `@/*` path alias for scripts run under plain Node.
//
// `tsconfig.json` maps `@/*` to `src/*`, which Next understands and Node does
// not. The check scripts in this directory import the real modules rather than
// copies, so they need the same mapping — this is the whole of it: rewrite the
// specifier and hand it back to the default resolver, which then finds the
// `.ts` file and strips its types.

const SRC = new URL("../src/", import.meta.url);

/// `async` and `await`ing each attempt, both load-bearing: resolve hooks are
/// asynchronous, so `next()` returns a promise and a failed candidate rejects
/// rather than throwing. Without the `await` the `try` catches nothing, the
/// loop returns the first (still pending, later rejecting) promise, and the
/// fallbacks below are dead code that has never run.
export async function resolve(specifier, context, next) {
  if (!specifier.startsWith("@/")) return next(specifier, context);
  const base = new URL(specifier.slice(2), SRC);
  // Extensionless, the way TypeScript imports are written. Try `.ts`, then
  // `.tsx`; failing both, hand the bare path to the default resolver so its
  // error is the one the caller sees — it names the file that is missing.
  for (const ext of [".ts", ".tsx"]) {
    try {
      return await next(base.href + ext, context);
    } catch {
      // Next candidate.
    }
  }
  return next(base.href, context);
}
