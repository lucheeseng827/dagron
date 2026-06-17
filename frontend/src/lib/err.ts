/// Normalize an unknown thrown value to a safe, human-readable message —
/// avoids shipping "[object Object]" or leaking internals to the UI.
export function errMsg(e: unknown): string {
  if (e instanceof Error) return e.message;
  if (typeof e === "string") return e;
  if (e && typeof e === "object" && "message" in e) {
    const m = (e as { message?: unknown }).message;
    if (typeof m === "string") return m;
  }
  return "Something went wrong.";
}
