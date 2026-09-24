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

/// The message to show for a failed response.
///
/// `docs/API.md` says handlers answer `{"error": "<message>"}`, and some do — the workflow
/// refusals, the viewer read-only gate, `archive.rs`, the 404 fallback. Most still answer a
/// plain-text body (issue #1196 tracks the sweep). Rendering the raw body covers the second
/// case and turns the first into `403: {"error":"..."}` in front of the user, so read the
/// envelope when it is there and fall back to the text when it is not.
///
/// Parsed from text rather than `res.json()`: the fallback needs the body either way, and a
/// response can only be read once.
export async function errorBody(res: Response): Promise<string> {
  const text = await res.text().catch(() => "");
  if (text) {
    try {
      const parsed: unknown = JSON.parse(text);
      if (parsed && typeof parsed === "object" && "error" in parsed) {
        const e = (parsed as { error?: unknown }).error;
        if (typeof e === "string" && e) return e;
      }
    } catch {
      // Not JSON: a plain-text handler, which is still the majority.
    }
  }
  return text || res.statusText;
}
