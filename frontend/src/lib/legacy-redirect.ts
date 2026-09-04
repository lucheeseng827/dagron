// Send pre-0.9 detail-page bookmarks to where those pages live now.
//
// The console used to route on dynamic segments (`/runs/<id>`). A statically
// exported site cannot: Next resolves dynamic segments at build time via
// generateStaticParams, and a run id is not knowable then — so those pages moved
// to query parameters (`/runs/detail/?id=<id>`).
//
// dagron-api serves any unexported path by falling back to `index.html`, so an old
// bookmark does load the app — but the root page redirects to /overview, and the id
// in the URL is simply lost. Someone following a link from an alert or a runbook
// lands on a dashboard with no explanation. This maps them across instead.
//
// It runs as a blocking inline script before hydration rather than as an effect in
// a component, for two reasons: there is no flash of the wrong page, and it does not
// depend on how Next's client router decides to treat a path it never exported.
//
// Segment matching, not a regex, because the reserved names are an allowlist that
// has to stay readable — a run whose id happened to be "detail" must not loop.
export const LEGACY_REDIRECT = `(function(){
  try{
    var seg = location.pathname.split("/").filter(Boolean);
    var q = location.search ? "&" + location.search.slice(1) : "";
    var hash = location.hash || "";
    var go = function(path, id){
      location.replace(path + "?id=" + encodeURIComponent(id) + q + hash);
    };
    // /runs/archive/<id>  ->  /runs/archive/?id=<id>
    if (seg[0] === "runs" && seg[1] === "archive" && seg[2]) {
      return go("/runs/archive/", seg[2]);
    }
    // /runs/<id>  ->  /runs/detail/?id=<id>   ("detail" and "archive" are pages)
    if (seg[0] === "runs" && seg.length === 2 &&
        seg[1] !== "detail" && seg[1] !== "archive") {
      return go("/runs/detail/", seg[1]);
    }
    // /workflows/<id>/history  ->  /workflows/history/?id=<id>
    if (seg[0] === "workflows" && seg.length === 3 && seg[2] === "history") {
      return go("/workflows/history/", seg[1]);
    }
    // /workflows/<id>  ->  /workflows/detail/?id=<id>
    if (seg[0] === "workflows" && seg.length === 2 &&
        seg[1] !== "detail" && seg[1] !== "history" && seg[1] !== "new") {
      return go("/workflows/detail/", seg[1]);
    }
  }catch(e){ /* a broken shim must never keep the console from loading */ }
})();`;
