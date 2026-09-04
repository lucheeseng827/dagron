/** @type {import('next').NextConfig} */
// App version shown in the sidebar brand, baked into the client bundle at build
// as NEXT_PUBLIC_APP_VERSION. Comes from the APP_VERSION build-arg, which the
// release workflow sets from the image tag.
//
// It used to fall back to this package's own version, which is npm packaging
// metadata that nobody bumps on release — so every build, including the shipped
// 0.5.0 images, displayed a hardcoded "v0.1.0". An unstamped build now says
// "dev", because not knowing the version is better than stating a wrong one.
const APP_VERSION = process.env.APP_VERSION || "dev";

const nextConfig = {
  // Emit a fully static site (out/) that any HTTP server can hand out — here,
  // dagron-api itself, which embeds it in the binary.
  //
  // This app never needed a Node runtime: 47 of its 65 source files are
  // "use client", it uses no server-only Next API (no next/headers, no cookies(),
  // no server actions, no route handlers, no middleware), and every request goes
  // through `BASE = "/api"` in src/lib/dagron-api.ts — a *relative* path. The
  // standalone server was only serving files and proxying.
  //
  // Serving from dagron-api makes /api same-origin for real, so the rewrite is
  // not just unnecessary — removing it FIXES the live-update stream. Next's
  // rewrites() proxy buffers responses and never flushes headers for an infinite
  // text/event-stream, so /api/events/stream hung and the console showed
  // "Offline" on a healthy stack. Direct to the API it is an immediate
  // 200 text/event-stream. DAGRON_API_URL is therefore no longer read.
  output: "export",
  // Emit `out/runs/index.html` rather than `out/runs.html`, so a plain static
  // file server resolves /runs without rewrite rules.
  trailingSlash: true,
  images: { unoptimized: true },
  env: { NEXT_PUBLIC_APP_VERSION: APP_VERSION },
};

module.exports = nextConfig;
