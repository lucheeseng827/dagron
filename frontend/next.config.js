/** @type {import('next').NextConfig} */
const DAGRON_API = process.env.DAGRON_API_URL || "http://localhost:8080";
// App version shown in the sidebar brand. Set via the APP_VERSION build-arg (the
// release/image tag); falls back to this package's version for local dev. Baked
// into the client bundle at build as NEXT_PUBLIC_APP_VERSION.
const APP_VERSION = process.env.APP_VERSION || require("./package.json").version;

const nextConfig = {
  // Emit a self-contained server bundle (.next/standalone) for a small runtime image.
  output: "standalone",
  env: { NEXT_PUBLIC_APP_VERSION: APP_VERSION },
  async rewrites() {
    // Proxy /api/* to dagron-api so the browser stays same-origin (cookies/CORS-free).
    return [{ source: "/api/:path*", destination: `${DAGRON_API}/api/:path*` }];
  },
};

module.exports = nextConfig;
