/** @type {import('next').NextConfig} */
const DAGRON_API = process.env.DAGRON_API_URL || "http://localhost:8080";

const nextConfig = {
  // Emit a self-contained server bundle (.next/standalone) for a small runtime image.
  output: "standalone",
  async rewrites() {
    // Proxy /api/* to dagron-api so the browser stays same-origin (cookies/CORS-free).
    return [{ source: "/api/:path*", destination: `${DAGRON_API}/api/:path*` }];
  },
};

module.exports = nextConfig;
