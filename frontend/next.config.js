/** @type {import('next').NextConfig} */
const DAGRON_API = process.env.DAGRON_API_URL || "http://localhost:8080";
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
  // Emit a self-contained server bundle (.next/standalone) for a small runtime image.
  output: "standalone",
  env: { NEXT_PUBLIC_APP_VERSION: APP_VERSION },
  async rewrites() {
    // Proxy /api/* to dagron-api so the browser stays same-origin (cookies/CORS-free).
    return [{ source: "/api/:path*", destination: `${DAGRON_API}/api/:path*` }];
  },
};

module.exports = nextConfig;
