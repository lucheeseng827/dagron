import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        // dagron status palette — also mirrored in lib/adapter.ts statusColor()
        bg: "var(--bg)",
        card: "var(--card)",
        fg: "var(--fg)",
        muted: "var(--muted)",
        accent: "var(--accent)",
      },
    },
  },
  plugins: [],
};

export default config;
