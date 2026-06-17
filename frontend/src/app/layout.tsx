import "./globals.css";
import type { Metadata } from "next";
import AuthGuard from "@/components/AuthGuard";

export const metadata: Metadata = {
  title: "dagron",
  description: "dagron workflow orchestrator",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" data-theme="dark">
      <body>
        {/* AuthGuard gates + chooses shell (protected) vs bare (login). */}
        <AuthGuard>{children}</AuthGuard>
      </body>
    </html>
  );
}
