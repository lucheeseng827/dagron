import "./globals.css";
import type { Metadata } from "next";
import { Archivo } from "next/font/google";
import AuthGuard from "@/components/AuthGuard";
import ToastProvider from "@/components/Toasts";

// Self-hosted at build time. globals.css used to @import this family from
// fonts.googleapis.com, which is a render-blocking request the console cannot
// make at all in an air-gapped install — a deployment mode dagron ships. Now
// the bytes are part of the bundle and there is no runtime request off-box.
const archivo = Archivo({
  // Archivo is a variable font: omit `weight` so next/font ships the one
  // variable file (the whole 100–900 axis) rather than four static instances.
  subsets: ["latin"],
  variable: "--font-sans",
  display: "swap",
});

export const metadata: Metadata = {
  title: "dagron",
  description: "dagron workflow orchestrator",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" data-theme="dark" className={archivo.variable}>
      <body>
        {/* AuthGuard gates + chooses shell (protected) vs bare (login). */}
        <ToastProvider>
          <AuthGuard>{children}</AuthGuard>
        </ToastProvider>
      </body>
    </html>
  );
}
