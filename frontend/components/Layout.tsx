import Link from "next/link";
import React from "react";
import { ThemeEffect } from "./ThemeEffect";

type LayoutProps = {
  children: React.ReactNode;
  rightSlot?: React.ReactNode;
  leftSlot?: React.ReactNode;
  mainClassName?: string;
  showLogo?: boolean;
  // NEW: layout mode is purely visual
  layout?: "centered" | "page";
};

export default function Layout({
  children,
  rightSlot,
  leftSlot,
  mainClassName,
  showLogo = true,
  layout = "centered",
}: LayoutProps) {
  const isPage = layout === "page";

  return (
    <main
      className={`min-h-screen flex flex-col items-center ${
        isPage ? "justify-start" : "justify-center"
      } relative overflow-hidden ${mainClassName || ""}`}
    >
      <ThemeEffect theme={isPage ? "light" : "dark"} />
      {/* Left-hand nav / Logo */}
      <div className="absolute top-8 left-8 z-50 flex items-center gap-4">
        {showLogo && (
          <div className="logo-container">
            <img
              src="/logo.svg"
              alt="Serendipity Capital"
              className="h-[38.4px] w-auto"
            />
          </div>
        )}
        {leftSlot}
      </div>

      {/* Right-hand nav (Archive / New search / etc.) */}
      <div className="absolute top-8 right-8 z-50">
        {rightSlot ?? (
          <Link
            href="/archive"
            className="text-sm font-medium text-white/90 hover:text-white hover:underline tracking-wide transition-colors font-mono"
          >
            View Archive
          </Link>
        )}
      </div>

      {/* Main content */}
      <div
        className={`w-full max-w-5xl flex-1 flex flex-col px-4 ${
          isPage
            ? "items-stretch justify-start pt-24 pb-16"
            : "items-center justify-center"
        }`}
      >
        {children}
      </div>
    </main>
  );
}
