import Link from "next/link";
import React from "react";
import { ThemeEffect } from "./ThemeEffect";
import Sidebar from "./Sidebar";

type LayoutProps = {
  children: React.ReactNode;
  rightSlot?: React.ReactNode;
  leftSlot?: React.ReactNode;
  hasRightPanel?: boolean;
  mainClassName?: string;
  showLogo?: boolean;
  showSidebar?: boolean;
  currentJobId?: string;
  // NEW: layout mode is purely visual
  layout?: "centered" | "page";
};

export default function Layout({
  children,
  rightSlot,
  leftSlot,
  hasRightPanel = false,
  mainClassName,
  showLogo = true,
  showSidebar = false,
  currentJobId,
  layout = "centered",
}: LayoutProps) {
  const isPage = layout === "page";

  return (
    <>
      {showSidebar && <Sidebar currentJobId={currentJobId} />}
      <main
        className={`min-h-screen flex flex-col items-center ${
          isPage ? "justify-start" : "justify-center"
        } relative overflow-hidden ${mainClassName || ""} ${
          showSidebar ? "ml-[234px]" : ""
        } ${hasRightPanel ? "mr-[504px]" : ""}`}
      >
        <ThemeEffect theme={isPage ? "light" : "dark"} />
        {/* Left-hand nav / Logo (hidden when sidebar is shown) */}
        {!showSidebar && (
          <div className="absolute top-8 left-8 z-50 flex items-center gap-4">
            {showLogo && (
              <Link href="/" className="logo-container block hover:opacity-80 transition-opacity">
                <img
                  src="/logo.svg"
                  alt="Serendipity Capital"
                  className="h-[38.4px] w-auto"
                />
              </Link>
            )}
            {leftSlot}
          </div>
        )}

        {/* Right-hand nav (hidden when sidebar is shown) */}
        {!showSidebar && (
          <div className="absolute top-8 right-8 z-50">
            {rightSlot ?? (
              <Link
                href="/archive"
                className="flex items-center gap-2 text-[11px] font-mono uppercase tracking-wider text-white/70 hover:text-white transition-colors"
              >
                <span>View All</span>
                <span>→</span>
              </Link>
            )}
          </div>
        )}

        {/* Main content */}
        <div
          className={`w-full max-w-5xl flex-1 flex flex-col px-4 ${
            isPage
              ? "items-stretch justify-start pt-12 pb-16"
              : "items-center justify-center"
          }`}
        >
          {children}
        </div>
      </main>
    </>
  );
}
