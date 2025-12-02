"use client";

import { useEffect } from "react";

export function ThemeEffect({ theme }: { theme: "dark" | "light" }) {
  useEffect(() => {
    if (theme === "light") {
      document.body.classList.add("theme-light");
    } else {
      document.body.classList.remove("theme-light");
    }
    // Cleanup on unmount or change
    return () => {
      document.body.classList.remove("theme-light");
    };
  }, [theme]);

  return null;
}

