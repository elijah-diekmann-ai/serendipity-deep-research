"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { fetchArchive, ArchiveJob } from "../lib/api";

type SidebarProps = {
  currentJobId?: string;
};

export default function Sidebar({ currentJobId }: SidebarProps) {
  const pathname = usePathname();
  const [jobs, setJobs] = useState<ArchiveJob[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchArchive(15, 0)
      .then((allJobs) => {
        // Only show completed jobs
        const completedJobs = allJobs.filter(
          (job) => job.job.status.toLowerCase() === "completed"
        );
        setJobs(completedJobs);
      })
      .catch(() => setJobs([]))
      .finally(() => setLoading(false));
  }, []);

  const getJobTitle = (job: ArchiveJob) => {
    const input = job.job.target_input;
    if (input.target_type === "person" && input.person_name) {
      return input.person_name;
    }
    return input.company_name || input.context?.slice(0, 30) || "Untitled";
  };

  const isActive = (jobId: string) => {
    return currentJobId === jobId || pathname === `/research/${jobId}`;
  };

  return (
    <aside className="fixed left-0 top-0 bottom-0 w-[210px] border-r border-gray-200/60 bg-white/50 backdrop-blur-sm flex flex-col z-40">
      {/* Logo */}
      <div className="p-5 border-b border-gray-200/60">
        <Link href="/" className="block hover:opacity-70 transition-opacity">
          <img
            src="/logo.svg"
            alt="Serendipity Capital"
            className="h-[28px] w-auto invert opacity-80"
          />
        </Link>
      </div>

      {/* New Research Button */}
      <div className="p-4">
        <Link
          href="/"
          className="flex items-center justify-center gap-2 w-full py-2.5 px-4 rounded-lg border border-gray-200 text-gray-700 hover:bg-gray-50 hover:border-gray-300 transition-all text-sm font-medium"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
          </svg>
          New Research
        </Link>
      </div>

      {/* Recent Jobs */}
      <div className="flex-1 overflow-y-auto px-3">
        <div className="text-[10px] font-medium text-gray-400 uppercase tracking-wider px-2 mb-2">
          Recent
        </div>

        {loading && (
          <div className="space-y-2">
            {[...Array(5)].map((_, i) => (
              <div key={i} className="h-9 bg-gray-100 rounded-lg animate-pulse" />
            ))}
          </div>
        )}

        {!loading && jobs.length === 0 && (
          <p className="text-xs text-gray-400 px-2 py-4">No research yet</p>
        )}

        {!loading && jobs.length > 0 && (
          <div className="space-y-0.5">
            {jobs.map((job) => (
              <Link
                key={job.job.id}
                href={`/research/${job.job.id}`}
                className={`block px-3 py-2 rounded-lg text-sm truncate transition-colors ${
                  isActive(job.job.id)
                    ? "bg-gray-100 text-gray-900 font-medium"
                    : "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
                }`}
              >
                {getJobTitle(job)}
              </Link>
            ))}
          </div>
        )}
      </div>

      {/* View All Archive */}
      <div className="p-4 border-t border-gray-200/60">
        <Link
          href="/archive"
          className={`flex items-center gap-2 text-xs transition-colors ${
            pathname === "/archive"
              ? "text-gray-900 font-medium"
              : "text-gray-500 hover:text-gray-700"
          }`}
        >
          <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M20.25 7.5l-.625 10.632a2.25 2.25 0 01-2.247 2.118H6.622a2.25 2.25 0 01-2.247-2.118L3.75 7.5M10 11.25h4M3.375 7.5h17.25c.621 0 1.125-.504 1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125z" />
          </svg>
          View All
        </Link>
      </div>
    </aside>
  );
}

