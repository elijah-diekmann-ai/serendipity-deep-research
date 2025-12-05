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

  const getJobType = (job: ArchiveJob) => {
    return job.job.target_input.target_type === "person" ? "P" : "C";
  };

  const isActive = (jobId: string) => {
    return currentJobId === jobId || pathname === `/research/${jobId}`;
  };

  return (
    <aside className="fixed left-0 top-0 bottom-0 w-[210px] border-r border-stone-200 bg-stone-50/90 flex flex-col z-40">
      {/* Logo */}
      <div className="p-5 border-b border-stone-200">
        <Link href="/" className="block hover:opacity-70 transition-opacity">
          <img
            src="/logo.svg"
            alt="Serendipity Capital"
            className="h-[34px] w-auto invert opacity-80"
          />
        </Link>
      </div>

      {/* New Research Button */}
      <div className="p-4 border-b border-stone-200">
        <Link
          href="/"
          className="flex items-center justify-center gap-2 w-full py-2.5 px-4 border border-stone-300 text-gray-600 hover:bg-white hover:border-stone-400 hover:text-gray-900 transition-all font-mono text-xs uppercase tracking-wider"
        >
          <span>+</span>
          <span>New Research</span>
        </Link>
      </div>

      {/* Recent Jobs */}
      <div className="flex-1 overflow-y-auto px-3 py-3">
        <div className="font-mono text-[10px] uppercase tracking-wider text-gray-400 px-2 mb-3">
          Recent
        </div>

        {loading && (
          <div className="space-y-1 px-2">
            {[...Array(5)].map((_, i) => (
              <div key={i} className="h-8 bg-stone-100 animate-pulse" />
            ))}
          </div>
        )}

        {!loading && jobs.length === 0 && (
          <p className="font-mono text-[11px] text-gray-400 px-2 py-4">
            No research yet
          </p>
        )}

        {!loading && jobs.length > 0 && (
          <div className="space-y-0.5">
            {jobs.map((job) => (
              <Link
                key={job.job.id}
                href={`/research/${job.job.id}`}
                className={`flex items-center gap-2 px-2 py-2 text-[15px] transition-colors ${
                  isActive(job.job.id)
                    ? "bg-white border-l-2 border-gray-900 text-gray-900"
                    : "text-gray-600 hover:bg-white hover:text-gray-900 border-l-2 border-transparent"
                }`}
              >
                <span className="font-mono text-[10px] text-gray-400 w-4">
                  {getJobType(job)}
                </span>
                <span className="truncate flex-1">
                  {getJobTitle(job)}
                </span>
              </Link>
            ))}
          </div>
        )}
      </div>

      {/* View All Archive */}
      <div className="p-4 border-t border-stone-200">
        <Link
          href="/archive"
          className={`flex items-center gap-2 font-mono text-[11px] uppercase tracking-wider transition-colors ${
            pathname === "/archive"
              ? "text-gray-900"
              : "text-gray-400 hover:text-gray-700"
          }`}
        >
          <span>→</span>
          <span>View All</span>
        </Link>
      </div>
    </aside>
  );
}
