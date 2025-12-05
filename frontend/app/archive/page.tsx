"use client";

import React, { useEffect, useState } from 'react';
import Link from 'next/link';
import Layout from '../../components/Layout';
import { fetchArchive, ArchiveJob } from '../../lib/api';

export default function ArchivePage() {
  const [jobs, setJobs] = useState<ArchiveJob[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchArchive()
      .then((allJobs) => {
        // Only show completed jobs in the archive
        const completedJobs = allJobs.filter(
          (job) => job.job.status.toLowerCase() === "completed"
        );
        setJobs(completedJobs);
      })
      .catch((err) => setError(err?.message || "Failed to load archive"))
      .finally(() => setLoading(false));
  }, []);

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  };

  const formatTime = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
    });
  };

  const getJobTitle = (job: ArchiveJob) => {
    const input = job.job.target_input;
    if (input.target_type === "person" && input.person_name) {
      return input.person_name;
    }
    return input.company_name || input.context?.slice(0, 50) || "Untitled";
  };

  const getJobType = (job: ArchiveJob) => {
    return job.job.target_input.target_type === "person" ? "person" : "company";
  };

  return (
    <Layout
      layout="page"
      mainClassName="bg-horizon-light"
      showSidebar={true}
    >
      <div className="w-full max-w-3xl">
        {/* Header */}
        <div className="mb-8">
          <div className="font-mono text-[11px] uppercase tracking-wider text-gray-400 mb-2">
            Archive
          </div>
          <h1 className="text-2xl font-light text-gray-900 tracking-tight">
            Research History
          </h1>
          <div className="mt-2 font-mono text-[11px] text-gray-400 flex items-center gap-2">
            <span>Total: {jobs.length}</span>
            <span className="text-gray-300">/</span>
          </div>
        </div>

        {loading && (
          <div className="py-12">
            <div className="flex items-center gap-3">
              <div className="flex space-x-1">
                <div className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-pulse" />
                <div className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-pulse" style={{ animationDelay: "150ms" }} />
                <div className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-pulse" style={{ animationDelay: "300ms" }} />
              </div>
              <span className="font-mono text-[11px] uppercase tracking-wider text-gray-400">
                Loading...
              </span>
            </div>
          </div>
        )}

        {error && (
          <div className="py-8">
            <div className="font-mono text-[13px] text-red-600 bg-red-50 border-l-2 border-red-500 py-2 px-3">
              {error}
            </div>
          </div>
        )}

        {!loading && !error && jobs.length === 0 && (
          <div className="py-16 border border-stone-200 bg-stone-50/50">
            <div className="text-center">
              <div className="font-mono text-[11px] uppercase tracking-wider text-gray-400 mb-2">
                [ Empty ]
              </div>
              <p className="font-mono text-[13px] text-gray-500">
                No research completed yet.
              </p>
            </div>
          </div>
        )}

        {!loading && !error && jobs.length > 0 && (
          <div className="border border-stone-200 divide-y divide-stone-100">
            {/* Table Header */}
            <div className="px-4 py-2 bg-stone-50/80 flex items-center font-mono text-[10px] uppercase tracking-wider text-gray-400">
              <div className="flex-1">Target</div>
              <div className="w-24 text-center">Type</div>
              <div className="w-36 text-right">Date</div>
            </div>
            
            {jobs.map((job) => (
              <Link
                key={job.job.id}
                href={`/research/${job.job.id}`}
                className="flex items-center px-4 py-3 hover:bg-stone-50 transition-colors group"
              >
                <div className="flex-1 min-w-0">
                  <span className="text-gray-900 font-medium text-[15px] truncate block">
                    {getJobTitle(job)}
                  </span>
                </div>
                <div className="w-24 text-center">
                  <span className="font-mono text-[11px] uppercase tracking-wider text-gray-400">
                    {getJobType(job)}
                  </span>
                </div>
                <div className="w-36 text-right font-mono text-[11px] text-gray-400 flex flex-col items-end">
                  <span>{formatDate(job.job.created_at)}</span>
                  <span className="text-gray-300">{formatTime(job.job.created_at)}</span>
                </div>
              </Link>
            ))}
          </div>
        )}
      </div>
    </Layout>
  );
}
