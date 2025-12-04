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
    return job.job.target_input.target_type === "person" ? "Person" : "Company";
  };

  const getStatusColor = (status: string) => {
    const colors: Record<string, string> = {
      completed: "text-green-600",
      processing: "text-blue-600",
      pending: "text-yellow-600",
      failed: "text-red-600",
    };
    return colors[status.toLowerCase()] || "text-gray-500";
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
          <h1 className="text-2xl font-light text-gray-900 mb-1 tracking-tight">
            Research Archive
          </h1>
          <p className="text-gray-500 text-sm">
            All previous research jobs
          </p>
        </div>

        {loading && (
          <div className="py-12 text-center">
            <div className="w-6 h-6 mx-auto border-2 border-gray-200 border-t-gray-500 rounded-full animate-spin" />
            <p className="text-gray-400 text-sm mt-4">Loading...</p>
          </div>
        )}

        {error && (
          <div className="py-8 text-center">
            <p className="text-red-500 text-sm">{error}</p>
          </div>
        )}

        {!loading && !error && jobs.length === 0 && (
          <div className="py-16 text-center border border-gray-200 rounded-xl bg-white/50">
            <p className="text-gray-500 text-sm">No research yet.</p>
            <p className="text-gray-400 text-xs mt-1">
              Start a new research from the sidebar.
            </p>
          </div>
        )}

        {!loading && !error && jobs.length > 0 && (
          <div className="border border-gray-200 rounded-xl bg-white/50 divide-y divide-gray-100 overflow-hidden">
            {jobs.map((job) => (
              <Link
                key={job.job.id}
                href={`/research/${job.job.id}`}
                className="flex items-center justify-between py-4 px-5 hover:bg-gray-50 transition-colors group"
              >
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-3 mb-1">
                    <span className="text-gray-900 font-medium truncate group-hover:text-blue-600 transition-colors">
                      {getJobTitle(job)}
                    </span>
                    <span className={`text-[10px] font-mono ${getStatusColor(job.job.status)}`}>
                      {job.job.status}
                    </span>
                  </div>
                  <div className="flex items-center gap-2 text-[11px] text-gray-400 font-mono">
                    <span>{getJobType(job)}</span>
                    <span className="text-gray-300">·</span>
                    <span>{formatDate(job.job.created_at)}</span>
                  </div>
                </div>
                <svg className="w-4 h-4 text-gray-300 group-hover:text-gray-500 transition-colors shrink-0 ml-4" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                </svg>
              </Link>
            ))}
          </div>
        )}
      </div>
    </Layout>
  );
}

