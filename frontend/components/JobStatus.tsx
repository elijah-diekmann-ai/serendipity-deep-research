"use client";

import { useEffect, useState, useRef, useMemo } from "react";
import axios from "axios";
import { API_BASE_URL } from "../lib/api";
import ReactMarkdown from "react-markdown";
import {
  addCitationAnchors,
  getSafeHostname,
  Citation,
  buildCitationIndexMap,
} from "../lib/citations";
import { QAPanel } from "./qa/QAPanel";

type LLMUsageTotals = {
  input?: number;
  output?: number;
  cached_input?: number;
  reasoning_output?: number;
  web_search_calls?: number;
};

type LLMProviderUsage = {
  model?: string;
  cost_usd?: number;
  totals?: LLMUsageTotals;
};

type LLMUsage = {
  providers?: Record<string, LLMProviderUsage>;
  total_cost_usd?: number;
};

type Job = {
  id: string;
  status: "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED";
  created_at: string;
  completed_at?: string;
  total_cost_usd?: number | null;
  llm_usage?: LLMUsage | null;
};

type Brief = {
  executive_summary?: string;
  business_model?: string;
  market?: string;
  team?: string;
  risks?: string;
  opportunities?: string;
  used_citations?: Citation[];
  all_citations?: Citation[];
  citations?: Citation[];
  [key: string]: any;
};

type TraceEvent = {
  id: number;
  created_at: string;
  phase: string;
  step?: string;
  label: string;
  detail?: string;
  meta?: Record<string, any> | null;
};

function renderMeta(evt: TraceEvent): string {
  const m = evt.meta || {};
  if (evt.phase === "PLANNING" && typeof m.num_exa_queries === "number") {
    return `Planner will issue ${m.num_exa_queries} Exa queries across ${
      m.steps?.length ?? "several"
    } steps.`;
  }
  if (evt.phase === "COLLECTION" && Array.isArray(m.steps_with_results)) {
    return `Connectors returned data for: ${m.steps_with_results.join(", ")}.`;
  }
  if (evt.phase === "ENTITY_RESOLUTION") {
    return [
      m.company_name && `Company: ${m.company_name}`,
      m.domain && `Domain: ${m.domain}`,
      typeof m.num_people === "number" && `People resolved: ${m.num_people}`,
      typeof m.num_web_snippets === "number" &&
        `Web snippets: ${m.num_web_snippets}`,
    ]
      .filter(Boolean)
      .join(" · ");
  }
  if (evt.phase === "WRITING" && evt.step?.startsWith("section:")) {
    return "Using curated sources and enforcing citation rules.";
  }
  if (evt.phase === "QA") {
    if (m.question_length) {
      return `Q&A: question length ${m.question_length} chars.`;
    }
    if (typeof m.num_sources_used === "number") {
      return `Q&A: used ${m.num_sources_used} sources.`;
    }
  }
  if (evt.phase === "COSTS") {
    if (typeof m.total_cost_usd === "number") {
      return `Total cost so far: $${m.total_cost_usd.toFixed(4)}`;
    }
    const openaiTotals = m.openai_totals;
    if (openaiTotals) {
      const tokens = [
        typeof openaiTotals.input === "number" && `in=${openaiTotals.input}`,
        typeof openaiTotals.output === "number" && `out=${openaiTotals.output}`,
        typeof m.openai_cost_usd === "number" &&
          `cost=$${Number(m.openai_cost_usd).toFixed(4)}`,
      ]
        .filter(Boolean)
        .join(" · ");
      return tokens || "";
    }
    if (typeof m.openai_cost_usd === "number") {
      return `Web-search cost so far: $${Number(m.openai_cost_usd).toFixed(4)}`;
    }
  }
  return "";
}

export default function JobStatus({ jobId }: { jobId: string }) {
  const [job, setJob] = useState<Job | null>(null);
  const [brief, setBrief] = useState<Brief | null>(null);
  const [pollCount, setPollCount] = useState(0);
  const [showAllCitations, setShowAllCitations] = useState(false);
  const [trace, setTrace] = useState<TraceEvent[]>([]);
  const [showSystemThoughts, setShowSystemThoughts] = useState(false);

  // NEW: auto-scroll behaviour (chat-style)
  const [autoScrollEnabled, setAutoScrollEnabled] = useState(true);
  const scrollRef = useRef<HTMLDivElement>(null);

  // Reset auto-scroll each time the panel is opened
  useEffect(() => {
    if (showSystemThoughts) {
      setAutoScrollEnabled(true);
    }
  }, [showSystemThoughts]);

  // Track if user has scrolled away from the bottom
  const handleTraceScroll = () => {
    const el = scrollRef.current;
    if (!el) return;

    const { scrollTop, scrollHeight, clientHeight } = el;
    const isNearBottom = scrollHeight - scrollTop - clientHeight < 40;
    setAutoScrollEnabled(isNearBottom);
  };

  // Auto-scroll to bottom when new events arrive and auto-scroll is enabled
  useEffect(() => {
    const el = scrollRef.current;
    if (!showSystemThoughts || !el || !autoScrollEnabled) return;

    el.scrollTo({
      top: el.scrollHeight,
      behavior: trace.length <= 1 ? "auto" : "smooth",
    });
  }, [trace, showSystemThoughts, autoScrollEnabled]);

  useEffect(() => {
    let interval: NodeJS.Timeout;

    async function fetchStatus() {
      try {
        const resp = await axios.get(`${API_BASE_URL}/research/${jobId}`);
        setJob(resp.data.job);
        setBrief(resp.data.brief);
        setTrace(resp.data.trace || []);
        setPollCount((c) => c + 1);

        if (
          resp.data.job.status === "COMPLETED" ||
          resp.data.job.status === "FAILED"
        ) {
          clearInterval(interval);
        }
      } catch (e) {
        console.error("Failed to fetch job status", e);
      }
    }

    fetchStatus();
    interval = setInterval(fetchStatus, 5000);

    return () => clearInterval(interval);
  }, [jobId]);

  const isStuck =
    job?.status === "PROCESSING" && pollCount >= 60; // ~5 minutes at 5s/poll

  const citationsToShow: Citation[] = useMemo(() => {
    if (!brief) return [];
    let list = showAllCitations
      ? brief.all_citations ?? brief.citations ?? []
      : brief.used_citations ?? brief.citations ?? [];
    list = Array.isArray(list) ? list : [];

    // Ensure all IDs referenced in the text are present in the list
    const referencedIds = new Set<number>();
    const regex = /\[S(\d+)\](?!\()/g;
    Object.values(brief).forEach((val) => {
      if (typeof val === "string") {
        let match;
        const localRegex = new RegExp(regex);
        while ((match = localRegex.exec(val)) !== null) {
          referencedIds.add(Number(match[1]));
        }
      }
    });

    const existingIds = new Set(list.map((c) => c.id));
    const missingIds = Array.from(referencedIds).filter(
      (id) => !existingIds.has(id)
    );

    if (missingIds.length > 0) {
      const placeholders: Citation[] = missingIds.map((id) => ({
        id,
        title: "Referenced Source (Details unavailable)",
        provider: "unknown",
        url: undefined,
      }));
      return [...list, ...placeholders];
    }

    return list;
  }, [brief, showAllCitations]);

  const renderedCitations = useMemo(
    () =>
      citationsToShow.filter(
        (citation): citation is Citation & { id: number } =>
          Boolean(citation) && typeof citation.id === "number"
      ),
    [citationsToShow]
  );

  // Stable index map: based on all_citations, independent of filters
  const citationIndexMap = useMemo(() => {
    const all = brief?.all_citations ?? brief?.citations ?? [];
    const map: Record<number, number> = {};
    if (Array.isArray(all)) {
      all.forEach((citation, idx) => {
        if (citation && typeof citation.id === "number") {
          map[citation.id] = idx + 1;
        }
      });
    }
    return map;
  }, [brief]);

  // Q&A Availability
  const qaEnabled =
    job?.status === "COMPLETED" &&
    !!brief &&
    Array.isArray(brief.all_citations ?? brief.citations ?? []);

  if (!job)
    return (
      <div className="p-4 text-center font-mono text-sm text-gray-500">Loading...</div>
    );

  const resolvedEntity = trace.find(
    (evt) => evt.phase === "ENTITY_RESOLUTION" && evt.meta?.company_name
  );
  const companyName = resolvedEntity?.meta?.company_name as string | undefined;
  const domain = resolvedEntity?.meta?.domain as string | undefined;
  const providerEntries = job.llm_usage?.providers
    ? Object.entries(job.llm_usage.providers)
    : [];

  const latestTraceId = trace.length > 0 ? trace[trace.length - 1].id : null;

  // For Q&A Drawer
  const allCitations = brief?.all_citations ?? brief?.citations ?? [];

  return (
    <>
      <QAPanel
        jobId={jobId}
        qaEnabled={qaEnabled}
        companyLabel={companyName || domain || "This target"}
        allCitations={allCitations}
      />

      <div className="space-y-6 text-gray-900">
        {/* Status header card */}
        <div className="border border-stone-200 bg-white p-6">
          <div className="flex items-start justify-between">
            <div>
              <div className="font-mono text-[11px] uppercase tracking-wider text-gray-400 mb-3">
                Research Run
              </div>
              <div className="flex items-center gap-3 mb-3">
                <div className={`w-2 h-2 rounded-full ${
                  job.status === "COMPLETED"
                    ? "bg-emerald-500"
                    : job.status === "FAILED"
                    ? "bg-red-500"
                    : "bg-blue-500 animate-pulse"
                }`} />
                <span className="font-mono text-[13px] uppercase tracking-wider text-gray-600">
                  {job.status === "PROCESSING" ? "Researching" : job.status}
                </span>
              </div>

              {(companyName || domain) && (
                <div className="font-mono text-xs text-gray-500 flex items-center gap-2">
                  <span>Target:</span>
                  <span className="text-gray-700">{companyName || domain}</span>
                  {domain && companyName && (
                    <>
                      <span className="text-gray-300">/</span>
                      <span className="text-gray-400">{domain}</span>
                    </>
                  )}
                </div>
              )}

              {job.status !== "COMPLETED" && job.status !== "FAILED" && (
                <p className="mt-4 font-mono text-xs text-gray-500">
                  Agents working. Brief will be ready shortly.
                </p>
              )}

              {typeof job.total_cost_usd === "number" && (
                <div className="mt-4 font-mono text-xs text-gray-500">
                  Cost: <span className="text-gray-700">${Number(job.total_cost_usd).toFixed(4)}</span>
                </div>
              )}

              {providerEntries.length > 0 && (
                <details className="mt-3 font-mono text-[11px] text-gray-600">
                  <summary className="cursor-pointer text-gray-500 hover:text-gray-700">
                    → Cost breakdown
                  </summary>
                  <div className="mt-2 pl-3 border-l border-stone-200 space-y-2">
                    {providerEntries.map(([providerKey, providerData]) => (
                      <div key={providerKey} className="space-y-0.5">
                        <div className="text-gray-700">
                          {providerKey}
                          {providerData.model && (
                            <span className="text-gray-400"> / {providerData.model}</span>
                          )}
                        </div>
                        <div className="text-gray-500">
                          in={providerData.totals?.input ?? 0} / out={providerData.totals?.output ?? 0}
                          {typeof providerData.totals?.cached_input === "number" &&
                            ` / cached=${providerData.totals?.cached_input}`}
                          {typeof providerData.totals?.web_search_calls === "number" &&
                            providerData.totals?.web_search_calls > 0 &&
                            ` / web=${providerData.totals?.web_search_calls}`}
                          {" / $"}{Number(providerData.cost_usd || 0).toFixed(4)}
                        </div>
                      </div>
                    ))}
                  </div>
                </details>
              )}
            </div>

            <div className="flex flex-col items-end space-y-2">
              {/* Stuck indicator */}
              {job.status !== "COMPLETED" && job.status !== "FAILED" && isStuck && (
                <div className="font-mono text-[11px] text-amber-600 border border-amber-200 bg-amber-50 px-2 py-1">
                  Taking longer than usual...
                </div>
              )}

              {trace.length > 0 && (
                <button
                  type="button"
                  onClick={() => setShowSystemThoughts((prev) => !prev)}
                  className="flex items-center gap-2 px-3 py-1.5 border border-stone-200 hover:border-stone-300 hover:bg-stone-50 transition-all font-mono text-[11px] uppercase tracking-wider text-gray-500 hover:text-gray-700"
                >
                  {job.status !== "COMPLETED" && job.status !== "FAILED" && (
                    <div className="w-2 h-2 rounded-full bg-blue-500 animate-pulse" />
                  )}
                  <span>System</span>
                  <span className={`transition-transform duration-200 ${showSystemThoughts ? "rotate-180" : ""}`}>
                    ↓
                  </span>
                </button>
              )}
            </div>
          </div>

          <div
            className={`grid transition-all duration-300 ease-in-out ${
              showSystemThoughts
                ? "grid-rows-[1fr] opacity-100 mt-4"
                : "grid-rows-[0fr] opacity-0 mt-0"
            }`}
          >
            <div className="overflow-hidden min-h-0">
              <div
                ref={scrollRef}
                onScroll={handleTraceScroll}
                className="border border-stone-200 bg-stone-50 p-4 font-mono text-[11px] text-gray-700 max-h-72 overflow-y-auto"
              >
                <div className="text-[9px] uppercase tracking-wider text-gray-400 mb-3">
                  System Thoughts — AI research pipeline trace
                </div>

                {trace.length === 0 && (
                  <div className="text-gray-400">No thoughts recorded yet.</div>
                )}

                {trace.length > 0 && (
                  <div className="space-y-3">
                    {trace.map((evt) => {
                      const isLatest = latestTraceId === evt.id;
                      return (
                        <div
                          key={evt.id}
                          className={`pl-3 border-l-2 ${
                            isLatest ? "border-blue-500" : "border-stone-200"
                          }`}
                        >
                          <div className="flex items-center justify-between gap-3">
                            <div className="text-gray-800">{evt.label}</div>
                            <span className="text-[9px] text-gray-400">
                              {new Date(evt.created_at).toLocaleTimeString()}
                            </span>
                          </div>
                          <div className="text-[9px] uppercase tracking-wider text-gray-400 mt-0.5">
                            {evt.phase}
                            {evt.step && ` / ${evt.step}`}
                          </div>
                          {evt.detail && (
                            <div className="mt-1 text-gray-600">
                              {evt.detail}
                            </div>
                          )}
                          {evt.meta && Object.keys(evt.meta).length > 0 && (
                            <div className="mt-1 text-[10px] text-gray-500">
                              {renderMeta(evt)}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </div>
          </div>

          {job.status === "FAILED" && (
            <div className="mt-4 font-mono text-xs text-red-600 bg-red-50 border-l-2 border-red-500 py-2 px-3">
              Something went wrong while processing this job.
            </div>
          )}
        </div>

        {/* Brief card */}
        {brief && (
          <div className="border border-stone-200 bg-white">
            <div className="p-6 space-y-8">
              {Object.entries(brief).map(([key, value]) => {
                if (
                  key === "citations" ||
                  key === "used_citations" ||
                  key === "all_citations" ||
                  key === "sources" ||
                  key === "section_order" ||
                  typeof value !== "string" ||
                  !value
                )
                  return null;

                const text = value as string;
                const markdownContent = text.trim().startsWith("-")
                  ? `\n${text}`
                  : text;
                const markdownWithAnchors = addCitationAnchors(
                  markdownContent,
                  citationIndexMap
                );
                const isUnverified = text.startsWith("⚠️ UNVERIFIED");

                return (
                  <section
                    key={key}
                    className="border-b border-stone-100 pb-6 last:border-0 last:pb-0"
                  >
                    <h2 className="font-mono text-sm tracking-wide text-gray-500 mb-4 capitalize">
                      {key.replace(/_/g, " ")}
                    </h2>
                    {isUnverified && (
                      <div className="mb-3 font-mono text-[10px] text-red-600 bg-red-50 border-l-2 border-red-500 py-2 px-3">
                        ⚠ UNVERIFIED — May contain unsupported claims
                      </div>
                    )}
                    <div className="prose prose-sm max-w-none text-gray-700 font-serif
                      prose-headings:font-mono prose-headings:text-xs prose-headings:tracking-wide prose-headings:text-gray-500 prose-headings:mt-6 prose-headings:mb-3
                      prose-h1:hidden prose-h2:hidden
                      prose-h3:text-gray-600
                      prose-p:text-[14px] prose-p:leading-[1.75]
                      prose-ul:list-disc prose-ul:pl-5
                      prose-ol:list-decimal prose-ol:pl-5
                      prose-li:text-[14px] prose-li:mb-1.5 prose-li:leading-[1.75]
                      prose-strong:text-gray-800 prose-strong:font-semibold
                      prose-a:text-blue-600 prose-a:no-underline hover:prose-a:underline
                    ">
                      <ReactMarkdown
                        components={{
                          a({ href, children, ...props }) {
                            if (href?.startsWith("#source-")) {
                              const id = Number(href.replace("#source-", ""));
                              const citation = renderedCitations.find(
                                (c) => c.id === id
                              );
                              const title =
                                citation?.title || citation?.url || "Source";
                              const domainName =
                                getSafeHostname(citation?.url) ||
                                citation?.provider;

                              return (
                                <a
                                  href={href}
                                  title={domainName ? `${title} – ${domainName}` : title}
                                  {...props}
                                  className="inline-flex items-center font-mono text-[10px] text-blue-600 hover:text-blue-700 bg-blue-50 px-1.5 py-0.5 border border-blue-200 hover:border-blue-300 transition-colors"
                                >
                                  {children}
                                </a>
                              );
                            }
                            return (
                              <a href={href} {...props}>
                                {children}
                              </a>
                            );
                          },
                        }}
                      >
                        {markdownWithAnchors}
                      </ReactMarkdown>
                    </div>
                  </section>
                );
              })}

              {/* Sources Section */}
              {renderedCitations.length > 0 && (
                <section className="pt-6 border-t border-stone-100">
                  <div className="flex items-center justify-between mb-4">
                    <h2 className="font-mono text-sm tracking-wide text-gray-500 capitalize">
                      Sources
                    </h2>
                    {brief?.all_citations &&
                      brief.all_citations.length >
                        (brief.used_citations || brief.citations || [])
                          .length && (
                        <button
                          type="button"
                          className="font-mono text-[10px] text-gray-500 hover:text-gray-700 transition-colors"
                          onClick={() =>
                            setShowAllCitations((previous) => !previous)
                          }
                        >
                          {showAllCitations
                            ? "← Show cited only"
                            : "Show all →"}
                        </button>
                      )}
                  </div>
                  <div className="space-y-2">
                    {renderedCitations.map((source: Citation) => {
                      const sourceDomain = getSafeHostname(source.url);

                      return (
                        <div
                          key={source.id}
                          id={`source-${source.id}`}
                          className="flex gap-3 items-start py-2 px-3 border border-stone-100 hover:border-stone-200 hover:bg-stone-50 transition-colors"
                        >
                          <span className="font-mono text-[10px] text-blue-600 bg-blue-50 px-1.5 py-0.5 border border-blue-200 shrink-0">
                            {citationIndexMap[source.id]
                              ? `S${citationIndexMap[source.id]}`
                              : `S${source.id}`}
                          </span>
                          <div className="flex-1 min-w-0">
                            <a
                              href={source.url || undefined}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="text-[14px] text-gray-700 hover:text-blue-600 hover:underline block truncate font-serif"
                            >
                              {source.title || source.url || "Untitled source"}
                            </a>
                            <div className="font-mono text-[10px] text-gray-400 mt-0.5">
                              {sourceDomain || "URL unavailable"}
                            </div>
                          </div>
                          <span className="font-mono text-[9px] uppercase tracking-wider text-gray-400 shrink-0">
                            {source.provider}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                </section>
              )}
            </div>
          </div>
        )}
      </div>
    </>
  );
}
