"use client";

import { useEffect, useState, useRef, useMemo } from "react";
import axios from "axios";
import { API_BASE_URL } from "../lib/api";
import ReactMarkdown from "react-markdown";

const CITATION_LINK_REGEX = /\[S(\d+)\](?!\()/g;

function addCitationAnchors(
  markdown: string,
  citationIndexMap: Record<number, number>
): string {
  if (!markdown) return markdown;
  return markdown.replace(
    CITATION_LINK_REGEX,
    (_match, id) => {
      const numericId = Number(id);
      const shortIndex = citationIndexMap[numericId];
      const displayLabel = shortIndex ? `S${shortIndex}` : `S${id}`;
      return `[${displayLabel}](#source-${numericId})`;
    }
  );
}

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

type Citation = {
  id: number;
  title?: string;
  url?: string;
  provider?: string;
};

type Brief = {
  executive_summary?: string;
  business_model?: string;
  market?: string;
  team?: string;
  risks?: string;
  opportunities?: string;
  // new fields from backend
  used_citations?: Citation[];
  all_citations?: Citation[];
  // backwards-compatible alias
  citations?: Citation[];
  // Dynamic fields support
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

function getSafeHostname(url?: string): string | null {
  if (!url) return null;
  try {
    return new URL(url).hostname;
  } catch {
    return null;
  }
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
        // Reset lastIndex for each string if reusing regex instance, or create new one
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

  if (!job)
    return (
      <div className="p-4 text-center text-gray-500">Loading job status...</div>
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

  return (
    <div className="space-y-6 text-gray-900">
      {/* Status header card */}
      <div className="glass-panel !bg-white/70 !border-black/5 !text-gray-900 rounded-xl p-6">
        <div className="flex items-start justify-between">
          <div>
            <div className="text-[11px] uppercase tracking-wider text-gray-500 mb-2">
              Research run
            </div>
            <div
              className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-mono mb-3 border ${
                job.status === "COMPLETED"
                  ? "bg-emerald-50 text-emerald-700 border-emerald-200"
                  : job.status === "FAILED"
                  ? "bg-rose-50 text-rose-700 border-rose-200"
                  : "bg-blue-50 text-blue-700 border-blue-200"
              }`}
            >
              {job.status}
            </div>

            {(companyName || domain) && (
              <div className="mt-1 text-xs text-gray-500">
                Target:{" "}
                <span className="font-mono text-gray-700">
                  {companyName || domain}
                </span>
                {domain && companyName && (
                  <span className="text-gray-400"> · {domain}</span>
                )}
              </div>
            )}

            {job.status !== "COMPLETED" && job.status !== "FAILED" && (
              <p className="mt-4 text-xs text-gray-600">
                We&apos;re collecting sources and drafting your brief. The full write-up
                will appear below as soon as it&apos;s ready.
              </p>
            )}

            {typeof job.total_cost_usd === "number" && (
              <div className="mt-4 text-xs text-gray-600">
                Total API cost:{" "}
                <span className="font-mono">
                  ${Number(job.total_cost_usd).toFixed(4)}
                </span>
              </div>
            )}

            {providerEntries.length > 0 && (
              <details className="mt-2 text-xs text-gray-700">
                <summary className="cursor-pointer underline decoration-dotted">
                  Cost breakdown
                </summary>
                <div className="mt-2 space-y-2">
                  {providerEntries.map(([providerKey, providerData]) => (
                    <div key={providerKey} className="space-y-0.5">
                      <div className="font-semibold text-gray-900">
                        {providerKey}
                        {providerData.model && (
                          <span className="text-gray-500 font-normal">
                            {" "}
                            · {providerData.model}
                          </span>
                        )}
                      </div>
                      <div className="font-mono text-gray-700">
                        in={providerData.totals?.input ?? 0} · out=
                        {providerData.totals?.output ?? 0}
                        {typeof providerData.totals?.cached_input === "number" &&
                          ` · cached=${providerData.totals?.cached_input}`}
                        {typeof providerData.totals?.web_search_calls === "number" &&
                          providerData.totals?.web_search_calls > 0 &&
                          ` · web_search_calls=${providerData.totals?.web_search_calls}`}
                        {" · cost=$"}
                        {Number(providerData.cost_usd || 0).toFixed(4)}
                      </div>
                    </div>
                  ))}
                </div>
              </details>
            )}
          </div>

          <div className="flex flex-col items-end space-y-2">
            {/* Pulse and stuck indicator */}
            {job.status !== "COMPLETED" && job.status !== "FAILED" && (
              <div className="flex flex-col items-end space-y-1 text-sm text-gray-600 mb-2">
                <div className="flex items-center space-x-2 animate-pulse">
                  <div className="w-2 h-2 bg-blue-600 rounded-full"></div>
                </div>
                {isStuck && (
                  <div className="mt-1 text-xs text-orange-600 bg-orange-50 border border-orange-200 rounded px-2 py-1">
                    Taking longer than usual...
                  </div>
                )}
              </div>
            )}

            {trace.length > 0 && (
              <button
                type="button"
                onClick={() => setShowSystemThoughts((prev) => !prev)}
                className="text-xs font-mono uppercase tracking-wider text-gray-500 hover:text-gray-800 flex items-center gap-1"
              >
                <span>
                  {showSystemThoughts
                    ? "Hide system thoughts"
                    : "View system thoughts"}
                </span>
                <span
                  className={`transform transition-transform duration-300 ${
                    showSystemThoughts ? "rotate-180" : ""
                  }`}
                >
                  ▼
                </span>
              </button>
            )}
          </div>
        </div>

        <div
          className={`overflow-hidden transition-[max-height,opacity] duration-500 ease-in-out ${
            showSystemThoughts ? "max-h-[500px] opacity-100" : "max-h-0 opacity-0"
          }`}
        >
          <div
            ref={scrollRef}
            onScroll={handleTraceScroll}
            className="mt-4 border border-black/10 bg-black/5 rounded-lg p-3 text-xs text-gray-800 max-h-72 overflow-y-auto trace-scroll-shadow"
          >
            <div className="font-mono text-[10px] text-gray-500 mb-2">
              SYSTEM THOUGHTS – high-level trace of the research pipeline
            </div>

            {trace.length === 0 && (
              <div className="text-gray-500">No system thoughts recorded yet.</div>
            )}

            {trace.length > 0 && (
              <ol className="relative ml-3 border-l border-gray-300">
                {trace.map((evt) => {
                  const isLatest = latestTraceId === evt.id;
                  return (
                    <li
                      key={evt.id}
                      className={`mb-4 ml-4 trace-item ${
                        isLatest ? "trace-item-latest" : ""
                      }`}
                    >
                      <span
                        className={`absolute -left-[9px] mt-[3px] w-2 h-2 rounded-full ${
                          isLatest
                            ? "bg-blue-600 shadow-[0_0_0_3px_rgba(37,99,235,0.45)]"
                            : "bg-blue-400"
                        }`}
                      />
                      <div className="flex items-center justify-between gap-3">
                        <div className="font-semibold text-gray-900">{evt.label}</div>
                        <span className="text-[10px] text-gray-500">
                          {new Date(evt.created_at).toLocaleTimeString()}
                        </span>
                      </div>
                      <div className="mt-0.5 text-[10px] uppercase tracking-wide text-gray-500">
                        {evt.phase}
                        {evt.step && ` · ${evt.step}`}
                      </div>
                      {evt.detail && (
                        <div className="mt-1 text-[11px] text-gray-700">
                          {evt.detail}
                        </div>
                      )}
                      {/* Optional compact meta preview, but not raw logs */}
                      {evt.meta && Object.keys(evt.meta).length > 0 && (
                        <div className="mt-1 text-[10px] text-gray-500">
                          {renderMeta(evt)}
                        </div>
                      )}
                    </li>
                  );
                })}
              </ol>
            )}
          </div>
        </div>

        {job.status === "FAILED" && (
          <div className="mt-4 bg-red-50 border border-red-200 p-4 rounded text-red-700 text-sm">
            Something went wrong while processing this job.
          </div>
        )}
      </div>

      {/* Brief card */}
      {brief && (
        <div className="p-8 glass-panel !bg-white !border-gray-200 !shadow-sm rounded-xl">
          <div className="space-y-8">
            {Object.entries(brief).map(([key, value]) => {
              if (
                key === "citations" ||
                key === "used_citations" ||
                key === "all_citations" ||
                key === "sources" ||
                !value
              )
                return null;

              const text = value as string;
              // Ensure lists starting on the first line are parsed as lists by ReactMarkdown
              const markdownContent = text.trim().startsWith("-") ? `\n${text}` : text;
              const markdownWithAnchors = addCitationAnchors(
                markdownContent,
                citationIndexMap
              );
              const isUnverified = text.startsWith("⚠️ UNVERIFIED");

              return (
                <section key={key} className="border-b border-gray-200 pb-6 last:border-0">
                  <h2 className="text-xl font-semibold text-gray-900 mb-3">
                    {key.replace(/_/g, " ").toUpperCase()}
                  </h2>
                  {isUnverified && (
                    <div className="mb-2 text-sm font-semibold text-red-700 bg-red-50 border border-red-200 rounded px-2 py-1">
                      This section is marked as UNVERIFIED. It may contain
                      hallucinations or unsupported claims – treat with caution.
                    </div>
                  )}
                  <div className="prose prose-sm max-w-none text-gray-700 [&>h1]:hidden [&>h2]:hidden [&>h3]:!text-base [&>h3]:font-semibold [&>h3]:mt-4 [&>h3]:mb-2 [&>p]:!text-sm [&>ul]:list-disc [&>ul]:pl-5 [&>ol]:list-decimal [&>ol]:pl-5 [&>ul>li]:!text-sm [&>ul>li]:mb-2 [&>ol>li]:!text-sm [&>ol>li]:mb-2">
                    <ReactMarkdown
                      components={{
                        a({ href, children, ...props }) {
                          if (href?.startsWith("#source-")) {
                            const id = Number(href.replace("#source-", ""));
                            const citation = renderedCitations.find((c) => c.id === id);
                            const title = citation?.title || citation?.url || "Source";
                            const domain = getSafeHostname(citation?.url) || citation?.provider;

                            return (
                              <a
                                href={href}
                                title={domain ? `${title} – ${domain}` : title}
                                {...props}
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
              <section className="pt-4">
                <div className="flex items-center justify-between mb-3">
                  <h2 className="text-xl font-semibold text-gray-900">SOURCES</h2>
                  {brief?.all_citations &&
                    brief.all_citations.length >
                      (brief.used_citations || brief.citations || []).length && (
                      <button
                        type="button"
                        className="text-xs text-blue-600 hover:text-blue-800 underline"
                        onClick={() =>
                          setShowAllCitations((previous) => !previous)
                        }
                      >
                        {showAllCitations
                          ? "Show only cited sources"
                          : "Show all sources"}
                      </button>
                    )}
                </div>
                <ul className="list-none space-y-2 text-base text-gray-600">
                  {renderedCitations.map((source: Citation) => {
                    const domain = getSafeHostname(source.url);

                    return (
                      <li
                        key={source.id}
                        id={`source-${source.id}`}
                        className="source-entry flex gap-2 items-start rounded-lg px-3 py-2 transition-colors duration-300"
                      >
                        <span className="font-mono text-sm text-blue-600 bg-blue-50 px-1 rounded mt-1">
                          {citationIndexMap[source.id]
                            ? `[S${citationIndexMap[source.id]}]`
                            : `[S${source.id}]`}
                        </span>
                        <div className="flex-1 min-w-0 space-y-1">
                          <a
                            href={source.url || undefined}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="hover:underline block truncate"
                          >
                            {source.title || source.url || "Untitled source"}
                          </a>
                          <div className="text-xs text-gray-500 break-all">
                            {domain || "URL unavailable"}
                          </div>
                        </div>
                        <span className="text-[10px] font-mono uppercase tracking-wide px-1.5 py-0.5 rounded bg-gray-100 text-gray-500">
                          {source.provider}
                        </span>
                      </li>
                    );
                  })}
                </ul>
              </section>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
