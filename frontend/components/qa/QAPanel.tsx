"use client";

import { useEffect, useRef, useState, useCallback } from "react";
import { 
  fetchJobQA, 
  askJobQuestion, 
  runMicroResearch,
  fetchJobSources,
  JobQAPair,
  JobQAPairExtended,
  ResearchPlanProposal,
  SourceOut,
} from "../../lib/api";
import { buildCitationIndexMap, Citation } from "../../lib/citations";
import { ChatBubble } from "./ChatBubble";
import { ResearchPlanCard } from "./ResearchPlanCard";

type QAPanelProps = {
  jobId: string;
  qaEnabled: boolean;
  companyLabel?: string;
  allCitations: Citation[];
  onCitationsUpdated?: (citations: Citation[]) => void;
};

// Extended history item that can include a research plan
type HistoryItem = JobQAPair & {
  research_plan?: ResearchPlanProposal | null;
  planExecuted?: boolean;
};

export function QAPanel({
  jobId,
  qaEnabled,
  companyLabel,
  allCitations,
  onCitationsUpdated,
}: QAPanelProps) {
  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [microResearchLoading, setMicroResearchLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [autoScrollEnabled, setAutoScrollEnabled] = useState(true);
  const [pendingQuestion, setPendingQuestion] = useState<string | null>(null);
  const [citations, setCitations] = useState<Citation[]>(allCitations);
  
  const transcriptRef = useRef<HTMLDivElement>(null);
  const citationIndexMap = buildCitationIndexMap(citations);

  // Sync citations when allCitations prop changes
  useEffect(() => {
    setCitations(allCitations);
  }, [allCitations]);

  useEffect(() => {
    if (qaEnabled && history.length === 0) {
      fetchJobQA(jobId)
        .then((data) => setHistory(data as HistoryItem[]))
        .catch((err) => console.error("Failed to load Q&A history", err));
    }
  }, [jobId, qaEnabled, history.length]);

  // Auto-scroll logic
  useEffect(() => {
    const el = transcriptRef.current;
    if (!el || !autoScrollEnabled) return;
    el.scrollTo({ top: el.scrollHeight, behavior: "smooth" });
  }, [history, autoScrollEnabled, loading, microResearchLoading]);

  const handleScroll = () => {
    const el = transcriptRef.current;
    if (!el) return;
    const isNearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 50;
    setAutoScrollEnabled(isNearBottom);
  };

  // Refresh citations after micro-research adds new sources
  const refreshCitations = useCallback(async () => {
    try {
      const sources = await fetchJobSources(jobId);
      const newCitations: Citation[] = sources.map((src: SourceOut) => ({
        id: src.id,
        url: src.url || "",
        title: src.title || "Source",
        provider: src.provider,
        published_date: src.published_date,
      }));
      setCitations(newCitations);
      onCitationsUpdated?.(newCitations);
    } catch (err) {
      console.error("Failed to refresh citations:", err);
    }
  }, [jobId, onCitationsUpdated]);

  async function handleSend(e: React.FormEvent) {
    e.preventDefault();
    const q = input.trim();
    if (!q || !qaEnabled || loading) return;

    // Immediately show the question and clear input
    setPendingQuestion(q);
    setInput("");
    setLoading(true);
    setError(null);
    setAutoScrollEnabled(true);

    try {
      const qa: JobQAPairExtended = await askJobQuestion(jobId, q);
      setPendingQuestion(null);
      
      // Add to history with research plan if present
      const historyItem: HistoryItem = {
        ...qa,
        research_plan: qa.research_plan,
        planExecuted: false,
      };
      setHistory((prev) => [...prev, historyItem]);
    } catch (err: any) {
      console.error("Q&A error:", err);
      setPendingQuestion(null);
      setError(err?.response?.data?.detail || "Failed to get answer.");
    } finally {
      setLoading(false);
    }
  }

  async function handleRunMicroResearch(qaId: number, planId: string) {
    if (microResearchLoading) return;
    
    setMicroResearchLoading(planId);
    setError(null);
    setAutoScrollEnabled(true);

    try {
      const newQa = await runMicroResearch(jobId, planId);
      
      // Mark the original Q&A's plan as executed
      setHistory((prev) => 
        prev.map((item) => 
          item.id === qaId 
            ? { ...item, planExecuted: true } 
            : item
        )
      );
      
      // Add the new answer to history (without a plan, since we just executed one)
      setHistory((prev) => [...prev, { ...newQa, planExecuted: true }]);
      
      // Refresh citations to include any new sources
      await refreshCitations();
      
    } catch (err: any) {
      console.error("Micro-research error:", err);
      setError(err?.response?.data?.detail || "Failed to run additional research.");
    } finally {
      setMicroResearchLoading(null);
    }
  }

  return (
    <aside className="fixed right-0 top-0 bottom-0 w-[480px] flex flex-col z-40 bg-stone-50/95 backdrop-blur-sm border-l border-stone-200">
      {/* Header */}
      <div className="shrink-0 px-5 py-4 border-b border-stone-200">
        <div className="flex items-center gap-3">
          <div className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
          <span className="font-mono text-[11px] uppercase tracking-wider text-gray-500">
            AI Terminal
          </span>
        </div>
        <div className="mt-2 font-mono text-[11px] text-gray-400 flex items-center gap-2">
          <span>Target:</span>
          <span className="text-gray-600">{companyLabel || "—"}</span>
          <span className="text-gray-300">/</span>
          <span>Sources: {citations.length}</span>
        </div>
      </div>

      {/* Transcript */}
      <div 
        className="flex-1 overflow-y-auto px-5 py-4"
        ref={transcriptRef}
        onScroll={handleScroll}
      >
        {history.length === 0 && !loading && !pendingQuestion && (
          <div className="h-full flex flex-col items-center justify-center text-center px-6">
            <div className="font-mono text-[10px] uppercase tracking-wider text-gray-400 mb-3">
              [ Ready ]
            </div>
            <p className="font-mono text-xs text-gray-500 leading-relaxed max-w-[280px]">
              Query the knowledge graph or ask agents to research further.
            </p>
            <div className="mt-6 font-mono text-[10px] text-gray-300">
              Press Enter to send
            </div>
          </div>
        )}

        <div className="space-y-6">
          {history.map((qa) => (
            <div key={qa.id} className="space-y-4">
              <ChatBubble
                role="user"
                markdown={qa.question}
                createdAt={qa.created_at}
                citationIndexMap={citationIndexMap}
                citations={citations}
              />
              <ChatBubble
                role="assistant"
                markdown={qa.answer_markdown}
                createdAt={qa.created_at}
                citationIndexMap={citationIndexMap}
                citations={citations}
              />
              
              {/* Research Plan Card (if present and not yet executed) */}
              {qa.research_plan && !qa.planExecuted && (
                <ResearchPlanCard
                  plan={qa.research_plan}
                  onRun={() => handleRunMicroResearch(qa.id, qa.research_plan!.plan_id)}
                  loading={microResearchLoading === qa.research_plan.plan_id}
                  disabled={!!microResearchLoading}
                />
              )}
              
              {/* Executed plan indicator */}
              {qa.research_plan && qa.planExecuted && (
                <div className="font-mono text-[10px] uppercase tracking-wider text-emerald-600 flex items-center gap-2 pl-1">
                  <span className="w-1 h-1 rounded-full bg-emerald-500" />
                  Research completed
                </div>
              )}
            </div>
          ))}

          {/* Show pending question immediately */}
          {pendingQuestion && (
            <ChatBubble
              role="user"
              markdown={pendingQuestion}
              citationIndexMap={citationIndexMap}
              citations={citations}
            />
          )}

          {/* Loading indicator for Q&A */}
          {loading && (
            <div className="flex items-center gap-3 py-2">
              <div className="flex space-x-1">
                <div className="w-1 h-1 bg-gray-400 rounded-full animate-pulse" />
                <div className="w-1 h-1 bg-gray-400 rounded-full animate-pulse" style={{ animationDelay: "150ms" }} />
                <div className="w-1 h-1 bg-gray-400 rounded-full animate-pulse" style={{ animationDelay: "300ms" }} />
              </div>
              <span className="font-mono text-[10px] uppercase tracking-wider text-gray-400">
                Processing...
              </span>
            </div>
          )}
          
          {/* Loading indicator for micro-research */}
          {microResearchLoading && (
            <div className="flex items-center gap-3 py-2 px-3 border border-stone-200 bg-white">
              <div className="flex space-x-1">
                <div className="w-1 h-1 bg-blue-400 rounded-full animate-pulse" />
                <div className="w-1 h-1 bg-blue-400 rounded-full animate-pulse" style={{ animationDelay: "150ms" }} />
                <div className="w-1 h-1 bg-blue-400 rounded-full animate-pulse" style={{ animationDelay: "300ms" }} />
              </div>
              <span className="font-mono text-[10px] uppercase tracking-wider text-gray-500">
                Running additional research...
              </span>
            </div>
          )}
          
          {error && (
            <div className="font-mono text-xs text-red-600 bg-red-50 border-l-2 border-red-500 py-2 px-3">
              {error}
            </div>
          )}
        </div>
      </div>

      {/* Composer */}
      <div className="shrink-0 p-4 border-t border-stone-200 bg-white/50">
        <form onSubmit={handleSend}>
          <div className="relative">
            <div className="flex items-center border border-stone-300 bg-white focus-within:border-stone-400 transition-colors">
              <span className="pl-3 pr-2 font-mono text-[10px] text-gray-400 select-none">{'>'}</span>
              <input
                type="text"
                className="flex-1 bg-transparent py-3 pr-3 placeholder:text-gray-400 focus:outline-none text-sm text-gray-800 font-mono"
                placeholder="Enter query..."
                value={input}
                onChange={(e) => setInput(e.target.value)}
                disabled={!qaEnabled || loading || !!microResearchLoading}
              />
              <button
                type="submit"
                disabled={!input.trim() || loading || !qaEnabled || !!microResearchLoading}
                className="px-4 py-2 mr-1 font-mono text-[10px] uppercase tracking-wider text-gray-500 hover:text-gray-900 disabled:text-gray-300 disabled:cursor-not-allowed transition-colors"
              >
                Run
              </button>
            </div>
            <div className="mt-2 flex items-center justify-between font-mono text-[9px] text-gray-400">
              <span>{input.length}/1000</span>
              <span>[Enter to send]</span>
            </div>
          </div>
        </form>
      </div>
    </aside>
  );
}
