"use client";

import { useEffect, useRef, useState } from "react";
import { fetchJobQA, askJobQuestion, JobQAPair } from "../../lib/api";
import { buildCitationIndexMap, Citation } from "../../lib/citations";
import { ChatBubble } from "./ChatBubble";

type QAPanelProps = {
  jobId: string;
  qaEnabled: boolean;
  companyLabel?: string;
  allCitations: Citation[];
};

export function QAPanel({
  jobId,
  qaEnabled,
  companyLabel,
  allCitations,
}: QAPanelProps) {
  const [history, setHistory] = useState<JobQAPair[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [autoScrollEnabled, setAutoScrollEnabled] = useState(true);
  const [pendingQuestion, setPendingQuestion] = useState<string | null>(null);
  
  const transcriptRef = useRef<HTMLDivElement>(null);
  const citationIndexMap = buildCitationIndexMap(allCitations);

  useEffect(() => {
    if (qaEnabled && history.length === 0) {
      fetchJobQA(jobId)
        .then(setHistory)
        .catch((err) => console.error("Failed to load Q&A history", err));
    }
  }, [jobId, qaEnabled, history.length]);

  // Auto-scroll logic
  useEffect(() => {
    const el = transcriptRef.current;
    if (!el || !autoScrollEnabled) return;
    el.scrollTo({ top: el.scrollHeight, behavior: "smooth" });
  }, [history, autoScrollEnabled]);

  const handleScroll = () => {
    const el = transcriptRef.current;
    if (!el) return;
    const isNearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 50;
    setAutoScrollEnabled(isNearBottom);
  };

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
      const qa = await askJobQuestion(jobId, q);
      setPendingQuestion(null);
      setHistory((prev) => [...prev, qa]);
    } catch (err: any) {
      console.error("Q&A error:", err);
      setPendingQuestion(null);
      setError(err?.response?.data?.detail || "Failed to get answer.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <aside className="qa-panel-glass fixed right-0 top-0 bottom-0 w-[420px] flex flex-col z-40">
      {/* Liquid glass background layers */}
      <div className="absolute inset-0 bg-gradient-to-b from-white/40 via-white/20 to-white/30 backdrop-blur-2xl" />
      <div className="absolute inset-0 bg-gradient-to-br from-blue-50/30 via-transparent to-indigo-50/20" />
      <div className="absolute inset-y-0 left-0 w-px bg-gradient-to-b from-white/60 via-white/20 to-white/40" />
      
      {/* Content */}
      <div className="relative flex flex-col h-full">
        {/* Header */}
        <div className="shrink-0 px-5 py-4">
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-gradient-to-br from-blue-400 to-indigo-500 shadow-sm shadow-blue-500/30" />
            <h2 className="text-[13px] font-medium text-gray-800">
              AI Analyst
            </h2>
          </div>
          <p className="text-[11px] text-gray-500 mt-0.5 ml-4">
            Query the knowledge graph for this research on <span className="font-medium text-gray-600">{companyLabel || "this target"}</span>
          </p>
        </div>

        {/* Transcript */}
        <div 
          className="flex-1 overflow-y-auto px-4 pb-4 space-y-4"
          ref={transcriptRef}
          onScroll={handleScroll}
        >
          {history.length === 0 && !loading && !pendingQuestion && (
            <div className="h-full flex flex-col items-center justify-center text-center px-6">
              <div className="w-12 h-12 mb-4 rounded-2xl bg-gradient-to-br from-blue-500/10 to-indigo-500/10 border border-white/40 shadow-inner flex items-center justify-center">
                <svg className="w-6 h-6 text-blue-500/70" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904 9 18.75l-.813-2.846a4.5 4.5 0 0 0-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 0 0 3.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 0 0 3.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 0 0-3.09 3.09ZM18.259 8.715 18 9.75l-.259-1.035a3.375 3.375 0 0 0-2.455-2.456L14.25 6l1.036-.259a3.375 3.375 0 0 0 2.455-2.456L18 2.25l.259 1.035a3.375 3.375 0 0 0 2.456 2.456L21.75 6l-1.035.259a3.375 3.375 0 0 0-2.456 2.456Z" />
                </svg>
              </div>
              <p className="text-sm font-medium text-gray-700 mb-1">Query knowledge graph</p>
              <p className="text-xs text-gray-400 leading-relaxed">
                Ask about details, sources, or specific facts from this research.
              </p>
            </div>
          )}

          {history.map((qa) => (
            <div key={qa.id} className="space-y-3">
              <ChatBubble
                role="user"
                markdown={qa.question}
                createdAt={qa.created_at}
                citationIndexMap={citationIndexMap}
                citations={allCitations}
              />
              <ChatBubble
                role="assistant"
                markdown={qa.answer_markdown}
                createdAt={qa.created_at}
                citationIndexMap={citationIndexMap}
                citations={allCitations}
              />
            </div>
          ))}

          {/* Show pending question immediately */}
          {pendingQuestion && (
            <ChatBubble
              role="user"
              markdown={pendingQuestion}
              citationIndexMap={citationIndexMap}
              citations={allCitations}
            />
          )}

          {loading && (
            <div className="flex items-center gap-2 text-gray-400 text-xs py-2">
              <div className="flex space-x-1">
                <div className="w-1.5 h-1.5 bg-blue-400/60 rounded-full animate-bounce" style={{ animationDelay: "0ms" }} />
                <div className="w-1.5 h-1.5 bg-blue-400/60 rounded-full animate-bounce" style={{ animationDelay: "150ms" }} />
                <div className="w-1.5 h-1.5 bg-blue-400/60 rounded-full animate-bounce" style={{ animationDelay: "300ms" }} />
              </div>
              <span className="text-gray-400">Thinking...</span>
            </div>
          )}
          
          {error && (
            <div className="text-xs text-red-600 bg-red-50/80 border border-red-100/50 p-2.5 rounded-lg text-center backdrop-blur-sm">
              {error}
            </div>
          )}
        </div>

        {/* Composer */}
        <div className="shrink-0 p-4">
          <form onSubmit={handleSend} className="relative">
            <div className="relative rounded-xl bg-white/60 backdrop-blur-sm border border-white/50 shadow-sm shadow-black/5 overflow-hidden focus-within:border-blue-300/50 focus-within:shadow-blue-500/10 transition-all">
              <textarea
                className="w-full bg-transparent py-3 pl-4 pr-12 placeholder:text-gray-400 focus:outline-none text-sm resize-none text-gray-800"
                rows={2}
                placeholder="Ask a question..."
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && !e.shiftKey) {
                    e.preventDefault();
                    handleSend(e);
                  }
                }}
                disabled={!qaEnabled || loading}
              />
              <button
                type="submit"
                disabled={!input.trim() || loading || !qaEnabled}
                className="absolute right-2 bottom-2.5 p-2 rounded-lg text-white bg-gradient-to-r from-blue-500 to-blue-600 hover:from-blue-600 hover:to-blue-700 disabled:from-gray-300 disabled:to-gray-300 disabled:cursor-not-allowed transition-all shadow-sm disabled:shadow-none"
                title="Send"
              >
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M22 2 11 13"/><path d="m22 2-7 20-4-9-9-4 20-7z"/></svg>
              </button>
            </div>
          </form>
        </div>
      </div>
    </aside>
  );
}

