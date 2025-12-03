"use client";

import { useEffect, useRef, useState } from "react";
import { fetchJobQA, askJobQuestion, JobQAPair } from "../../lib/api";
import { buildCitationIndexMap, Citation } from "../../lib/citations";
import { ChatBubble } from "./ChatBubble";

type QADrawerProps = {
  open: boolean;
  onClose: () => void;
  jobId: string;
  qaEnabled: boolean;
  companyLabel?: string;
  domain?: string;
  allCitations: Citation[];
};

export function QADrawer({
  open,
  onClose,
  jobId,
  qaEnabled,
  companyLabel,
  allCitations,
}: QADrawerProps) {
  const [history, setHistory] = useState<JobQAPair[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [autoScrollEnabled, setAutoScrollEnabled] = useState(true);
  
  const transcriptRef = useRef<HTMLDivElement>(null);
  const citationIndexMap = buildCitationIndexMap(allCitations);

  useEffect(() => {
    if (open) {
      document.body.classList.add("qa-drawer-open");
      // Load history if empty
      if (history.length === 0 && qaEnabled) {
        fetchJobQA(jobId)
          .then(setHistory)
          .catch((err) => console.error("Failed to load Q&A history", err));
      }
    } else {
      document.body.classList.remove("qa-drawer-open");
    }

    const handler = (e: KeyboardEvent) => {
      if (open && e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    return () => {
      window.removeEventListener("keydown", handler);
      document.body.classList.remove("qa-drawer-open"); // Cleanup
    };
  }, [open, jobId, qaEnabled, onClose, history.length]);

  // Auto-scroll logic
  useEffect(() => {
    const el = transcriptRef.current;
    if (!el || !autoScrollEnabled) return;
    el.scrollTo({ top: el.scrollHeight, behavior: "smooth" });
  }, [history, autoScrollEnabled, open]);

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

    setLoading(true);
    setError(null);
    setAutoScrollEnabled(true); // Force scroll to bottom on new message

    // Optimistic update (optional, but we wait for real response for ID/formatting)
    try {
      const qa = await askJobQuestion(jobId, q);
      setHistory((prev) => [...prev, qa]);
      setInput("");
    } catch (err: any) {
      console.error("Q&A error:", err);
      setError(err?.response?.data?.detail || "Failed to get answer.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <>
      <div
        className={`qa-scrim ${open ? "qa-scrim--open" : ""}`}
        onClick={onClose}
        aria-hidden="true"
      />
      <aside
        className={`qa-drawer ${open ? "qa-drawer--open" : ""}`}
        aria-hidden={!open}
        role="dialog"
        aria-label="Q&A drawer"
      >
        <div className="qa-glass h-full flex flex-col">
          {/* Header */}
          <div className="shrink-0 px-5 py-4 border-b border-gray-200/50 flex items-center justify-between bg-white/40 backdrop-blur-md">
            <div>
              <h2 className="text-sm font-semibold text-gray-900 uppercase tracking-wide">
                AI Analyst
              </h2>
              <p className="text-xs text-gray-500 truncate max-w-[240px]">
                {companyLabel}
              </p>
            </div>
            <button
              onClick={onClose}
              className="p-2 -mr-2 text-gray-400 hover:text-gray-700 transition-colors rounded-full hover:bg-black/5"
              aria-label="Close drawer"
            >
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>
            </button>
          </div>

          {/* Transcript */}
          <div 
            className="flex-1 overflow-y-auto p-5 space-y-6 bg-white/30 scroll-smooth"
            ref={transcriptRef}
            onScroll={handleScroll}
          >
            {history.length === 0 && !loading && (
              <div className="h-full flex flex-col items-center justify-center text-center text-gray-500 px-6 opacity-60">
                <svg className="w-12 h-12 mb-3 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="1.5"><path d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" strokeLinecap="round" strokeLinejoin="round"/></svg>
                <p className="text-sm">Ask questions about the brief, sources, or specific details.</p>
              </div>
            )}

            {history.map((qa) => (
              <div key={qa.id}>
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

            {loading && (
              <div className="bubble bubble--assistant w-16 flex items-center justify-center py-3">
                <div className="flex space-x-1">
                  <div className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: "0ms" }} />
                  <div className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: "150ms" }} />
                  <div className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: "300ms" }} />
                </div>
              </div>
            )}
            
            {error && (
              <div className="text-xs text-red-600 bg-red-50 border border-red-100 p-2 rounded-md text-center">
                {error}
              </div>
            )}
          </div>

          {/* Composer */}
          <div className="shrink-0 p-4 bg-white/60 border-t border-gray-200/50 backdrop-blur-md">
            <form onSubmit={handleSend} className="relative">
              <textarea
                className="w-full rounded-xl border-0 bg-white py-3 pl-4 pr-12 shadow-sm ring-1 ring-inset ring-gray-300 placeholder:text-gray-400 focus:ring-2 focus:ring-inset focus:ring-blue-600 text-sm resize-none text-gray-900"
                rows={2} // Start small
                placeholder={qaEnabled ? "Type your question..." : "Q&A not available"}
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
                className="absolute right-2 bottom-2 p-1.5 rounded-lg text-white bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors"
                title="Send"
              >
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M22 2 11 13"/><path d="m22 2-7 20-4-9-9-4 20-7z"/></svg>
              </button>
            </form>
            <div className="mt-2 text-[10px] text-gray-400 text-center">
              Answers are generated from the sources collected in this research run.
            </div>
          </div>
        </div>
      </aside>
    </>
  );
}

